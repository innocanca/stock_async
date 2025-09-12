# -*- coding: utf-8 -*-
"""
定时同步调度器模块
负责每天自动同步股票数据到数据库
"""

import time
import schedule
import logging
from datetime import datetime, timedelta
from typing import Optional, List
import threading
import signal
import sys
import os

from fetcher import StockDataFetcher
from database import StockDatabase
from utils import format_date, get_special_operations

logger = logging.getLogger(__name__)


class StockDataScheduler:
    """股票数据定时同步器"""
    
    def __init__(self, sync_time: str = "18:00", weekend_sync: bool = False):
        """
        初始化调度器
        
        Args:
            sync_time: 每日同步时间（HH:MM格式）
            weekend_sync: 是否在周末也执行同步
        """
        self.sync_time = sync_time
        self.weekend_sync = weekend_sync
        self.fetcher = None
        self.db = StockDatabase()
        self.is_running = False
        self.sync_thread = None
        
        # 设置信号处理
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """处理停止信号"""
        logger.info(f"接收到信号 {signum}，正在停止调度器...")
        self.stop()
        sys.exit(0)
    
    def initialize_fetcher(self):
        """初始化数据获取器"""
        if self.fetcher is None:
            self.fetcher = StockDataFetcher()
    
    def get_latest_trading_date(self) -> Optional[str]:
        """
        获取最新的交易日期
        
        Returns:
            str: 最新交易日期（YYYYMMDD格式）
        """
        try:
            today = datetime.now()
            
            # 尝试最近10天，找到最新的交易日
            for i in range(10):
                check_date = (today - timedelta(days=i))
                
                # 跳过周末（如果不同步周末数据）
                if not self.weekend_sync and check_date.weekday() >= 5:
                    continue
                
                date_str = check_date.strftime('%Y%m%d')
                
                # 检查是否有交易数据
                self.initialize_fetcher()
                test_df = self.fetcher.get_daily_with_retry(trade_date=date_str, max_retries=1)
                
                if test_df is not None and not test_df.empty:
                    logger.info(f"确定最新交易日: {date_str}")
                    return date_str
            
            logger.warning("无法确定最新交易日")
            return None
            
        except Exception as e:
            logger.error(f"获取最新交易日失败: {e}")
            return None
    
    def sync_daily_data(self, target_date: str = None, stocks: List[str] = None) -> bool:
        """
        同步指定日期的股票数据
        
        Args:
            target_date: 目标日期（YYYYMMDD格式），None表示最新交易日
            stocks: 股票代码列表，None表示获取主板股票
            
        Returns:
            bool: 同步是否成功
        """
        try:
            start_time = time.time()
            
            # 确定同步日期
            if target_date is None:
                target_date = self.get_latest_trading_date()
                if target_date is None:
                    logger.error("无法确定同步日期")
                    return False
            
            logger.info(f"🔄 开始同步 {target_date} 的股票数据...")
            
            self.initialize_fetcher()
            
            # 确定股票列表
            if stocks is None:
                stocks = self.fetcher.get_main_board_stocks()
                if not stocks:
                    logger.error("无法获取主板股票列表")
                    return False
            
            logger.info(f"📈 准备同步 {len(stocks)} 只股票的数据")
            
            # 检查数据是否已存在
            with self.db:
                # 查询当日已有数据
                existing_data = self.db.query_data(
                    start_date=target_date, 
                    end_date=target_date
                )
                
                if existing_data is not None and not existing_data.empty:
                    existing_stocks = set(existing_data['ts_code'].values)
                    logger.info(f"📊 数据库中已有 {len(existing_stocks)} 只股票的 {target_date} 数据")
                    
                    # 过滤出需要更新的股票
                    stocks_to_sync = [s for s in stocks if s not in existing_stocks]
                    if stocks_to_sync:
                        logger.info(f"🔄 需要新增 {len(stocks_to_sync)} 只股票数据")
                        stocks = stocks_to_sync
                    else:
                        logger.info(f"✅ {target_date} 的数据已是最新，无需同步")
                        return True
            
            # 获取当日全市场数据
            all_data = []
            success_count = 0
            
            for i, stock_code in enumerate(stocks, 1):
                try:
                    df = self.fetcher.get_daily_with_retry(trade_date=target_date, ts_code=stock_code)
                    if df is not None and not df.empty:
                        all_data.append(df)
                        success_count += 1
                    
                    # 显示进度
                    if i % 100 == 0 or i == len(stocks):
                        logger.info(f"进度: {i}/{len(stocks)} ({i/len(stocks)*100:.1f}%), 成功: {success_count}")
                    
                    # 短暂延迟避免API限制
                    time.sleep(0.1)
                    
                except Exception as e:
                    logger.warning(f"获取 {stock_code} 数据失败: {e}")
                    continue
            
            if not all_data:
                logger.warning(f"未获取到 {target_date} 的任何新数据")
                return False
            
            # 合并数据并插入数据库
            import pandas as pd
            combined_df = pd.concat(all_data, ignore_index=True)
            
            logger.info(f"💾 准备插入 {len(combined_df)} 条记录到数据库...")
            
            with self.db:
                success = self.db.insert_daily_data(combined_df)
                
                if success:
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    logger.info(f"✅ {target_date} 数据同步成功！")
                    logger.info(f"   📊 插入记录: {len(combined_df)} 条")
                    logger.info(f"   📈 成功股票: {success_count}/{len(stocks)} 只")
                    logger.info(f"   ⏱️ 耗时: {duration:.1f} 秒")
                    
                    # 显示数据库最新状态
                    stats = self.db.get_stats()
                    logger.info(f"   📈 数据库总记录: {stats.get('total_records', 0):,}")
                    
                    return True
                else:
                    logger.error(f"❌ {target_date} 数据插入数据库失败")
                    return False
                    
        except Exception as e:
            logger.error(f"同步 {target_date} 数据时发生错误: {e}")
            return False
    
    def schedule_daily_sync(self):
        """设置每日定时同步"""
        schedule.clear()  # 清除之前的任务
        
        if self.weekend_sync:
            # 每天都同步
            schedule.every().day.at(self.sync_time).do(self.sync_daily_data)
            logger.info(f"📅 已设置每日 {self.sync_time} 自动同步（包括周末）")
        else:
            # 只在工作日同步
            schedule.every().monday.at(self.sync_time).do(self.sync_daily_data)
            schedule.every().tuesday.at(self.sync_time).do(self.sync_daily_data)
            schedule.every().wednesday.at(self.sync_time).do(self.sync_daily_data)
            schedule.every().thursday.at(self.sync_time).do(self.sync_daily_data)
            schedule.every().friday.at(self.sync_time).do(self.sync_daily_data)
            logger.info(f"📅 已设置工作日 {self.sync_time} 自动同步（周末不同步）")
    
    def run_daemon(self):
        """运行守护进程模式"""
        logger.info("🤖 启动股票数据定时同步守护进程...")
        logger.info(f"   同步时间: 每日 {self.sync_time}")
        logger.info(f"   周末同步: {'是' if self.weekend_sync else '否'}")
        
        self.is_running = True
        self.schedule_daily_sync()
        
        # 启动时立即执行一次同步（可选）
        logger.info("🔄 启动时执行一次数据同步...")
        self.sync_daily_data()
        
        # 主循环
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # 每分钟检查一次
                
        except KeyboardInterrupt:
            logger.info("接收到中断信号，正在停止...")
        finally:
            self.stop()
    
    def run_daemon_threaded(self):
        """在后台线程中运行守护进程"""
        if self.sync_thread and self.sync_thread.is_alive():
            logger.warning("守护进程已在运行中")
            return False
        
        self.sync_thread = threading.Thread(target=self.run_daemon, daemon=True)
        self.sync_thread.start()
        logger.info("🚀 守护进程已在后台启动")
        return True
    
    def stop(self):
        """停止调度器"""
        self.is_running = False
        schedule.clear()
        logger.info("🛑 调度器已停止")
    
    def get_next_sync_time(self) -> Optional[str]:
        """获取下次同步时间"""
        try:
            jobs = schedule.get_jobs()
            if not jobs:
                return None
            
            next_run = min(job.next_run for job in jobs)
            return next_run.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    def manual_sync(self, date_str: str = None) -> bool:
        """
        手动触发同步
        
        Args:
            date_str: 日期字符串（YYYY-MM-DD格式），None表示最新交易日
            
        Returns:
            bool: 同步是否成功
        """
        target_date = None
        if date_str:
            try:
                target_date = format_date(date_str)
            except Exception as e:
                logger.error(f"日期格式错误: {e}")
                return False
        
        return self.sync_daily_data(target_date)


class DailyDataSyncer:
    """每日数据同步器（简化版本）"""
    
    @staticmethod
    def sync_today() -> bool:
        """同步今天的数据"""
        syncer = StockDataScheduler()
        return syncer.sync_daily_data()
    
    @staticmethod
    def sync_date(date_str: str) -> bool:
        """
        同步指定日期的数据
        
        Args:
            date_str: 日期字符串（YYYY-MM-DD格式）
            
        Returns:
            bool: 同步是否成功
        """
        syncer = StockDataScheduler()
        return syncer.manual_sync(date_str)
    
    @staticmethod
    def sync_missing_dates(start_date: str, end_date: str = None) -> dict:
        """
        同步缺失的日期数据
        
        Args:
            start_date: 开始日期（YYYY-MM-DD格式）
            end_date: 结束日期（YYYY-MM-DD格式），None表示到今天
            
        Returns:
            dict: 同步统计信息
        """
        if end_date is None:
            end_date = datetime.now().strftime('%Y-%m-%d')
        
        syncer = StockDataScheduler()
        
        try:
            start_dt = datetime.strptime(start_date, '%Y-%m-%d')
            end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        except ValueError as e:
            logger.error(f"日期格式错误: {e}")
            return {}
        
        stats = {
            'total_days': 0,
            'successful_days': 0,
            'failed_days': [],
            'skipped_days': 0
        }
        
        current_date = start_dt
        while current_date <= end_dt:
            # 跳过周末（除非特别配置）
            if not syncer.weekend_sync and current_date.weekday() >= 5:
                stats['skipped_days'] += 1
                current_date += timedelta(days=1)
                continue
            
            date_str = current_date.strftime('%Y-%m-%d')
            stats['total_days'] += 1
            
            logger.info(f"🔄 同步 {date_str} 数据...")
            
            if syncer.manual_sync(date_str):
                stats['successful_days'] += 1
                logger.info(f"✅ {date_str} 同步成功")
            else:
                stats['failed_days'].append(date_str)
                logger.warning(f"❌ {date_str} 同步失败")
            
            current_date += timedelta(days=1)
            time.sleep(1)  # 避免频繁操作
        
        logger.info(f"📊 批量同步完成：")
        logger.info(f"   总天数: {stats['total_days']}")
        logger.info(f"   成功: {stats['successful_days']}")
        logger.info(f"   失败: {len(stats['failed_days'])}")
        logger.info(f"   跳过: {stats['skipped_days']}")
        
        return stats


def create_cron_job(sync_time: str = "18:00", weekend_sync: bool = False, 
                   script_path: str = None) -> str:
    """
    创建cron任务配置
    
    Args:
        sync_time: 同步时间（HH:MM格式）
        weekend_sync: 是否包括周末
        script_path: 脚本路径
        
    Returns:
        str: cron配置字符串
    """
    if script_path is None:
        script_path = os.path.abspath(__file__)
        script_dir = os.path.dirname(script_path)
        script_path = os.path.join(script_dir, "main.py")
    
    hour, minute = sync_time.split(':')
    
    if weekend_sync:
        # 每天执行
        cron_schedule = f"{minute} {hour} * * *"
    else:
        # 只在工作日执行（周一到周五）
        cron_schedule = f"{minute} {hour} * * 1-5"
    
    # 构造完整的cron命令
    python_path = sys.executable
    log_file = os.path.join(os.path.dirname(script_path), "sync.log")
    
    cron_command = f"{cron_schedule} cd {os.path.dirname(script_path)} && {python_path} {script_path} --sync >> {log_file} 2>&1"
    
    return cron_command


def setup_systemd_service(sync_time: str = "18:00", weekend_sync: bool = False,
                         script_path: str = None, user: str = None) -> str:
    """
    创建systemd服务配置
    
    Args:
        sync_time: 同步时间
        weekend_sync: 是否包括周末
        script_path: 脚本路径
        user: 运行用户
        
    Returns:
        str: systemd服务配置内容
    """
    if script_path is None:
        script_path = os.path.abspath(__file__)
        script_dir = os.path.dirname(script_path)
        script_path = os.path.join(script_dir, "main.py")
    
    if user is None:
        import getpass
        user = getpass.getuser()
    
    service_content = f"""[Unit]
Description=Stock Data Daily Sync
After=network.target mysql.service

[Service]
Type=simple
User={user}
WorkingDirectory={os.path.dirname(script_path)}
ExecStart={sys.executable} {script_path} --daemon --sync-time {sync_time} {'--weekend-sync' if weekend_sync else ''}
Restart=always
RestartSec=300

[Install]
WantedBy=multi-user.target
"""
    
    return service_content


def install_cron_job(sync_time: str = "18:00", weekend_sync: bool = False):
    """
    安装cron任务
    
    Args:
        sync_time: 同步时间
        weekend_sync: 是否包括周末
    """
    cron_command = create_cron_job(sync_time, weekend_sync)
    
    print("🔧 Cron任务配置：")
    print("=" * 60)
    print(cron_command)
    print("=" * 60)
    
    print("\\n📝 安装步骤：")
    print("1. 复制上面的配置")
    print("2. 运行 'crontab -e' 编辑cron任务")
    print("3. 粘贴配置并保存")
    print("4. 运行 'crontab -l' 验证任务已添加")
    
    print("\\n💡 说明：")
    print(f"   - 每日 {sync_time} 自动执行数据同步")
    print(f"   - 周末执行: {'是' if weekend_sync else '否'}")
    print(f"   - 日志文件: sync.log")


def install_systemd_service(sync_time: str = "18:00", weekend_sync: bool = False):
    """
    安装systemd服务
    
    Args:
        sync_time: 同步时间
        weekend_sync: 是否包括周末
    """
    service_content = setup_systemd_service(sync_time, weekend_sync)
    
    print("🔧 Systemd服务配置：")
    print("=" * 60)
    print(service_content)
    print("=" * 60)
    
    print("\\n📝 安装步骤：")
    print("1. 将上面的配置保存为 /etc/systemd/system/stock-sync.service")
    print("2. 运行 'sudo systemctl daemon-reload' 重载配置")
    print("3. 运行 'sudo systemctl enable stock-sync' 设置开机启动")
    print("4. 运行 'sudo systemctl start stock-sync' 启动服务")
    print("5. 运行 'sudo systemctl status stock-sync' 检查状态")
    
    print("\\n💡 说明：")
    print("   - 系统级服务，开机自动启动")
    print("   - 自动重启机制，服务异常时自动恢复") 
    print("   - 更稳定，适合生产环境")
