# -*- coding: utf-8 -*-
"""
命令行接口模块
处理命令行参数解析和执行逻辑
"""

import argparse
import logging
import sys
import time
import pandas as pd
from typing import Optional

from config import DEFAULT_CONFIG_MODE, ARGS_CONFIG
from utils import (
    format_date, load_config_defaults, merge_config_and_args, 
    print_current_config, estimate_execution_time, validate_stock_codes
)
from fetcher import StockDataFetcher
from database import StockDatabase

logger = logging.getLogger(__name__)


class StockDataCLI:
    """股票数据命令行接口"""
    
    def __init__(self):
        """初始化CLI"""
        self.db = StockDatabase()
        self.fetcher = None
    
    def create_parser(self) -> argparse.ArgumentParser:
        """
        创建命令行参数解析器
        
        Returns:
            argparse.ArgumentParser: 配置好的参数解析器
        """
        parser = argparse.ArgumentParser(description='股票数据获取和存储工具')
        
        # 基础参数
        parser.add_argument('--codes', nargs='*', help='股票代码列表，如：000001.SZ 000002.SZ')
        parser.add_argument('--start-date', help='开始日期（YYYY-MM-DD或YYYYMMDD格式）')
        parser.add_argument('--end-date', help='结束日期（YYYY-MM-DD或YYYYMMDD格式）')
        
        # 功能选项
        parser.add_argument('--create-db', action='store_true', help='创建数据库和数据表')
        parser.add_argument('--query', action='store_true', help='查询数据库中的数据')
        parser.add_argument('--stats', action='store_true', help='显示数据库统计信息')
        parser.add_argument('--trade-date', help='获取指定交易日的数据（YYYY-MM-DD或YYYYMMDD格式）')
        parser.add_argument('--latest', action='store_true', help='获取最新交易日的数据')
        
        # 模式和配置
        parser.add_argument('--mode', choices=['ts_code', 'trade_date'], default='ts_code', 
                           help='数据获取模式：ts_code按股票获取历史数据，trade_date按日期获取当日数据')
        parser.add_argument('--main-board', action='store_true', 
                           help='获取A股主板所有股票数据（当不指定--codes时默认开启）')
        
        # 批量处理参数
        parser.add_argument('--batch-size', type=int, default=50, 
                           help='批量获取的批次大小，防止API调用过于频繁（默认50）')
        parser.add_argument('--delay', type=float, default=0.1, 
                           help='每次API调用的延迟时间，单位秒（默认0.1）')
        parser.add_argument('--limit', type=int, 
                           help='限制获取的股票数量，用于测试（默认无限制）')
        
        # 高效获取模式
        parser.add_argument('--market-mode', action='store_true',
                           help='使用全市场模式：通过交易日循环获取（推荐用于大批量历史数据）')
        parser.add_argument('--exchange', choices=['SSE', 'SZSE'], default='SSE',
                           help='交易所选择，用于交易日历（SSE上交所，SZSE深交所，默认SSE）')
        parser.add_argument('--batch-days', type=int, default=10,
                           help='全市场模式下每批插入的交易日数量（默认10天）')
        parser.add_argument('--use-batch-insert', action='store_true', default=True,
                           help='使用分批插入优化性能（默认开启，推荐大数据量使用）')
        
        # 配置文件相关
        parser.add_argument('--config', default=DEFAULT_CONFIG_MODE,
                           choices=list(ARGS_CONFIG.keys()),
                           help=f'使用预设的配置模式（默认: {DEFAULT_CONFIG_MODE}）。'
                                f'可选: {", ".join(ARGS_CONFIG.keys())}')
        parser.add_argument('--show-config', action='store_true',
                           help='显示当前配置并退出')
        
        return parser
    
    def parse_and_merge_args(self, args=None) -> argparse.Namespace:
        """
        解析并合并命令行参数和配置文件
        
        Args:
            args: 命令行参数列表（用于测试）
            
        Returns:
            argparse.Namespace: 合并后的参数
        """
        parser = self.create_parser()
        parsed_args = parser.parse_args(args)
        
        # 加载配置文件默认值
        config_defaults = load_config_defaults(parsed_args.config)
        
        # 合并配置文件和命令行参数
        merged_args = merge_config_and_args(config_defaults, parsed_args)
        
        return merged_args
    
    def handle_show_config(self, args: argparse.Namespace) -> bool:
        """
        处理显示配置的请求
        
        Args:
            args: 命令行参数
            
        Returns:
            bool: 是否应该退出程序
        """
        if args.show_config:
            print_current_config(args)
            return True
        return False
    
    def handle_database_operations(self, args: argparse.Namespace) -> Optional[bool]:
        """
        处理数据库相关操作
        
        Args:
            args: 命令行参数
            
        Returns:
            Optional[bool]: None表示继续执行，True表示成功退出，False表示失败退出
        """
        # 创建数据库和数据表
        if getattr(args, 'create_db', False) or not (getattr(args, 'query', False) or getattr(args, 'stats', False)):
            logger.info("正在创建数据库和数据表...")
            self.db.create_database()
            with self.db:
                self.db.create_daily_table()
        
        # 查询数据库
        if getattr(args, 'query', False):
            with self.db:
                query_limit = getattr(args, 'limit', 100) or 100
                df = self.db.query_data(limit=query_limit)
                if df is not None and not df.empty:
                    print(f"\\n最新的股票数据（前{min(query_limit, 20)}条）：")
                    print(df.head(20))
                else:
                    print("数据库中没有数据")
            return True
        
        # 显示统计信息
        if getattr(args, 'stats', False):
            with self.db:
                stats = self.db.get_stats()
                print("\\n数据库统计信息：")
                print(f"总记录数: {stats.get('total_records', 0):,}")
                print(f"股票数量: {stats.get('stock_count', 0)}")
                if stats.get('date_range'):
                    print(f"日期范围: {stats['date_range']['min_date']} 到 {stats['date_range']['max_date']}")
                print(f"最后更新: {stats.get('last_update', 'N/A')}")
            return True
        
        return None
    
    def initialize_fetcher(self) -> None:
        """初始化数据获取器"""
        if self.fetcher is None:
            self.fetcher = StockDataFetcher()
    
    def get_stock_codes(self, args: argparse.Namespace) -> list:
        """
        获取股票代码列表
        
        Args:
            args: 命令行参数
            
        Returns:
            list: 股票代码列表
        """
        if args.codes:
            # 验证股票代码格式
            if not validate_stock_codes(args.codes):
                logger.warning("存在无效的股票代码格式")
            
            logger.info(f"使用指定的股票代码: {len(args.codes)}只")
            return args.codes
        else:
            # 获取A股主板股票
            logger.info("未指定股票代码，将获取所有A股主板股票数据...")
            self.initialize_fetcher()
            
            stock_codes = self.fetcher.get_main_board_stocks()
            
            if not stock_codes:
                from config import DEFAULT_STOCK_CODES
                logger.error("无法获取主板股票列表，使用默认股票代码")
                stock_codes = DEFAULT_STOCK_CODES
            elif args.limit and args.limit > 0:
                stock_codes = stock_codes[:args.limit]
                logger.info(f"限制获取股票数量: {len(stock_codes)}只（用于测试）")
            
            return stock_codes
    
    def handle_trade_date_mode(self, args: argparse.Namespace) -> Optional[bool]:
        """
        处理指定交易日期模式
        
        Args:
            args: 命令行参数
            
        Returns:
            Optional[bool]: None表示继续执行，True表示成功，False表示失败
        """
        if not getattr(args, 'trade_date', None):
            return None
        
        self.initialize_fetcher()
        trade_date_formatted = format_date(args.trade_date)
        stock_codes = self.get_stock_codes(args)
        
        logger.info(f"获取 {trade_date_formatted} 交易日数据...")
        logger.info(f"股票数量: {len(stock_codes)}只")
        
        if len(stock_codes) > 100:
            logger.warning(f"即将获取 {len(stock_codes)} 只股票的交易日数据，这可能需要一些时间...")
        
        # 获取指定交易日的数据
        all_data = []
        for ts_code in stock_codes:
            df = self.fetcher.get_daily_by_date(trade_date_formatted, ts_code)
            if df is not None and not df.empty:
                all_data.append(df)
            time.sleep(0.1)  # 避免API频率限制
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"成功获取 {len(combined_df)} 条交易日数据")
            
            # 存储到数据库
            with self.db:
                success = self.db.insert_daily_data(combined_df)
                if success:
                    logger.info("交易日数据存储成功！")
                    return True
                else:
                    logger.error("交易日数据存储失败")
                    return False
        else:
            logger.warning(f"未获取到 {trade_date_formatted} 的数据")
            return False
    
    def handle_latest_mode(self, args: argparse.Namespace) -> Optional[bool]:
        """
        处理最新交易日模式
        
        Args:
            args: 命令行参数
            
        Returns:
            Optional[bool]: None表示继续执行，True表示成功，False表示失败
        """
        if not getattr(args, 'latest', False):
            return None
        
        self.initialize_fetcher()
        stock_codes = self.get_stock_codes(args)
        
        logger.info(f"获取最新交易日数据...")
        logger.info(f"股票数量: {len(stock_codes)}只")
        
        if len(stock_codes) > 100:
            logger.warning(f"即将获取 {len(stock_codes)} 只股票的最新交易日数据，这可能需要一些时间...")
        
        df = self.fetcher.get_latest_trading_day_data(stock_codes)
        if df is not None and not df.empty:
            logger.info(f"成功获取最新交易日 {len(df)} 条数据")
            
            # 存储到数据库
            with self.db:
                success = self.db.insert_daily_data(df)
                if success:
                    logger.info("最新交易日数据存储成功！")
                    
                    # 显示获取的数据
                    print("\\n最新交易日数据：")
                    print(df[['ts_code', 'trade_date', 'open', 'high', 'low', 'close', 'pct_chg', 'vol']].to_string(index=False))
                    return True
                else:
                    logger.error("最新交易日数据存储失败")
                    return False
        else:
            logger.warning("未获取到最新交易日数据")
            return False
    
    def handle_historical_data(self, args: argparse.Namespace) -> bool:
        """
        处理历史数据获取
        
        Args:
            args: 命令行参数
            
        Returns:
            bool: 是否成功
        """
        self.initialize_fetcher()
        
        # 处理日期参数
        start_date = format_date(args.start_date) if args.start_date else None
        end_date = format_date(args.end_date) if args.end_date else None
        
        # 检查是否使用全市场模式
        if getattr(args, 'market_mode', False):
            return self.handle_market_mode_data(args, start_date, end_date)
        else:
            return self.handle_stock_mode_data(args, start_date, end_date)
    
    def handle_market_mode_data(self, args: argparse.Namespace, start_date: str, end_date: str) -> bool:
        """
        处理全市场模式的数据获取（通过交易日循环）
        
        Args:
            args: 命令行参数
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 是否成功
        """
        if not start_date or not end_date:
            logger.error("全市场模式需要指定开始和结束日期")
            return False
        
        logger.info(f"🚀 使用全市场模式获取数据")
        logger.info(f"📅 日期范围: {start_date} 到 {end_date}")
        logger.info(f"🏢 交易所: {args.exchange}")
        logger.info(f"⏱️ API延迟: {args.delay}秒")
        
        # 检查是否使用分批插入
        use_batch_insert = getattr(args, 'use_batch_insert', True)
        batch_days = getattr(args, 'batch_days', 10)
        
        if use_batch_insert:
            logger.info(f"💾 使用分批插入模式，每 {batch_days} 个交易日插入一次")
            return self.handle_batch_insert_mode(args, start_date, end_date, batch_days)
        else:
            logger.info(f"💾 使用一次性插入模式（不推荐大数据量使用）")
            return self.handle_single_insert_mode(args, start_date, end_date)
    
    def handle_batch_insert_mode(self, args: argparse.Namespace, start_date: str, end_date: str, batch_days: int) -> bool:
        """
        处理分批插入模式（推荐用于大数据量）
        
        Args:
            args: 命令行参数
            start_date: 开始日期
            end_date: 结束日期
            batch_days: 每批处理的交易日数量
            
        Returns:
            bool: 是否成功
        """
        # 预估时间
        estimated_time = self.fetcher.estimate_market_data_time(start_date, end_date, args.delay)
        logger.info(f"⏰ 预估总耗时: {estimated_time}")
        
        # 使用分批插入方法
        with self.db:
            stats = self.fetcher.get_all_market_data_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                delay=args.delay,
                exchange=args.exchange,
                db_instance=self.db,
                batch_days=batch_days
            )
        
        if not stats or stats.get('total_records', 0) == 0:
            logger.error("❌ 全市场数据获取和插入失败")
            return False
        
        # 显示最终统计信息
        logger.info("✅ 全市场数据获取和插入完成！")
        
        # 获取数据库最新统计
        with self.db:
            db_stats = self.db.get_stats()
            logger.info(f"📊 数据库当前状态:")
            logger.info(f"   总记录数: {db_stats.get('total_records', 0):,}")
            logger.info(f"   股票数量: {db_stats.get('stock_count', 0)}")
            if db_stats.get('date_range'):
                logger.info(f"   数据范围: {db_stats['date_range']['min_date']} 到 {db_stats['date_range']['max_date']}")
        
        # 判断成功率
        success_rate = stats.get('batch_insert_success', 0) / max(stats.get('total_batches', 1), 1)
        return success_rate >= 0.8  # 80%以上成功率认为是成功的
    
    def handle_single_insert_mode(self, args: argparse.Namespace, start_date: str, end_date: str) -> bool:
        """
        处理一次性插入模式（不推荐大数据量使用）
        
        Args:
            args: 命令行参数
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 是否成功
        """
        logger.warning("⚠️ 使用一次性插入模式，大数据量可能导致性能问题")
        
        # 预估时间
        estimated_time = self.fetcher.estimate_market_data_time(start_date, end_date, args.delay)
        logger.info(f"⏰ 预估耗时: {estimated_time}")
        
        # 获取全市场数据
        df = self.fetcher.get_all_market_data_by_dates(
            start_date=start_date,
            end_date=end_date, 
            delay=args.delay,
            exchange=args.exchange
        )
        
        if df.empty:
            logger.error("未获取到任何全市场数据")
            return False
        
        logger.info(f"📊 准备插入 {len(df):,} 条记录到数据库...")
        
        # 存储到数据库
        logger.info("💾 开始一次性插入全市场数据到MySQL数据库...")
        with self.db:
            success = self.db.insert_daily_data(df)
            if success:
                logger.info("✅ 全市场数据存储成功！")
                
                # 显示统计信息
                stats = self.db.get_stats()
                logger.info(f"📊 数据库总记录数: {stats.get('total_records', 0):,}")
                logger.info(f"📈 涉及股票数量: {stats.get('stock_count', 0)}")
                return True
            else:
                logger.error("❌ 全市场数据存储失败")
                return False
    
    def handle_stock_mode_data(self, args: argparse.Namespace, start_date: str, end_date: str) -> bool:
        """
        处理股票模式的数据获取（通过股票代码循环）
        
        Args:
            args: 命令行参数
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            bool: 是否成功
        """
        stock_codes = self.get_stock_codes(args)
        
        logger.info(f"📊 使用股票模式获取数据")
        logger.info(f"📈 股票数量: {len(stock_codes)}只")
        logger.info(f"📅 日期范围: {start_date or 'N/A'} 到 {end_date or '今天'}")
        logger.info(f"🔧 数据获取模式: {args.mode}")
        logger.info(f"⚙️ 批次大小: {args.batch_size}, 延迟: {args.delay}秒")
        
        # 如果股票数量很多，给出提示和建议
        if len(stock_codes) > 100:
            estimated_time = estimate_execution_time(len(stock_codes), args.delay, args.batch_size)
            logger.warning(f"⚠️  即将获取 {len(stock_codes)} 只股票数据，预计需要 {estimated_time}")
            logger.warning("💡 建议：对于大批量历史数据，推荐使用 --market-mode 参数，效率更高")
            logger.info("   如需测试，可使用 --limit 参数限制股票数量")
        
        # 获取股票数据
        df = self.fetcher.get_multiple_stocks_data(
            stock_codes, start_date, end_date, 
            batch_size=args.batch_size, delay=args.delay
        )
        
        if df.empty:
            logger.warning("没有获取到任何数据")
            return False
        
        # 存储到数据库
        logger.info("正在将数据存储到MySQL数据库...")
        with self.db:
            success = self.db.insert_daily_data(df)
            if success:
                logger.info("✅ 数据存储成功！")
                
                # 显示统计信息
                stats = self.db.get_stats()
                logger.info(f"📊 数据库总记录数: {stats.get('total_records', 0):,}")
                logger.info(f"📈 涉及股票数量: {stats.get('stock_count', 0)}")
                return True
            else:
                logger.error("❌ 数据存储失败")
                return False
    
    def run(self, args=None) -> int:
        """
        运行CLI程序
        
        Args:
            args: 命令行参数列表（用于测试）
            
        Returns:
            int: 退出代码，0表示成功，1表示失败
        """
        try:
            # 解析参数
            parsed_args = self.parse_and_merge_args(args)
            
            # 如果只是显示配置，则打印并退出
            if self.handle_show_config(parsed_args):
                return 0
            
            # 显示当前配置
            print_current_config(parsed_args)
            
            # 处理数据库操作
            db_result = self.handle_database_operations(parsed_args)
            if db_result is not None:
                return 0 if db_result else 1
            
            # 处理特定交易日数据获取
            trade_date_result = self.handle_trade_date_mode(parsed_args)
            if trade_date_result is not None:
                return 0 if trade_date_result else 1
            
            # 处理获取最新交易日数据
            latest_result = self.handle_latest_mode(parsed_args)
            if latest_result is not None:
                return 0 if latest_result else 1
            
            # 处理历史数据获取
            if self.handle_historical_data(parsed_args):
                return 0
            else:
                return 1
                
        except Exception as e:
            logger.error(f"程序执行失败: {e}")
            return 1
