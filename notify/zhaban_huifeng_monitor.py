#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
炸板回封监控脚本

策略：监控炸板后又重新涨到9个点要回封的股票
核心逻辑：
1. 炸板识别：股票曾经涨停（10%），后来从涨停价回落（炸板）
2. 回封趋势：炸板后重新上涨到9%以上，有回封涨停的潜力
3. 实时监控：使用tushare实时数据接口持续监控
4. 及时通知：发现符合条件的股票立即推送通知

使用接口：tushare.realtime_list() - 实时涨跌幅排名数据
数据源：东方财富(dc) 或 新浪(sina)
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
import tushare as ts
from datetime import datetime, timedelta
import time
import json
from typing import Dict, List, Optional

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('zhaban_huifeng_monitor.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class ZhaBanHuiFengMonitor:
    """炸板回封监控器"""
    
    def __init__(self, data_source: str = 'dc'):
        """
        初始化监控器
        
        Args:
            data_source: 数据源，'dc' 东方财富 或 'sina' 新浪
        """
        self.data_source = data_source
        self.zhaban_history = {}  # 记录炸板历史 {ts_code: {'max_pct': float, 'zhaban_time': str, 'zhaban_pct': float}}
        self.huifeng_candidates = {}  # 回封候选股票
        self.notification_sent = set()  # 已发送通知的股票，避免重复通知
        self.monitor_start_time = datetime.now()
        
        # 策略参数
        self.ZHABAN_THRESHOLD = 9.5  # 炸板阈值：从涨停回落到此涨幅以下算炸板
        self.HUIFENG_THRESHOLD = 9.0  # 回封阈值：重新涨到此涨幅以上算要回封
        self.LIMIT_UP_THRESHOLD = 9.8  # 涨停阈值：涨幅超过此值算涨停
        self.MIN_AMOUNT = 50000000  # 最小成交额：5000万元
        
        logger.info(f"🚀 炸板回封监控器初始化完成")
        logger.info(f"📊 数据源: {'东方财富' if data_source == 'dc' else '新浪'}")
        logger.info(f"⚙️  策略参数: 炸板阈值<{self.ZHABAN_THRESHOLD}%, 回封阈值>{self.HUIFENG_THRESHOLD}%, 涨停阈值>{self.LIMIT_UP_THRESHOLD}%")

    def get_realtime_data(self, max_retries: int = 3) -> Optional[pd.DataFrame]:
        """获取实时股票数据"""
        for attempt in range(max_retries):
            try:
                logger.info(f"📡 正在获取实时数据... (尝试 {attempt + 1}/{max_retries})")
                df = ts.realtime_list(src=self.data_source)
                
                if df is None or df.empty:
                    logger.warning("⚠️ 未获取到实时数据")
                    if attempt < max_retries - 1:
                        logger.info("⏳ 等待3秒后重试...")
                        time.sleep(3)
                        continue
                    return None
                
                # 统一列名为小写
                df.columns = df.columns.str.lower()
                
                # 数据清理
                df = df.dropna(subset=['ts_code', 'pct_change', 'amount'])
                
                # 过滤掉成交额过小的股票
                df = df[df['amount'] >= self.MIN_AMOUNT / 10000]  # amount单位是万元
                
                # 过滤掉ST股票和特殊股票
                df = df[~df['name'].str.contains('ST|退|\\*', na=False)]
                
                # 排除创业板、科创板、北交所
                df = df[~df['ts_code'].str.startswith(('300', '688', '430', '830'))]
                
                logger.info(f"✅ 获取到 {len(df)} 只股票的实时数据")
                return df
                
            except Exception as e:
                logger.error(f"❌ 获取实时数据失败 (尝试 {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    logger.info("⏳ 等待5秒后重试...")
                    time.sleep(5)
                else:
                    logger.error("❌ 所有重试都失败，可能是非交易时间或网络问题")
        
        return None

    def get_mock_data(self) -> pd.DataFrame:
        """生成模拟数据用于测试（非交易时间使用）"""
        logger.info("🧪 生成模拟测试数据...")
        
        mock_data = []
        
        # 模拟一些炸板回封的股票
        mock_stocks = [
            {'ts_code': '000001.SZ', 'name': '平安银行', 'price': 12.50, 'pct_change': 9.2, 'amount': 800000, 'volume': 6400000, 'turnover_rate': 3.5},
            {'ts_code': '000002.SZ', 'name': '万科A', 'price': 8.90, 'pct_change': 9.5, 'amount': 1200000, 'volume': 13483146, 'turnover_rate': 4.2},
            {'ts_code': '600036.SH', 'name': '招商银行', 'price': 35.20, 'pct_change': 8.8, 'amount': 600000, 'volume': 1704545, 'turnover_rate': 2.1},
            {'ts_code': '000858.SZ', 'name': '五粮液', 'price': 168.00, 'pct_change': 9.7, 'amount': 1500000, 'volume': 892857, 'turnover_rate': 5.8},
            {'ts_code': '002594.SZ', 'name': '比亚迪', 'price': 280.50, 'pct_change': 9.1, 'amount': 2000000, 'volume': 713377, 'turnover_rate': 6.3},
        ]
        
        return pd.DataFrame(mock_stocks)

    def identify_zhaban_stocks(self, df: pd.DataFrame) -> Dict:
        """
        识别炸板股票
        
        炸板定义：
        1. 当日曾经涨停或接近涨停（涨幅 > 9.8%）
        2. 目前涨幅回落到 9.5% 以下
        """
        zhaban_stocks = {}
        current_time = datetime.now().strftime('%H:%M:%S')
        
        for _, row in df.iterrows():
            ts_code = row['ts_code']
            current_pct = row['pct_change']
            stock_name = row['name']
            
            # 检查是否曾经涨停
            if ts_code not in self.zhaban_history:
                # 首次遇到这只股票，记录当前涨幅作为最高涨幅
                if current_pct >= self.LIMIT_UP_THRESHOLD:
                    self.zhaban_history[ts_code] = {
                        'max_pct': current_pct,
                        'stock_name': stock_name,
                        'first_seen_time': current_time,
                        'is_zhaban': False
                    }
            else:
                # 更新最高涨幅
                if current_pct > self.zhaban_history[ts_code]['max_pct']:
                    self.zhaban_history[ts_code]['max_pct'] = current_pct
                
                # 检查是否构成炸板
                max_pct = self.zhaban_history[ts_code]['max_pct']
                if (max_pct >= self.LIMIT_UP_THRESHOLD and  # 曾经涨停
                    current_pct <= self.ZHABAN_THRESHOLD and  # 现在回落
                    not self.zhaban_history[ts_code].get('is_zhaban', False)):  # 尚未标记为炸板
                    
                    # 标记为炸板
                    self.zhaban_history[ts_code].update({
                        'is_zhaban': True,
                        'zhaban_time': current_time,
                        'zhaban_pct': current_pct,
                        'zhaban_from_pct': max_pct
                    })
                    
                    zhaban_stocks[ts_code] = {
                        'stock_name': stock_name,
                        'current_pct': current_pct,
                        'max_pct': max_pct,
                        'zhaban_time': current_time,
                        'price': row['price'],
                        'amount': row['amount'] * 10000  # 转为元
                    }
                    
                    logger.info(f"🔥 发现炸板: {stock_name}({ts_code}) 从{max_pct:.2f}%回落到{current_pct:.2f}%")
        
        return zhaban_stocks

    def identify_huifeng_candidates(self, df: pd.DataFrame) -> Dict:
        """
        识别回封候选股票
        
        回封定义：
        1. 已经炸板的股票
        2. 重新上涨到 9% 以上
        """
        huifeng_candidates = {}
        current_time = datetime.now().strftime('%H:%M:%S')
        
        for _, row in df.iterrows():
            ts_code = row['ts_code']
            current_pct = row['pct_change']
            stock_name = row['name']
            
            # 检查是否是已炸板且符合回封条件的股票
            if (ts_code in self.zhaban_history and 
                self.zhaban_history[ts_code].get('is_zhaban', False) and
                current_pct >= self.HUIFENG_THRESHOLD):
                
                zhaban_info = self.zhaban_history[ts_code]
                
                # 计算回封力度
                zhaban_low_pct = zhaban_info.get('zhaban_pct', current_pct)
                huifeng_strength = current_pct - zhaban_low_pct  # 从炸板低点的回升幅度
                
                huifeng_candidates[ts_code] = {
                    'stock_name': stock_name,
                    'current_pct': current_pct,
                    'max_pct': zhaban_info['max_pct'],
                    'zhaban_pct': zhaban_low_pct,
                    'huifeng_strength': huifeng_strength,
                    'zhaban_time': zhaban_info.get('zhaban_time', 'N/A'),
                    'current_time': current_time,
                    'price': row['price'],
                    'amount': row['amount'] * 10000,  # 转为元
                    'volume': row['volume'],
                    'turnover_rate': row.get('turnover_rate', 0),
                }
                
                # 新发现的回封候选
                if ts_code not in self.huifeng_candidates:
                    logger.info(f"🎯 发现回封候选: {stock_name}({ts_code}) 从{zhaban_low_pct:.2f}%回升到{current_pct:.2f}%，回封力度{huifeng_strength:.2f}%")
        
        return huifeng_candidates

    def analyze_huifeng_quality(self, candidates: Dict) -> Dict:
        """分析回封候选股票的质量"""
        quality_analysis = {}
        
        for ts_code, info in candidates.items():
            current_pct = info['current_pct']
            huifeng_strength = info['huifeng_strength']
            amount = info['amount']
            turnover_rate = info.get('turnover_rate', 0)
            
            # 计算质量评分 (0-100)
            score = 0
            
            # 涨幅接近程度评分 (0-40分)
            if current_pct >= 9.8:
                score += 40
            elif current_pct >= 9.5:
                score += 30
            elif current_pct >= 9.2:
                score += 20
            elif current_pct >= 9.0:
                score += 10
            
            # 回封力度评分 (0-25分)
            if huifeng_strength >= 2.0:
                score += 25
            elif huifeng_strength >= 1.5:
                score += 20
            elif huifeng_strength >= 1.0:
                score += 15
            elif huifeng_strength >= 0.5:
                score += 10
            
            # 成交活跃度评分 (0-20分)
            if amount >= 5e8:  # 5亿以上
                score += 20
            elif amount >= 2e8:  # 2-5亿
                score += 15
            elif amount >= 1e8:  # 1-2亿
                score += 10
            elif amount >= 5e7:  # 5000万-1亿
                score += 5
            
            # 换手率评分 (0-15分)
            if turnover_rate >= 10:
                score += 15
            elif turnover_rate >= 6:
                score += 12
            elif turnover_rate >= 3:
                score += 8
            elif turnover_rate >= 1:
                score += 4
            
            # 评级
            if score >= 80:
                grade = "A+ 极强"
            elif score >= 70:
                grade = "A 很强"
            elif score >= 60:
                grade = "B+ 较强"
            elif score >= 50:
                grade = "B 一般"
            else:
                grade = "C 较弱"
            
            quality_analysis[ts_code] = {
                **info,
                'quality_score': score,
                'grade': grade
            }
        
        return quality_analysis

    def create_notification_message(self, huifeng_data: Dict) -> str:
        """创建通知消息"""
        if not huifeng_data:
            return None
        
        current_time = datetime.now().strftime('%H:%M:%S')
        
        # 按质量评分排序
        sorted_stocks = sorted(huifeng_data.items(), 
                              key=lambda x: x[1]['quality_score'], 
                              reverse=True)
        
        content = f"""## 🚨 炸板回封监控报告 ({current_time})

✅ **发现 {len(sorted_stocks)} 只炸板回封候选股票**

### 🎯 监控策略
🔹 炸板：曾涨停后回落至9.5%以下
🔹 回封：重新上涨至9%以上
🔹 目标：捕捉二次封板机会

### 📊 回封候选股票

| 排名 | 股票名称 | 代码 | 当前涨幅 | 最高涨幅 | 回封力度 | 评分 | 评级 | 成交额 |
|------|----------|------|----------|----------|----------|------|------|--------|"""

        for i, (ts_code, info) in enumerate(sorted_stocks[:10], 1):
            stock_name = info['stock_name']
            current_pct = info['current_pct']
            max_pct = info['max_pct']
            huifeng_strength = info['huifeng_strength']
            quality_score = info['quality_score']
            grade = info['grade']
            
            # 格式化成交额
            amount = info['amount']
            if amount >= 1e8:
                amount_str = f"{amount/1e8:.1f}亿"
            else:
                amount_str = f"{amount/1e4:.0f}万"
            
            stock_code = ts_code.split('.')[0] if '.' in ts_code else ts_code
            
            # 涨幅颜色
            pct_color = "🟢" if current_pct > 0 else "🔴" if current_pct < 0 else "⚪"
            
            content += f"""
| {i} | {stock_name} | `{stock_code}` | {pct_color}{current_pct:+.2f}% | {max_pct:.2f}% | +{huifeng_strength:.2f}% | {quality_score:.0f}分 | {grade} | {amount_str} |"""
        
        # 添加详细分析
        if sorted_stocks:
            content += f"""

### 🔍 重点关注股票详情

| 股票 | 炸板时间 | 炸板涨幅 | 当前涨幅 | 价格 | 换手率 | 成交状况 |
|------|----------|----------|----------|------|--------|----------|"""
            
            for ts_code, info in sorted_stocks[:5]:
                stock_name = info['stock_name'][:6]
                zhaban_time = info.get('zhaban_time', 'N/A')
                zhaban_pct = info.get('zhaban_pct', 0)
                current_pct = info['current_pct']
                price = info['price']
                turnover_rate = info.get('turnover_rate', 0)
                amount = info['amount']
                
                amount_desc = f"{amount/1e8:.1f}亿" if amount >= 1e8 else f"{amount/1e4:.0f}万"
                
                content += f"""
| {stock_name} | {zhaban_time} | {zhaban_pct:.2f}% | {current_pct:.2f}% | {price:.2f} | {turnover_rate:.2f}% | {amount_desc} |"""
        
        content += f"""

### 📋 操作建议

| 项目 | 建议 |
|------|------|
| 🎯 **关注重点** | 评分80分以上的A+级股票 |
| 📈 **入场时机** | 涨幅突破9.5%且量能配合 |
| 🎪 **目标位** | 封板涨停或9.8%以上 |
| 🛑 **止损位** | 跌破9%或回到炸板低点 |
| ⏰ **操作周期** | 短线操作，当日或次日 |

### ⚠️ 风险提示

- 炸板股票波动较大，需要快进快出
- 关注大盘环境，避免系统性风险
- 严格控制仓位，单票不超过总仓位10%
- 设置好止损，防范二次炸板风险

---
**📊 监控统计:**
- 累计监控炸板股票：{len(self.zhaban_history)} 只
- 当前回封候选：{len(huifeng_data)} 只
- 监控开始时间：{self.monitor_start_time.strftime('%H:%M:%S')}

---
*炸板回封监控 | 数据时间: {current_time} | 仅供参考* 🚨
"""
        
        return content

    def run_single_scan(self, use_mock_data: bool = False) -> bool:
        """执行一次扫描"""
        try:
            # 获取实时数据或模拟数据
            if use_mock_data:
                realtime_df = self.get_mock_data()
                logger.info("🧪 使用模拟数据进行测试")
            else:
                realtime_df = self.get_realtime_data()
                if realtime_df is None:
                    logger.warning("⚠️ 实时数据获取失败，尝试使用模拟数据测试算法...")
                    realtime_df = self.get_mock_data()
                    if realtime_df is None:
                        return False
            
            # 识别炸板股票
            zhaban_stocks = self.identify_zhaban_stocks(realtime_df)
            
            # 识别回封候选
            huifeng_candidates = self.identify_huifeng_candidates(realtime_df)
            
            if huifeng_candidates:
                # 分析回封质量
                huifeng_analysis = self.analyze_huifeng_quality(huifeng_candidates)
                
                # 更新候选列表
                self.huifeng_candidates.update(huifeng_analysis)
                
                # 过滤高质量候选（评分>=60分）
                high_quality = {k: v for k, v in huifeng_analysis.items() 
                              if v['quality_score'] >= 60}
                
                if high_quality:
                    # 创建并发送通知
                    message = self.create_notification_message(high_quality)
                    if message:
                        logger.info("📢 发送炸板回封通知...")
                        # send_result = send_markdown_message(message)
                        # 这里暂时注释掉发送功能，可以打印消息查看效果
                        print("="*50)
                        print("通知消息预览:")
                        print(message)
                        print("="*50)
                        return True
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 扫描执行失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def run_monitor(self, interval: int = 30, duration_minutes: int = 240):
        """
        运行监控程序
        
        Args:
            interval: 扫描间隔（秒）
            duration_minutes: 监控持续时间（分钟）
        """
        logger.info(f"🚀 开始炸板回封监控...")
        logger.info(f"⏰ 扫描间隔: {interval}秒，监控时长: {duration_minutes}分钟")
        
        start_time = datetime.now()
        end_time = start_time + timedelta(minutes=duration_minutes)
        scan_count = 0
        
        try:
            while datetime.now() < end_time:
                scan_count += 1
                current_time = datetime.now().strftime('%H:%M:%S')
                
                logger.info(f"📡 第{scan_count}次扫描开始 ({current_time})")
                
                success = self.run_single_scan()
                
                if success:
                    logger.info(f"✅ 第{scan_count}次扫描完成")
                else:
                    logger.warning(f"⚠️ 第{scan_count}次扫描异常")
                
                # 等待下次扫描
                if datetime.now() < end_time:
                    logger.info(f"⏳ 等待{interval}秒后进行下次扫描...")
                    time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info("👋 用户中断监控程序")
        except Exception as e:
            logger.error(f"❌ 监控程序异常: {e}")
            
        # 输出监控总结
        total_time = datetime.now() - start_time
        logger.info(f"📊 监控总结:")
        logger.info(f"   - 总扫描次数: {scan_count}")
        logger.info(f"   - 总监控时长: {total_time}")
        logger.info(f"   - 发现炸板股票: {len(self.zhaban_history)}")
        logger.info(f"   - 发现回封候选: {len(self.huifeng_candidates)}")


def main():
    """主函数"""
    try:
        # 创建监控器
        monitor = ZhaBanHuiFengMonitor(data_source='sina')  # 使用东方财富数据源
        
        # 可以选择运行方式：
        # 1. 单次扫描测试
        # monitor.run_single_scan()
        
        # 2. 持续监控
        monitor.run_monitor(interval=30, duration_minutes=240)  # 每30秒扫描一次，监控4小时
        
        return 0
        
    except Exception as e:
        logger.error(f"❌ 程序执行失败: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    exit(main())
