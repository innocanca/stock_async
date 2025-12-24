#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询周线三连小阳及以上的千亿市值主板股票

功能：
1. 分析最近的周线走势
2. 识别连续阳线（收盘价>开盘价）
3. 筛选1000亿以上市值股票
4. 按连续阳线周数排序

使用方法：
python query_consecutive_yang_lines.py
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# 添加项目根目录到 Python 路径，确保可以导入根目录下的 database / fetcher / log_config
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(CURRENT_DIR))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from database import StockDatabase
from fetcher import StockDataFetcher

# 配置日志
from log_config import get_logger
logger = get_logger(__name__)


class ConsecutiveYangLinesAnalyzer:
    """连续阳线分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
        
        # 1000亿以上市值的知名主板股票列表（按2024年市值排序）
        self.mega_cap_stocks = {
            # 万亿级市值股票
            '600519.SH': {'name': '贵州茅台', 'market_cap': '20000+', 'industry': '白酒'},
            '601318.SH': {'name': '中国平安', 'market_cap': '15000+', 'industry': '保险'},
            '601398.SH': {'name': '工商银行', 'market_cap': '15000+', 'industry': '银行'},
            '601939.SH': {'name': '建设银行', 'market_cap': '12000+', 'industry': '银行'},
            '000858.SZ': {'name': '五粮液', 'market_cap': '8000+', 'industry': '白酒'},
            '000333.SZ': {'name': '美的集团', 'market_cap': '7000+', 'industry': '家电'},
            '002594.SZ': {'name': '比亚迪', 'market_cap': '7000+', 'industry': '新能源汽车'},
            '600036.SH': {'name': '招商银行', 'market_cap': '6000+', 'industry': '银行'},
            
            # 5000-10000亿市值股票
            '601988.SH': {'name': '中国银行', 'market_cap': '5000+', 'industry': '银行'},
            '600887.SH': {'name': '伊利股份', 'market_cap': '4000+', 'industry': '食品饮料'},
            '000001.SZ': {'name': '平安银行', 'market_cap': '3500+', 'industry': '银行'},
            '002415.SZ': {'name': '海康威视', 'market_cap': '3500+', 'industry': '安防'},
            '000002.SZ': {'name': '万科A', 'market_cap': '3000+', 'industry': '房地产'},
            '600900.SH': {'name': '长江电力', 'market_cap': '3000+', 'industry': '电力'},
            '600276.SH': {'name': '恒瑞医药', 'market_cap': '3000+', 'industry': '医药'},
            '002475.SZ': {'name': '立讯精密', 'market_cap': '2800+', 'industry': '消费电子'},
            '601166.SH': {'name': '兴业银行', 'market_cap': '2500+', 'industry': '银行'},
            '000063.SZ': {'name': '中兴通讯', 'market_cap': '2500+', 'industry': '通信设备'},
            '600030.SH': {'name': '中信证券', 'market_cap': '2500+', 'industry': '券商'},
            '002714.SZ': {'name': '牧原股份', 'market_cap': '2500+', 'industry': '农业'},
            
            # 2000-3000亿市值股票
            '601328.SH': {'name': '交通银行', 'market_cap': '2000+', 'industry': '银行'},
            '600585.SH': {'name': '海螺水泥', 'market_cap': '2000+', 'industry': '建材'},
            '000876.SZ': {'name': '新希望', 'market_cap': '2000+', 'industry': '农业'},
            '600660.SH': {'name': '福耀玻璃', 'market_cap': '2000+', 'industry': '汽车零部件'},
            '002304.SZ': {'name': '洋河股份', 'market_cap': '2000+', 'industry': '白酒'},
            '000895.SZ': {'name': '双汇发展', 'market_cap': '1800+', 'industry': '食品饮料'},
            '600809.SH': {'name': '山西汾酒', 'market_cap': '1800+', 'industry': '白酒'},
            '002032.SZ': {'name': '苏泊尔', 'market_cap': '1800+', 'industry': '家电'},
            '002241.SZ': {'name': '歌尔股份', 'market_cap': '1800+', 'industry': '消费电子'},
            '002230.SZ': {'name': '科大讯飞', 'market_cap': '1800+', 'industry': '人工智能'},
            
            # 1000-2000亿市值股票
            '600048.SH': {'name': '保利发展', 'market_cap': '1500+', 'industry': '房地产'},
            '000338.SZ': {'name': '潍柴动力', 'market_cap': '1500+', 'industry': '机械设备'},
            '601601.SH': {'name': '中国太保', 'market_cap': '1500+', 'industry': '保险'},
            '601628.SH': {'name': '中国人寿', 'market_cap': '1500+', 'industry': '保险'},
            '600028.SH': {'name': '中国石化', 'market_cap': '1500+', 'industry': '石油化工'},
            '601857.SH': {'name': '中国石油', 'market_cap': '1500+', 'industry': '石油化工'},
            '600031.SH': {'name': '三一重工', 'market_cap': '1400+', 'industry': '机械设备'},
            '002352.SZ': {'name': '顺丰控股', 'market_cap': '1400+', 'industry': '物流'},
            '000100.SZ': {'name': 'TCL科技', 'market_cap': '1300+', 'industry': '消费电子'},
            '600570.SH': {'name': '恒生电子', 'market_cap': '1300+', 'industry': '软件'},
            '002027.SZ': {'name': '分众传媒', 'market_cap': '1200+', 'industry': '传媒'},
            '002142.SZ': {'name': '宁波银行', 'market_cap': '1200+', 'industry': '银行'},
            '000157.SZ': {'name': '中联重科', 'market_cap': '1200+', 'industry': '机械设备'},
            '002202.SZ': {'name': '金风科技', 'market_cap': '1200+', 'industry': '风电'},
            '601012.SH': {'name': '隆基绿能', 'market_cap': '1200+', 'industry': '光伏'},
            '600104.SH': {'name': '上汽集团', 'market_cap': '1200+', 'industry': '汽车'},
            '000166.SZ': {'name': '申万宏源', 'market_cap': '1100+', 'industry': '券商'},
            '002236.SZ': {'name': '大华股份', 'market_cap': '1100+', 'industry': '安防'},
            '601668.SH': {'name': '中国建筑', 'market_cap': '1100+', 'industry': '建筑'},
            '600690.SH': {'name': '海尔智家', 'market_cap': '1000+', 'industry': '家电'},
        }
    
    def get_mega_cap_weekly_data(self, weeks_back: int = 12) -> Optional[pd.DataFrame]:
        """
        获取千亿市值股票的周线数据
        
        Args:
            weeks_back: 回溯周数
            
        Returns:
            pd.DataFrame: 周线数据
        """
        try:
            # 计算日期范围
            end_date = datetime.now()
            start_date = end_date - timedelta(weeks=weeks_back)
            
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            logger.info(f"获取千亿市值股票 {start_date_str} 至 {end_date_str} 的周线数据...")
            
            with self.db:
                df = self.db.query_weekly_data(
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                
                if df is None or df.empty:
                    logger.error("未找到周线数据")
                    return None
                
                # 只保留千亿市值股票
                mega_cap_df = df[df['ts_code'].isin(self.mega_cap_stocks.keys())].copy()
                
                if mega_cap_df.empty:
                    logger.error("未找到千亿市值股票的周线数据")
                    return None
                
                logger.info(f"获取到 {len(mega_cap_df)} 条千亿市值股票周线记录，涵盖 {mega_cap_df['ts_code'].nunique()} 只股票")
                return mega_cap_df
                
        except Exception as e:
            logger.error(f"获取周线数据失败: {e}")
            return None
    
    def analyze_consecutive_yang_lines(self, df: pd.DataFrame, min_consecutive: int = 3) -> pd.DataFrame:
        """
        分析连续阳线
        
        Args:
            df: 周线数据
            min_consecutive: 最少连续阳线周数
            
        Returns:
            pd.DataFrame: 包含连续阳线分析的数据
        """
        try:
            results = []
            
            for ts_code in df['ts_code'].unique():
                stock_data = df[df['ts_code'] == ts_code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                if len(stock_data) < min_consecutive:
                    continue
                
                # 判断是否为阳线：收盘价 > 开盘价
                stock_data['is_yang'] = stock_data['close'] > stock_data['open']
                
                # 计算从最新一周开始往前的连续阳线数量
                consecutive_yang = 0
                for i in range(len(stock_data) - 1, -1, -1):
                    if stock_data.iloc[i]['is_yang']:
                        consecutive_yang += 1
                    else:
                        break
                
                # 只保留达到最少连续阳线要求的股票
                if consecutive_yang >= min_consecutive:
                    latest_record = stock_data.iloc[-1]
                    stock_info = self.mega_cap_stocks.get(ts_code, {})
                    
                    # 计算最近几周的涨跌幅
                    recent_weeks = min(consecutive_yang, len(stock_data))
                    start_price = stock_data.iloc[-recent_weeks]['open']
                    end_price = latest_record['close']
                    total_return = ((end_price - start_price) / start_price * 100) if start_price > 0 else 0
                    
                    results.append({
                        'ts_code': ts_code,
                        'stock_name': stock_info.get('name', '未知'),
                        'market_cap': stock_info.get('market_cap', '未知'),
                        'industry': stock_info.get('industry', '其他'),
                        'consecutive_yang_weeks': consecutive_yang,
                        'latest_trade_date': latest_record['trade_date'],
                        'latest_close': latest_record['close'],
                        'latest_open': latest_record['open'],
                        'latest_pct_chg': latest_record['pct_chg'],
                        'latest_vol': latest_record['vol'],
                        'latest_amount': latest_record['amount'],
                        'total_return_during_yang': total_return,
                        'avg_weekly_return': total_return / consecutive_yang if consecutive_yang > 0 else 0
                    })
            
            if not results:
                logger.warning(f"未找到连续{min_consecutive}周以上阳线的千亿市值股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            # 按连续阳线周数排序，然后按市值排序
            result_df = result_df.sort_values(['consecutive_yang_weeks', 'total_return_during_yang'], ascending=[False, False])
            
            logger.info(f"找到 {len(result_df)} 只连续阳线的千亿市值股票")
            return result_df
            
        except Exception as e:
            logger.error(f"分析连续阳线失败: {e}")
            return pd.DataFrame()
    
    def analyze_mega_cap_yang_lines(self) -> Optional[pd.DataFrame]:
        """
        主分析函数：找到周线三连阳及以上的千亿市值主板股票
        
        Returns:
            pd.DataFrame: 分析结果
        """
        results = self.get_analysis_results(min_consecutive=3)
        if not results:
            return pd.DataFrame()
        return pd.DataFrame(results)

    def get_analysis_results(self, min_consecutive: int = 3) -> List[Dict]:
        """
        获取分析结果列表，供 API 调用。
        """
        try:
            logger.info(f"🔍 开始分析周线连续阳线：最少 {min_consecutive} 周...")
            logger.info(f"📊 分析范围：{len(self.mega_cap_stocks)} 只千亿市值股票")
            
            # 1. 获取千亿市值股票的周线数据
            weekly_df = self.get_mega_cap_weekly_data(weeks_back=12)
            if weekly_df is None or weekly_df.empty:
                return []
            
            # 2. 分析连续阳线
            result_df = self.analyze_consecutive_yang_lines(weekly_df, min_consecutive=min_consecutive)
            
            # 如果 3 周没有结果，自动尝试 2 周
            if result_df.empty and min_consecutive >= 3:
                logger.info(f"未找到连续 {min_consecutive} 周阳线，尝试降低到 2 周...")
                result_df = self.analyze_consecutive_yang_lines(weekly_df, min_consecutive=2)
            
            if result_df.empty:
                return []
            
            # 3. 转换数值类型为标准 Python 类型，避免 JSON 序列化错误
            records = result_df.to_dict(orient="records")
            for r in records:
                for k, v in r.items():
                    if pd.isna(v):
                        r[k] = None
                    elif hasattr(v, 'item'): # numpy types
                        r[k] = v.item()
            
            return records
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            return []


def display_yang_lines_results(df: pd.DataFrame):
    """
    显示连续阳线分析结果
    
    Args:
        df: 分析结果数据
    """
    if df is None or df.empty:
        logger.error("❌ 未找到符合条件的千亿市值股票")
        return
    
    logger.info("📋 符合条件的千亿市值股票列表（周线连续阳线）：")
    logger.info("=" * 130)
    logger.info(f"{'排名':<4} {'股票代码':<12} {'股票名称':<12} {'市值(亿)':<10} {'行业':<12} {'连续阳线':<8} {'最新价':<8} {'总涨幅%':<8} {'周均涨幅%':<10}")
    logger.info("=" * 130)
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        logger.info(
            f"{i:<4} "
            f"{row['ts_code']:<12} "
            f"{row['stock_name']:<12} "
            f"{row['market_cap']:<10} "
            f"{row['industry']:<12} "
            f"{row['consecutive_yang_weeks']:<8}周 "
            f"{row['latest_close']:<8.2f} "
            f"{row['total_return_during_yang']:<8.2f} "
            f"{row['avg_weekly_return']:<10.2f}"
        )
    
    logger.info("=" * 130)
    logger.info(f"总共找到 {len(df)} 只符合条件的千亿市值股票")
    
    # 统计信息
    logger.info(f"\n📊 统计信息：")
    logger.info(f"   平均连续阳线周数: {df['consecutive_yang_weeks'].mean():.1f}周")
    logger.info(f"   最多连续阳线周数: {df['consecutive_yang_weeks'].max()}周")
    logger.info(f"   平均连续阳线期间涨幅: {df['total_return_during_yang'].mean():.2f}%")
    logger.info(f"   平均周涨幅: {df['avg_weekly_return'].mean():.2f}%")
    
    # 行业分布
    if 'industry' in df.columns:
        industry_counts = df['industry'].value_counts()
        logger.info(f"\n📈 行业分布：")
        for industry, count in industry_counts.items():
            logger.info(f"   {industry}: {count} 只")
    
    # 市值分布
    mega_cap_count = len(df[df['market_cap'].str.contains('5000\\+|8000\\+|15000\\+|20000\\+', na=False)])
    large_cap_count = len(df) - mega_cap_count
    logger.info(f"\n💰 市值分布：")
    logger.info(f"   超大市值(5000亿+): {mega_cap_count} 只")
    logger.info(f"   大市值(1000-5000亿): {large_cap_count} 只")


def main():
    """主函数"""
    logger.info("🚀 开始查询周线三连阳及以上的千亿市值主板股票...")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        analyzer = ConsecutiveYangLinesAnalyzer()
        result_df = analyzer.analyze_mega_cap_yang_lines()
        
        if result_df is not None and not result_df.empty:
            display_yang_lines_results(result_df)
            
            # 保存结果到文件
            output_file = f"consecutive_yang_lines_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"\n💾 结果已保存到文件: {output_file}")
            
            logger.info("\n💡 投资策略解读：")
            logger.info("   ✅ 连续阳线表明股价处于上升趋势")
            logger.info("   ✅ 千亿市值确保了足够的流动性和稳定性")
            logger.info("   ✅ 主板股票通常基本面较为扎实")
            logger.info("   ⚠️  注意观察是否到达阻力位")
            logger.info("   ⚠️  建议结合成交量变化进行分析")
            logger.info("   ⚠️  关注市场整体走势和板块轮动")
            
        else:
            logger.error("❌ 未找到符合条件的千亿市值股票，可能原因：")
            logger.error("   1. 近期市场整体调整，连续阳线股票较少")
            logger.error("   2. 大市值股票走势相对稳健，很少出现连续强势上涨")
            logger.error("   3. 可以考虑降低连续阳线周数要求（如2周）")
            
    except Exception as e:
        logger.error(f"❌ 查询过程发生异常: {e}")
        return False
        
    finally:
        end_time = datetime.now()
        total_duration = end_time - start_time
        logger.info(f"\n⏰ 查询总耗时: {total_duration}")
    
    return True


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)
