#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询周线放量且处于相对低位的600亿+市值主板股票

功能：
1. 分析周线成交量放大情况
2. 识别处于相对低位的股票
3. 筛选600亿以上市值股票
4. 只包含主板股票

使用方法：
python query_volume_low_position_stocks.py
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


class VolumeLowPositionAnalyzer:
    """周线放量低位股票分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
        
        # 600亿以上市值的主板股票列表（2024年数据）
        self.large_cap_stocks = {
            # 万亿级市值（5000亿+）
            '600519.SH': {'name': '贵州茅台', 'market_cap': 20000, 'industry': '白酒'},
            '601318.SH': {'name': '中国平安', 'market_cap': 15000, 'industry': '保险'},
            '601398.SH': {'name': '工商银行', 'market_cap': 15000, 'industry': '银行'},
            '601939.SH': {'name': '建设银行', 'market_cap': 12000, 'industry': '银行'},
            '000858.SZ': {'name': '五粮液', 'market_cap': 8000, 'industry': '白酒'},
            '000333.SZ': {'name': '美的集团', 'market_cap': 7000, 'industry': '家电'},
            '002594.SZ': {'name': '比亚迪', 'market_cap': 7000, 'industry': '新能源汽车'},
            '600036.SH': {'name': '招商银行', 'market_cap': 6000, 'industry': '银行'},
            '601988.SH': {'name': '中国银行', 'market_cap': 5000, 'industry': '银行'},
            
            # 3000-5000亿市值
            '600887.SH': {'name': '伊利股份', 'market_cap': 4000, 'industry': '食品饮料'},
            '000001.SZ': {'name': '平安银行', 'market_cap': 3500, 'industry': '银行'},
            '002415.SZ': {'name': '海康威视', 'market_cap': 3500, 'industry': '安防'},
            '000002.SZ': {'name': '万科A', 'market_cap': 3000, 'industry': '房地产'},
            '600900.SH': {'name': '长江电力', 'market_cap': 3000, 'industry': '电力'},
            '600276.SH': {'name': '恒瑞医药', 'market_cap': 3000, 'industry': '医药'},
            '002475.SZ': {'name': '立讯精密', 'market_cap': 2800, 'industry': '消费电子'},
            
            # 2000-3000亿市值
            '601166.SH': {'name': '兴业银行', 'market_cap': 2500, 'industry': '银行'},
            '000063.SZ': {'name': '中兴通讯', 'market_cap': 2500, 'industry': '通信设备'},
            '600030.SH': {'name': '中信证券', 'market_cap': 2500, 'industry': '券商'},
            '002714.SZ': {'name': '牧原股份', 'market_cap': 2500, 'industry': '农业'},
            '601328.SH': {'name': '交通银行', 'market_cap': 2000, 'industry': '银行'},
            '600585.SH': {'name': '海螺水泥', 'market_cap': 2000, 'industry': '建材'},
            '000895.SZ': {'name': '双汇发展', 'market_cap': 1800, 'industry': '食品饮料'},
            '600809.SH': {'name': '山西汾酒', 'market_cap': 1800, 'industry': '白酒'},
            '002304.SZ': {'name': '洋河股份', 'market_cap': 2000, 'industry': '白酒'},
            
            # 1000-2000亿市值
            '600048.SH': {'name': '保利发展', 'market_cap': 1500, 'industry': '房地产'},
            '000338.SZ': {'name': '潍柴动力', 'market_cap': 1500, 'industry': '机械设备'},
            '601601.SH': {'name': '中国太保', 'market_cap': 1500, 'industry': '保险'},
            '601628.SH': {'name': '中国人寿', 'market_cap': 1500, 'industry': '保险'},
            '600028.SH': {'name': '中国石化', 'market_cap': 1500, 'industry': '石油化工'},
            '601857.SH': {'name': '中国石油', 'market_cap': 1500, 'industry': '石油化工'},
            '600031.SH': {'name': '三一重工', 'market_cap': 1400, 'industry': '机械设备'},
            '002352.SZ': {'name': '顺丰控股', 'market_cap': 1400, 'industry': '物流'},
            '000100.SZ': {'name': 'TCL科技', 'market_cap': 1300, 'industry': '消费电子'},
            '600570.SH': {'name': '恒生电子', 'market_cap': 1300, 'industry': '软件'},
            '002027.SZ': {'name': '分众传媒', 'market_cap': 1200, 'industry': '传媒'},
            '002142.SZ': {'name': '宁波银行', 'market_cap': 1200, 'industry': '银行'},
            '000157.SZ': {'name': '中联重科', 'market_cap': 1200, 'industry': '机械设备'},
            '002202.SZ': {'name': '金风科技', 'market_cap': 1200, 'industry': '风电'},
            '601012.SH': {'name': '隆基绿能', 'market_cap': 1200, 'industry': '光伏'},
            '600104.SH': {'name': '上汽集团', 'market_cap': 1200, 'industry': '汽车'},
            '000166.SZ': {'name': '申万宏源', 'market_cap': 1100, 'industry': '券商'},
            '002236.SZ': {'name': '大华股份', 'market_cap': 1100, 'industry': '安防'},
            '601668.SH': {'name': '中国建筑', 'market_cap': 1100, 'industry': '建筑'},
            '600690.SH': {'name': '海尔智家', 'market_cap': 1000, 'industry': '家电'},
            
            # 600-1000亿市值
            '000876.SZ': {'name': '新希望', 'market_cap': 800, 'industry': '农业'},
            '000858.SZ': {'name': '五粮液', 'market_cap': 8000, 'industry': '白酒'},  # 重复了，修正
            '600132.SH': {'name': '重庆啤酒', 'market_cap': 900, 'industry': '食品饮料'},
            '000596.SZ': {'name': '古井贡酒', 'market_cap': 800, 'industry': '白酒'},
            '600600.SH': {'name': '青岛啤酒', 'market_cap': 900, 'industry': '食品饮料'},
            '000568.SZ': {'name': '泸州老窖', 'market_cap': 900, 'industry': '白酒'},
            '600519.SH': {'name': '贵州茅台', 'market_cap': 20000, 'industry': '白酒'},  # 重复了
            '000999.SZ': {'name': '华润三九', 'market_cap': 700, 'industry': '医药'},
            '000661.SZ': {'name': '长春高新', 'market_cap': 800, 'industry': '医药'},
            '600660.SH': {'name': '福耀玻璃', 'market_cap': 700, 'industry': '汽车零部件'},
            '002008.SZ': {'name': '大族激光', 'market_cap': 600, 'industry': '机械设备'},
            '002129.SZ': {'name': '中环股份', 'market_cap': 600, 'industry': '半导体'},
        }
        
        # 去除重复，只保留600亿以上的
        self.filtered_stocks = {k: v for k, v in self.large_cap_stocks.items() 
                               if v['market_cap'] >= 600}
    
    def get_weekly_data(self, weeks_back: int = 20) -> Optional[pd.DataFrame]:
        """
        获取大市值股票的周线数据
        
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
            
            logger.info(f"获取600亿+市值股票 {start_date_str} 至 {end_date_str} 的周线数据...")
            
            with self.db:
                df = self.db.query_weekly_data(
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                
                if df is None or df.empty:
                    logger.error("未找到周线数据")
                    return None
                
                # 筛选主板股票
                main_board_df = self.filter_main_board_stocks(df)
                
                # 只保留600亿以上市值股票
                large_cap_df = main_board_df[main_board_df['ts_code'].isin(self.filtered_stocks.keys())].copy()
                
                if large_cap_df.empty:
                    logger.error("未找到600亿+市值股票的周线数据")
                    return None
                
                logger.info(f"获取到 {len(large_cap_df)} 条600亿+市值股票周线记录，涵盖 {large_cap_df['ts_code'].nunique()} 只股票")
                return large_cap_df
                
        except Exception as e:
            logger.error(f"获取周线数据失败: {e}")
            return None
    
    def filter_main_board_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        筛选主板股票
        
        Args:
            df: 周线数据
            
        Returns:
            pd.DataFrame: 主板股票数据
        """
        try:
            import re
            
            def is_main_board(ts_code):
                # 沪市主板：600xxx, 601xxx, 603xxx, 605xxx
                # 深市主板：000xxx, 001xxx, 002xxx
                main_board_patterns = [
                    r'^60[0135]\d{3}\.SH$',  # 沪市主板
                    r'^00[012]\d{3}\.SZ$'   # 深市主板和中小板
                ]
                
                for pattern in main_board_patterns:
                    if re.match(pattern, ts_code):
                        return True
                return False
            
            # 筛选主板股票
            main_board_df = df[df['ts_code'].apply(is_main_board)].copy()
            
            logger.info(f"筛选出 {main_board_df['ts_code'].nunique()} 只主板股票")
            return main_board_df
            
        except Exception as e:
            logger.error(f"筛选主板股票失败: {e}")
            return df
    
    def analyze_volume_surge(self, df: pd.DataFrame, min_volume_ratio: float = 1.5) -> pd.DataFrame:
        """
        分析成交量放大情况
        
        Args:
            df: 周线数据
            min_volume_ratio: 最小成交量放大倍数
            
        Returns:
            pd.DataFrame: 包含成交量分析的数据
        """
        try:
            results = []
            
            for ts_code in df['ts_code'].unique():
                stock_data = df[df['ts_code'] == ts_code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                if len(stock_data) < 8:  # 至少需要8周数据
                    continue
                
                # 最近2周的平均成交量
                recent_2weeks = stock_data.tail(2)['vol'].mean()
                
                # 之前10周的平均成交量（作为基准）
                if len(stock_data) >= 12:
                    baseline_weeks = stock_data.iloc[-12:-2]['vol'].mean()
                else:
                    baseline_weeks = stock_data.iloc[:-2]['vol'].mean()
                
                if baseline_weeks > 0:
                    volume_ratio = recent_2weeks / baseline_weeks
                    
                    # 只保留成交量放大的股票
                    if volume_ratio >= min_volume_ratio:
                        latest_record = stock_data.iloc[-1]
                        stock_info = self.filtered_stocks.get(ts_code, {})
                        
                        results.append({
                            'ts_code': ts_code,
                            'stock_name': stock_info.get('name', '未知'),
                            'market_cap': stock_info.get('market_cap', 0),
                            'industry': stock_info.get('industry', '其他'),
                            'latest_trade_date': latest_record['trade_date'],
                            'latest_close': latest_record['close'],
                            'latest_vol': latest_record['vol'],
                            'recent_2weeks_avg_vol': recent_2weeks,
                            'baseline_avg_vol': baseline_weeks,
                            'volume_surge_ratio': volume_ratio,
                            'latest_pct_chg': latest_record['pct_chg'],
                            'latest_amount': latest_record['amount'],
                            'stock_data': stock_data  # 保留完整数据用于后续分析
                        })
            
            if not results:
                logger.warning(f"未找到成交量放大{min_volume_ratio}倍以上的股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('volume_surge_ratio', ascending=False)
            
            logger.info(f"找到 {len(result_df)} 只成交量放大的600亿+市值股票")
            return result_df
            
        except Exception as e:
            logger.error(f"分析成交量放大失败: {e}")
            return pd.DataFrame()
    
    def analyze_low_position(self, df: pd.DataFrame, lookback_weeks: int = 16) -> pd.DataFrame:
        """
        分析是否处于相对低位
        
        Args:
            df: 包含成交量分析的数据
            lookback_weeks: 回望周数来判断相对低位
            
        Returns:
            pd.DataFrame: 包含低位分析的数据
        """
        try:
            low_position_results = []
            
            for _, row in df.iterrows():
                stock_data = row['stock_data']
                
                if len(stock_data) < lookback_weeks:
                    continue
                
                # 分析最近16周的价格分布
                recent_data = stock_data.tail(lookback_weeks)
                
                highest_price = recent_data['high'].max()
                lowest_price = recent_data['low'].min()
                current_price = row['latest_close']
                
                # 计算当前价格在区间中的位置（0表示最低点，1表示最高点）
                if highest_price > lowest_price:
                    price_position = (current_price - lowest_price) / (highest_price - lowest_price)
                else:
                    price_position = 0.5  # 如果没有波动，设为中位
                
                # 计算距离最高点的跌幅
                decline_from_high = (highest_price - current_price) / highest_price * 100
                
                # 判断是否处于相对低位（价格位置在30%以下，或者从高点下跌超过20%）
                is_low_position = price_position <= 0.3 or decline_from_high >= 20
                
                if is_low_position:
                    # 计算技术指标
                    ma_short = recent_data.tail(4)['close'].mean()  # 4周均线
                    ma_long = recent_data.tail(8)['close'].mean()   # 8周均线
                    
                    low_position_results.append({
                        'ts_code': row['ts_code'],
                        'stock_name': row['stock_name'],
                        'market_cap': row['market_cap'],
                        'industry': row['industry'],
                        'latest_close': row['latest_close'],
                        'volume_surge_ratio': row['volume_surge_ratio'],
                        'latest_pct_chg': row['latest_pct_chg'],
                        'latest_amount': row['latest_amount'],
                        'price_position': price_position,
                        'decline_from_high': decline_from_high,
                        'highest_price_16w': highest_price,
                        'lowest_price_16w': lowest_price,
                        'ma_4w': ma_short,
                        'ma_8w': ma_long,
                        'relative_to_ma4w': (current_price - ma_short) / ma_short * 100,
                        'relative_to_ma8w': (current_price - ma_long) / ma_long * 100
                    })
            
            if not low_position_results:
                logger.warning("未找到处于相对低位的股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(low_position_results)
            # 按价格位置排序（越低越排前面）
            result_df = result_df.sort_values(['price_position', 'volume_surge_ratio'], ascending=[True, False])
            
            logger.info(f"找到 {len(result_df)} 只处于相对低位的股票")
            return result_df
            
        except Exception as e:
            logger.error(f"分析相对低位失败: {e}")
            return pd.DataFrame()
    
    def analyze_volume_low_position(self) -> Optional[pd.DataFrame]:
        """
        主分析函数：找到周线放量且处于相对低位的600亿+市值股票
        
        Returns:
            pd.DataFrame: 分析结果
        """
        try:
            logger.info("🔍 开始分析周线放量且处于相对低位的600亿+市值股票...")
            logger.info(f"📊 分析范围：{len(self.filtered_stocks)} 只600亿+市值股票")
            
            # 1. 获取周线数据
            weekly_df = self.get_weekly_data(weeks_back=20)
            if weekly_df is None or weekly_df.empty:
                return None
            
            # 2. 分析成交量放大
            volume_surge_df = self.analyze_volume_surge(weekly_df, min_volume_ratio=1.5)
            if volume_surge_df.empty:
                logger.error("未找到成交量放大的股票")
                return None
            
            # 3. 分析相对低位
            low_position_df = self.analyze_low_position(volume_surge_df, lookback_weeks=16)
            if low_position_df.empty:
                logger.error("未找到处于相对低位的股票")
                return None
            
            logger.info(f"✅ 找到 {len(low_position_df)} 只符合条件的股票")
            return low_position_df
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            return None


def display_results(df: pd.DataFrame):
    """
    显示分析结果
    
    Args:
        df: 分析结果数据
    """
    if df is None or df.empty:
        logger.error("❌ 未找到符合条件的股票")
        return
    
    logger.info("📋 符合条件的股票列表（周线放量+相对低位+600亿+市值）：")
    logger.info("=" * 140)
    logger.info(f"{'排名':<4} {'股票代码':<12} {'股票名称':<12} {'市值(亿)':<8} {'行业':<12} {'最新价':<8} "
               f"{'成交量倍数':<10} {'价格位置':<8} {'距高点跌幅%':<12} {'相对4周均线%':<12}")
    logger.info("=" * 140)
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        logger.info(
            f"{i:<4} "
            f"{row['ts_code']:<12} "
            f"{row['stock_name']:<12} "
            f"{row['market_cap']:<8.0f} "
            f"{row['industry']:<12} "
            f"{row['latest_close']:<8.2f} "
            f"{row['volume_surge_ratio']:<10.2f} "
            f"{row['price_position']:<8.1%} "
            f"{row['decline_from_high']:<12.1f} "
            f"{row['relative_to_ma4w']:<12.1f}"
        )
    
    logger.info("=" * 140)
    logger.info(f"总共找到 {len(df)} 只符合条件的股票")
    
    # 统计信息
    logger.info(f"\n📊 统计信息：")
    logger.info(f"   平均市值: {df['market_cap'].mean():.0f}亿元")
    logger.info(f"   平均成交量放大倍数: {df['volume_surge_ratio'].mean():.2f}")
    logger.info(f"   平均价格位置: {df['price_position'].mean():.1%}")
    logger.info(f"   平均距高点跌幅: {df['decline_from_high'].mean():.1f}%")
    
    # 行业分布
    if 'industry' in df.columns:
        industry_counts = df['industry'].value_counts()
        logger.info(f"\n📈 行业分布：")
        for industry, count in industry_counts.items():
            logger.info(f"   {industry}: {count} 只")
    
    # 投资提示
    logger.info(f"\n💡 投资提示：")
    logger.info("   🔍 价格位置：数值越小表示越接近低点")
    logger.info("   📊 成交量倍数：表示最近2周相对前期的放大倍数")
    logger.info("   📉 距高点跌幅：正值表示从高点回调的幅度")
    logger.info("   ⚠️  建议结合基本面和技术面进一步分析")


def main():
    """主函数"""
    logger.info("🚀 开始查询周线放量且处于相对低位的600亿+市值股票...")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        analyzer = VolumeLowPositionAnalyzer()
        result_df = analyzer.analyze_volume_low_position()
        
        if result_df is not None and not result_df.empty:
            display_results(result_df)
            
            # 保存结果到文件
            output_file = f"volume_low_position_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"\n💾 结果已保存到文件: {output_file}")
            
            logger.info("\n💡 投资策略解读：")
            logger.info("   ✅ 周线放量：表明有资金关注，可能有重要变化")
            logger.info("   ✅ 相对低位：价格处于近期区间低位，安全边际较高")
            logger.info("   ✅ 600亿+市值：流动性好，基本面相对稳健")
            logger.info("   ✅ 主板股票：规范性好，信息透明度高")
            logger.info("   ⚠️  建议关注放量原因和基本面变化")
            
        else:
            logger.error("❌ 未找到符合条件的股票，可能原因：")
            logger.error("   1. 近期大市值股票成交量相对稳定")
            logger.error("   2. 大部分股票不在相对低位")
            logger.error("   3. 可以适当降低筛选标准")
            
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

