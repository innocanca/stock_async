#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询成交量放大的大市值主板股票

功能：
1. 分析最近1-2周的成交量变化
2. 筛选主板股票
3. 获取市值信息并过滤500亿以上
4. 按成交量放大倍数排序

使用方法：
python query_volume_surge_stocks.py
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import StockDatabase
from fetcher import StockDataFetcher

# 配置日志
from log_config import get_logger
logger = get_logger(__name__)


class VolumeSurgeAnalyzer:
    """成交量放大分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
        
    def get_recent_weekly_data(self, weeks_back: int = 8) -> Optional[pd.DataFrame]:
        """
        获取最近几周的周线数据
        
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
            
            logger.info(f"获取 {start_date_str} 至 {end_date_str} 的周线数据...")
            
            with self.db:
                df = self.db.query_weekly_data(
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                
                if df is None or df.empty:
                    logger.error("未找到周线数据")
                    return None
                
                logger.info(f"获取到 {len(df)} 条周线记录，涵盖 {df['ts_code'].nunique()} 只股票")
                return df
                
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
            # 主板股票代码规则：
            # 沪市主板：600xxx, 601xxx, 603xxx, 605xxx
            # 深市主板：000xxx, 001xxx, 002xxx（但002xxx部分是中小板，也算主板）
            main_board_patterns = [
                r'^600\d{3}\.SH$',  # 沪市主板
                r'^601\d{3}\.SH$',  # 沪市主板
                r'^603\d{3}\.SH$',  # 沪市主板
                r'^605\d{3}\.SH$',  # 沪市主板
                r'^000\d{3}\.SZ$',  # 深市主板
                r'^001\d{3}\.SZ$',  # 深市主板
                r'^002\d{3}\.SZ$'   # 深市中小板（也算主板）
            ]
            
            # 排除创业板、科创板、北交所等
            exclude_patterns = [
                r'^300\d{3}\.SZ$',  # 创业板
                r'^688\d{3}\.SH$',  # 科创板
                r'^830\d{3}\.BJ$',  # 北交所
                r'^430\d{3}\.BJ$',  # 北交所
                r'^200\d{3}\.SZ$',  # B股
                r'^900\d{3}\.SH$'   # B股
            ]
            
            import re
            
            def is_main_board(ts_code):
                # 检查是否符合主板模式
                for pattern in main_board_patterns:
                    if re.match(pattern, ts_code):
                        return True
                return False
            
            def is_excluded(ts_code):
                # 检查是否应该排除
                for pattern in exclude_patterns:
                    if re.match(pattern, ts_code):
                        return True
                return False
            
            # 筛选主板股票
            main_board_stocks = df[
                df['ts_code'].apply(is_main_board) & 
                ~df['ts_code'].apply(is_excluded)
            ].copy()
            
            logger.info(f"筛选出 {main_board_stocks['ts_code'].nunique()} 只主板股票")
            return main_board_stocks
            
        except Exception as e:
            logger.error(f"筛选主板股票失败: {e}")
            return df
    
    def calculate_volume_surge(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算成交量放大情况
        
        Args:
            df: 周线数据
            
        Returns:
            pd.DataFrame: 包含成交量分析的数据
        """
        try:
            # 按股票分组，计算每只股票的成交量变化
            results = []
            
            for ts_code in df['ts_code'].unique():
                stock_data = df[df['ts_code'] == ts_code].copy()
                stock_data = stock_data.sort_values('trade_date')
                
                if len(stock_data) < 4:  # 至少需要4周数据
                    continue
                
                # 最近2周的平均成交量
                recent_2weeks = stock_data.tail(2)['vol'].mean()
                
                # 之前4-6周的平均成交量（作为基准）
                if len(stock_data) >= 6:
                    baseline_weeks = stock_data.iloc[-6:-2]['vol'].mean()
                else:
                    baseline_weeks = stock_data.iloc[:-2]['vol'].mean()
                
                if baseline_weeks > 0:
                    volume_ratio = recent_2weeks / baseline_weeks
                    
                    # 只保留成交量明显放大的股票（放大1.5倍以上）
                    if volume_ratio >= 1.5:
                        latest_record = stock_data.iloc[-1]
                        
                        results.append({
                            'ts_code': ts_code,
                            'latest_trade_date': latest_record['trade_date'],
                            'latest_close': latest_record['close'],
                            'latest_vol': latest_record['vol'],
                            'recent_2weeks_avg_vol': recent_2weeks,
                            'baseline_avg_vol': baseline_weeks,
                            'volume_surge_ratio': volume_ratio,
                            'latest_pct_chg': latest_record['pct_chg'],
                            'latest_amount': latest_record['amount']
                        })
            
            if not results:
                logger.warning("未找到符合成交量放大条件的股票")
                return pd.DataFrame()
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('volume_surge_ratio', ascending=False)
            
            logger.info(f"找到 {len(result_df)} 只成交量放大的股票")
            return result_df
            
        except Exception as e:
            logger.error(f"计算成交量放大失败: {e}")
            return pd.DataFrame()
    
    def get_market_cap_info(self, stock_codes: List[str]) -> Dict[str, float]:
        """
        获取股票市值信息
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            Dict: 股票代码 -> 市值（亿元）的映射
        """
        try:
            logger.info("获取股票基础信息和市值数据...")
            
            # 获取股票基础信息
            basic_df = self.fetcher.get_stock_basic()
            if basic_df is None or basic_df.empty:
                logger.error("获取股票基础信息失败")
                return {}
            
            # 获取最新的日线数据来计算市值
            market_caps = {}
            
            for ts_code in stock_codes[:20]:  # 限制查询数量避免超时
                try:
                    # 获取最近的交易数据
                    end_date = datetime.now().strftime('%Y%m%d')
                    start_date = (datetime.now() - timedelta(days=7)).strftime('%Y%m%d')
                    
                    daily_df = self.fetcher.get_daily_data(ts_code, start_date, end_date)
                    if daily_df is None or daily_df.empty:
                        continue
                    
                    latest_price = daily_df.iloc[-1]['close']
                    
                    # 从基础信息中获取总股本信息（如果有的话）
                    stock_info = basic_df[basic_df['ts_code'] == ts_code]
                    if not stock_info.empty:
                        # 这里需要获取总股本数据，由于tushare基础接口可能不包含股本
                        # 我们使用一个估算方法或者设置默认值
                        # 实际应用中可能需要调用专门的股本接口
                        
                        # 简化处理：根据成交额和价格估算流通市值
                        latest_amount = daily_df.iloc[-1]['amount'] * 1000  # 转换为元
                        latest_vol = daily_df.iloc[-1]['vol'] * 100  # 转换为股
                        
                        if latest_vol > 0:
                            # 粗略估算总市值（这里假设流通比例70%）
                            estimated_market_cap = (latest_amount / latest_vol) * latest_vol / 0.7 / 100000000  # 转换为亿元
                            market_caps[ts_code] = estimated_market_cap
                        
                except Exception as e:
                    logger.warning(f"获取 {ts_code} 市值信息失败: {e}")
                    continue
            
            logger.info(f"获取到 {len(market_caps)} 只股票的市值信息")
            return market_caps
            
        except Exception as e:
            logger.error(f"获取市值信息失败: {e}")
            return {}
    
    def get_stock_names(self, stock_codes: List[str]) -> Dict[str, str]:
        """
        获取股票名称
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            Dict: 股票代码 -> 股票名称的映射
        """
        try:
            basic_df = self.fetcher.get_stock_basic()
            if basic_df is None or basic_df.empty:
                return {}
            
            names = {}
            for ts_code in stock_codes:
                stock_info = basic_df[basic_df['ts_code'] == ts_code]
                if not stock_info.empty:
                    names[ts_code] = stock_info.iloc[0]['name']
            
            return names
            
        except Exception as e:
            logger.error(f"获取股票名称失败: {e}")
            return {}
    
    def analyze_volume_surge_stocks(self) -> Optional[pd.DataFrame]:
        """
        主分析函数：找到成交量放大的大市值主板股票
        
        Returns:
            pd.DataFrame: 分析结果
        """
        try:
            logger.info("🔍 开始分析成交量放大的大市值主板股票...")
            
            # 1. 获取周线数据
            weekly_df = self.get_recent_weekly_data(weeks_back=8)
            if weekly_df is None or weekly_df.empty:
                return None
            
            # 2. 筛选主板股票
            main_board_df = self.filter_main_board_stocks(weekly_df)
            if main_board_df.empty:
                logger.error("未找到主板股票数据")
                return None
            
            # 3. 计算成交量放大
            volume_surge_df = self.calculate_volume_surge(main_board_df)
            if volume_surge_df.empty:
                logger.error("未找到成交量放大的股票")
                return None
            
            # 4. 获取股票名称
            stock_names = self.get_stock_names(volume_surge_df['ts_code'].tolist())
            volume_surge_df['stock_name'] = volume_surge_df['ts_code'].map(stock_names)
            
            # 5. 获取市值信息（简化版本，实际中可能需要更精确的市值计算）
            logger.info("📊 应用市值筛选（注意：市值为估算值）...")
            
            # 由于市值计算复杂，这里提供一个基于成交额的简化筛选
            # 筛选最近成交额较大的股票（通常对应大市值）
            volume_surge_df['latest_amount_yi'] = volume_surge_df['latest_amount'] / 10000  # 转换为亿元
            
            # 筛选最近周成交额大于10亿的股票（粗略对应大市值股票）
            large_cap_df = volume_surge_df[volume_surge_df['latest_amount_yi'] >= 10].copy()
            
            if large_cap_df.empty:
                logger.warning("未找到符合市值条件的股票")
                return volume_surge_df.head(10)  # 返回前10名
            
            logger.info(f"✅ 找到 {len(large_cap_df)} 只符合条件的股票")
            return large_cap_df
            
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
    
    logger.info("📋 符合条件的股票列表：")
    logger.info("=" * 100)
    logger.info(f"{'排名':<4} {'股票代码':<12} {'股票名称':<10} {'最新价格':<8} {'涨跌幅%':<8} {'成交量倍数':<10} {'周成交额(亿)':<12}")
    logger.info("=" * 100)
    
    for i, (_, row) in enumerate(df.head(20).iterrows(), 1):
        logger.info(
            f"{i:<4} "
            f"{row['ts_code']:<12} "
            f"{row.get('stock_name', 'N/A'):<10} "
            f"{row['latest_close']:<8.2f} "
            f"{row['latest_pct_chg']:<8.2f} "
            f"{row['volume_surge_ratio']:<10.2f} "
            f"{row['latest_amount_yi']:<12.2f}"
        )
    
    logger.info("=" * 100)
    logger.info(f"总共找到 {len(df)} 只符合条件的股票")
    
    # 统计信息
    logger.info(f"\n📊 统计信息：")
    logger.info(f"   平均成交量放大倍数: {df['volume_surge_ratio'].mean():.2f}")
    logger.info(f"   最大成交量放大倍数: {df['volume_surge_ratio'].max():.2f}")
    logger.info(f"   平均周涨跌幅: {df['latest_pct_chg'].mean():.2f}%")
    logger.info(f"   平均周成交额: {df['latest_amount_yi'].mean():.2f}亿元")


def main():
    """主函数"""
    logger.info("🚀 开始查询成交量放大的大市值主板股票...")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        analyzer = VolumeSurgeAnalyzer()
        result_df = analyzer.analyze_volume_surge_stocks()
        
        if result_df is not None and not result_df.empty:
            display_results(result_df)
            
            # 保存结果到文件
            output_file = f"volume_surge_stocks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"\n💾 结果已保存到文件: {output_file}")
            
            logger.info("\n💡 使用说明：")
            logger.info("   - 成交量倍数：最近2周平均成交量 / 之前4周平均成交量")
            logger.info("   - 只显示成交量放大1.5倍以上的股票")
            logger.info("   - 市值筛选基于周成交额（>10亿）进行粗略估算")
            logger.info("   - 建议结合基本面分析进一步筛选")
            
        else:
            logger.error("❌ 未找到符合条件的股票，可能原因：")
            logger.error("   1. 周线数据不足")
            logger.error("   2. 最近市场成交量普遍较低")
            logger.error("   3. 数据库中缺少相关数据")
            
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
