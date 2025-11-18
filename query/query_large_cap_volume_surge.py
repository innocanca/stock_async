#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询500亿以上市值的成交量放大主板股票（精确版）

功能：
1. 根据知名大市值股票列表进行筛选
2. 分析成交量放大情况
3. 结合基本面信息

使用方法：
python query_large_cap_volume_surge.py
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


class LargeCapVolumeSurgeAnalyzer:
    """大市值股票成交量放大分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
        
        # 手工整理的500亿以上市值主板股票列表（截至2024年）
        self.large_cap_stocks = {
            # 沪市主板大市值股票
            '600519.SH': '贵州茅台',    # 万亿级
            '600036.SH': '招商银行',    # 千亿级
            '600000.SH': '浦发银行',
            '600887.SH': '伊利股份',
            '600276.SH': '恒瑞医药',
            '600030.SH': '中信证券',
            '600050.SH': '中国联通',
            '600104.SH': '上汽集团',
            '600690.SH': '海尔智家',
            '600703.SH': '三安光电',
            '600837.SH': '海通证券',
            '600900.SH': '长江电力',
            '601012.SH': '隆基绿能',
            '601066.SH': '中信建投',
            '601166.SH': '兴业银行',
            '601169.SH': '北京银行',
            '601229.SH': '上海银行',
            '601288.SH': '农业银行',
            '601318.SH': '中国平安',    # 万亿级
            '601328.SH': '交通银行',
            '601336.SH': '新华保险',
            '601390.SH': '中国中铁',
            '601398.SH': '工商银行',    # 万亿级
            '601601.SH': '中国太保',
            '601628.SH': '中国人寿',
            '601668.SH': '中国建筑',
            '601688.SH': '华泰证券',
            '601766.SH': '中国中车',
            '601788.SH': '光大证券',
            '601818.SH': '光大银行',
            '601828.SH': '美凯龙',
            '601857.SH': '中国石油',
            '601888.SH': '中国国旅',
            '601898.SH': '中煤能源',
            '601919.SH': '中远海控',
            '601939.SH': '建设银行',    # 万亿级
            '601985.SH': '中国核电',
            '601988.SH': '中国银行',
            '601989.SH': '中国重工',
            '600028.SH': '中国石化',
            '600031.SH': '三一重工',
            '600048.SH': '保利地产',
            '600585.SH': '海螺水泥',
            '600660.SH': '福耀玻璃',
            '600809.SH': '山西汾酒',
            '600570.SH': '恒生电子',
            
            # 深市主板大市值股票
            '000001.SZ': '平安银行',
            '000002.SZ': '万科A',
            '000063.SZ': '中兴通讯',
            '000100.SZ': 'TCL科技',
            '000157.SZ': '中联重科',
            '000166.SZ': '申万宏源',
            '000333.SZ': '美的集团',    # 千亿级
            '000338.SZ': '潍柴动力',
            '000858.SZ': '五粮液',      # 千亿级
            '000895.SZ': '双汇发展',
            '000938.SZ': '紫光股份',
            '000961.SZ': '中南建设',
            '002001.SZ': '新和成',
            '002007.SZ': '华兰生物',
            '002024.SZ': '苏宁易购',
            '002027.SZ': '分众传媒',
            '002032.SZ': '苏泊尔',
            '002142.SZ': '宁波银行',
            '002202.SZ': '金风科技',
            '002230.SZ': '科大讯飞',
            '002236.SZ': '大华股份',
            '002241.SZ': '歌尔股份',
            '002304.SZ': '洋河股份',
            '002352.SZ': '顺丰控股',
            '002415.SZ': '海康威视',
            '002456.SZ': '欧菲光',
            '002475.SZ': '立讯精密',
            '002493.SZ': '荣盛石化',
            '002508.SZ': '老板电器',
            '002594.SZ': '比亚迪',      # 千亿级
            '002601.SZ': '龙佰集团',
            '002602.SZ': '世纪华通',
            '002714.SZ': '牧原股份',
            '002736.SZ': '国信证券',
            '002791.SZ': '坚朗五金',
            '000876.SZ': '新希望',
        }
    
    def get_large_cap_weekly_data(self, weeks_back: int = 8) -> Optional[pd.DataFrame]:
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
            
            logger.info(f"获取大市值股票 {start_date_str} 至 {end_date_str} 的周线数据...")
            
            with self.db:
                df = self.db.query_weekly_data(
                    start_date=start_date_str,
                    end_date=end_date_str
                )
                
                if df is None or df.empty:
                    logger.error("未找到周线数据")
                    return None
                
                # 只保留大市值股票
                large_cap_df = df[df['ts_code'].isin(self.large_cap_stocks.keys())].copy()
                
                if large_cap_df.empty:
                    logger.error("未找到大市值股票的周线数据")
                    return None
                
                logger.info(f"获取到 {len(large_cap_df)} 条大市值股票周线记录，涵盖 {large_cap_df['ts_code'].nunique()} 只股票")
                return large_cap_df
                
        except Exception as e:
            logger.error(f"获取周线数据失败: {e}")
            return None
    
    def calculate_volume_surge(self, df: pd.DataFrame, min_surge_ratio: float = 1.8) -> pd.DataFrame:
        """
        计算成交量放大情况（对大市值股票使用更严格的标准）
        
        Args:
            df: 周线数据
            min_surge_ratio: 最小放大倍数
            
        Returns:
            pd.DataFrame: 包含成交量分析的数据
        """
        try:
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
                    
                    # 对大市值股票使用更严格的成交量放大标准
                    if volume_ratio >= min_surge_ratio:
                        latest_record = stock_data.iloc[-1]
                        stock_name = self.large_cap_stocks.get(ts_code, '未知')
                        
                        results.append({
                            'ts_code': ts_code,
                            'stock_name': stock_name,
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
                logger.warning(f"未找到成交量放大{min_surge_ratio}倍以上的大市值股票")
                # 降低标准重新查询
                return self.calculate_volume_surge(df, min_surge_ratio=1.5)
            
            result_df = pd.DataFrame(results)
            result_df = result_df.sort_values('volume_surge_ratio', ascending=False)
            
            logger.info(f"找到 {len(result_df)} 只成交量放大的大市值股票")
            return result_df
            
        except Exception as e:
            logger.error(f"计算成交量放大失败: {e}")
            return pd.DataFrame()
    
    def get_additional_info(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        获取额外的股票信息
        
        Args:
            df: 包含股票数据的DataFrame
            
        Returns:
            pd.DataFrame: 添加了额外信息的DataFrame
        """
        try:
            # 添加行业信息等
            enhanced_df = df.copy()
            
            # 简化的行业分类
            industry_mapping = {
                '600519.SH': '白酒',
                '600036.SH': '银行',
                '601318.SH': '保险',
                '000858.SZ': '白酒',
                '000333.SZ': '家电',
                '002594.SZ': '新能源汽车',
                '002415.SZ': '安防',
                '002475.SZ': '消费电子',
                '600900.SH': '电力',
                '601012.SH': '光伏',
                '600276.SH': '医药',
                '000063.SZ': '通信设备',
                '002202.SZ': '风电',
                '002230.SZ': '人工智能',
            }
            
            enhanced_df['industry'] = enhanced_df['ts_code'].map(industry_mapping).fillna('其他')
            
            return enhanced_df
            
        except Exception as e:
            logger.error(f"获取额外信息失败: {e}")
            return df
    
    def analyze_large_cap_volume_surge(self) -> Optional[pd.DataFrame]:
        """
        主分析函数：找到成交量放大的500亿以上市值主板股票
        
        Returns:
            pd.DataFrame: 分析结果
        """
        try:
            logger.info("🔍 开始分析500亿以上市值股票的成交量放大情况...")
            logger.info(f"📊 分析范围：{len(self.large_cap_stocks)} 只知名大市值股票")
            
            # 1. 获取大市值股票的周线数据
            weekly_df = self.get_large_cap_weekly_data(weeks_back=8)
            if weekly_df is None or weekly_df.empty:
                return None
            
            # 2. 计算成交量放大
            volume_surge_df = self.calculate_volume_surge(weekly_df, min_surge_ratio=1.8)
            if volume_surge_df.empty:
                logger.error("未找到成交量明显放大的大市值股票")
                return None
            
            # 3. 获取额外信息
            result_df = self.get_additional_info(volume_surge_df)
            
            logger.info(f"✅ 找到 {len(result_df)} 只符合条件的大市值股票")
            return result_df
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            return None


def display_large_cap_results(df: pd.DataFrame):
    """
    显示大市值股票分析结果
    
    Args:
        df: 分析结果数据
    """
    if df is None or df.empty:
        logger.error("❌ 未找到符合条件的大市值股票")
        return
    
    logger.info("📋 符合条件的500亿+市值股票列表：")
    logger.info("=" * 110)
    logger.info(f"{'排名':<4} {'股票代码':<12} {'股票名称':<12} {'行业':<10} {'最新价':<8} {'涨跌幅%':<8} {'成交量倍数':<10} {'周成交额(亿)':<12}")
    logger.info("=" * 110)
    
    for i, (_, row) in enumerate(df.iterrows(), 1):
        amount_yi = row['latest_amount'] / 10000  # 转换为亿元
        logger.info(
            f"{i:<4} "
            f"{row['ts_code']:<12} "
            f"{row['stock_name']:<12} "
            f"{row.get('industry', '其他'):<10} "
            f"{row['latest_close']:<8.2f} "
            f"{row['latest_pct_chg']:<8.2f} "
            f"{row['volume_surge_ratio']:<10.2f} "
            f"{amount_yi:<12.2f}"
        )
    
    logger.info("=" * 110)
    logger.info(f"总共找到 {len(df)} 只符合条件的大市值股票")
    
    # 行业分布统计
    if 'industry' in df.columns:
        industry_counts = df['industry'].value_counts()
        logger.info(f"\n📊 行业分布：")
        for industry, count in industry_counts.items():
            logger.info(f"   {industry}: {count} 只")
    
    # 统计信息
    logger.info(f"\n📈 统计信息：")
    logger.info(f"   平均成交量放大倍数: {df['volume_surge_ratio'].mean():.2f}")
    logger.info(f"   最大成交量放大倍数: {df['volume_surge_ratio'].max():.2f}")
    logger.info(f"   平均周涨跌幅: {df['latest_pct_chg'].mean():.2f}%")


def main():
    """主函数"""
    logger.info("🚀 开始查询500亿以上市值的成交量放大股票...")
    logger.info("=" * 60)
    
    start_time = datetime.now()
    
    try:
        analyzer = LargeCapVolumeSurgeAnalyzer()
        result_df = analyzer.analyze_large_cap_volume_surge()
        
        if result_df is not None and not result_df.empty:
            display_large_cap_results(result_df)
            
            # 保存结果到文件
            output_file = f"large_cap_volume_surge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            logger.info(f"\n💾 结果已保存到文件: {output_file}")
            
            logger.info("\n💡 投资建议：")
            logger.info("   ✅ 这些都是知名的大市值蓝筹股")
            logger.info("   ✅ 成交量放大可能预示着重要变化")
            logger.info("   ⚠️  建议结合基本面、技术面和消息面综合分析")
            logger.info("   ⚠️  关注放大背后的原因（业绩、政策、事件等）")
            
        else:
            logger.error("❌ 未找到符合条件的大市值股票，可能原因：")
            logger.error("   1. 最近大市值股票成交量相对稳定")
            logger.error("   2. 需要降低成交量放大的标准")
            logger.error("   3. 可以关注成交量放大1.5倍以上的股票")
            
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
