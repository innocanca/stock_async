#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询业绩好、PE低估的大市值主板股票

筛选条件：
1. 市场板块：主板
2. 总市值：> 500亿
3. 估值指标：PE(TTM) < 20 (可调整)
4. 业绩指标：净利润同比增长率 > 10% (最近一期报告)

使用方法：
python query_undervalued_growth.py
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
from log_config import get_logger

logger = get_logger(__name__)


class UndervaluedGrowthAnalyzer:
    """低估值成长股分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
    
    def get_market_valuations(self, min_mv: float = 5000000, max_pe: float = 25) -> pd.DataFrame:
        """
        获取市场估值数据 (市值、PE)
        
        Args:
            min_mv: 最小市值（万元），500亿 = 5000000万元
            max_pe: 最大市盈率（TTM）
            
        Returns:
            pd.DataFrame: 符合估值条件的股票列表
        """
        logger.info("📊 获取全市场估值数据(daily_basic)...")
        
        try:
            # 获取最近一个交易日
            today = datetime.now().strftime('%Y%m%d')
            # 尝试获取，如果今天是周末或非交易日，可能需要往前推几天
            # 简单的做法是直接请求最新日期，tushare会自动处理或者我们需要重试
            
            df = None
            for i in range(5):
                trade_date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
                logger.info(f"   尝试获取 {trade_date} 的估值数据...")
                try:
                    # fields: ts_code, trade_date, close, turnover_rate, pe_ttm, pb, total_mv
                    df = self.fetcher.pro.daily_basic(
                        trade_date=trade_date, 
                        fields='ts_code,trade_date,close,pe_ttm,pb,total_mv'
                    )
                    if df is not None and not df.empty:
                        logger.info(f"   ✅ 成功获取 {len(df)} 条估值数据")
                        break
                except Exception as e:
                    logger.warning(f"   ⚠️ 获取 {trade_date} 数据失败: {e}")
            
            if df is None or df.empty:
                logger.error("❌ 无法获取估值数据，可能是权限不足或非交易日")
                return pd.DataFrame()
            
            # 筛选主板股票
            # 60xxxx.SH, 00xxxx.SZ
            df = df[df['ts_code'].str.match(r'^(60|00)\d{4}\.(SH|SZ)$')]
            logger.info(f"   主板股票数量: {len(df)}")
            
            # 筛选市值
            # total_mv 单位是万元
            df = df[df['total_mv'] >= min_mv]
            logger.info(f"   市值>{min_mv/10000:.0f}亿的股票数量: {len(df)}")
            
            # 筛选PE
            # 过滤亏损股 (pe_ttm > 0) 和 高估值股
            df = df[(df['pe_ttm'] > 0) & (df['pe_ttm'] <= max_pe)]
            logger.info(f"   PE(TTM)<{max_pe}的股票数量: {len(df)}")
            
            return df
            
        except Exception as e:
            logger.error(f"获取估值数据失败: {e}")
            return pd.DataFrame()

    def get_financial_growth(self, stock_codes: List[str]) -> Dict[str, Dict]:
        """
        从本地数据库查询业绩增长情况
        
        Args:
            stock_codes: 目标股票代码列表
            
        Returns:
            Dict: {ts_code: {growth_rate, report_period, ...}}
        """
        logger.info(f"📉 查询 {len(stock_codes)} 只股票的业绩增长情况...")
        
        if not stock_codes:
            return {}
            
        growth_data = {}
        
        try:
            with self.db:
                # 1. 确定最近的报告期
                # 查询数据库中最新的报告期（取多数股票都有的最新日期）
                cursor = self.db.connection.cursor()
                
                # 将股票列表转换为SQL格式
                stocks_str = "'" + "','".join(stock_codes) + "'"
                
                # 获取最新的两个主要报告期（例如 20240930, 20230930）
                # 为了简化，我们查询每只股票最新的两条年报或季报记录
                # 注意：这里假设数据库已经初始化了利润表数据
                
                # 批量查询效率更高
                # 查询每只股票最近一期的归母净利润
                sql = f"""
                SELECT ts_code, end_date, n_income_attr_p
                FROM income_data
                WHERE ts_code IN ({stocks_str})
                ORDER BY end_date DESC
                """
                
                # 由于全部查询可能数据量较大，我们分批或者按股票逐个查，或者直接全量查再内存处理
                # 鉴于只有几百只大市值股票，全量查是可以接受的
                cursor.execute(sql)
                results = cursor.fetchall()
                
                if not results:
                    logger.warning("数据库中没有利润表数据")
                    return {}
                
                # 在内存中处理数据
                stock_financials = {}
                for row in results:
                    ts_code, end_date, net_profit = row
                    if ts_code not in stock_financials:
                        stock_financials[ts_code] = []
                    stock_financials[ts_code].append({
                        'date': end_date,
                        'profit': float(net_profit) if net_profit is not None else 0
                    })
                
                # 计算增长率
                for ts_code, records in stock_financials.items():
                    if len(records) < 2:
                        continue
                        
                    # 排序，确保按日期降序
                    records.sort(key=lambda x: x['date'], reverse=True)
                    
                    # 取最近一期
                    current = records[0]
                    current_date = current['date']
                    
                    # 寻找去年同期（日期减一年）
                    last_year_date = current_date.replace(year=current_date.year - 1)
                    
                    # 在记录中查找去年同期
                    last_year_record = next((item for item in records if item['date'] == last_year_date), None)
                    
                    if last_year_record:
                        last_profit = last_year_record['profit']
                        curr_profit = current['profit']
                        
                        # 计算增长率
                        # 避免除以0或负数导致的计算异常逻辑（这里简单处理，分母取绝对值）
                        if last_profit != 0:
                            growth_rate = (curr_profit - last_profit) / abs(last_profit) * 100
                        else:
                            growth_rate = 0
                            
                        growth_data[ts_code] = {
                            'growth_rate': growth_rate,
                            'current_period': current_date.strftime('%Y-%m-%d'),
                            'current_profit': curr_profit,
                            'last_profit': last_profit
                        }
        
        except Exception as e:
            logger.error(f"查询业绩数据失败: {e}")
            
        return growth_data

    def get_stock_names(self, stock_codes: List[str]) -> Dict[str, str]:
        """获取股票名称"""
        names = {}
        try:
            with self.db:
                cursor = self.db.connection.cursor()
                stocks_str = "'" + "','".join(stock_codes) + "'"
                cursor.execute(f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({stocks_str})")
                results = cursor.fetchall()
                for row in results:
                    names[row[0]] = row[1]
        except Exception as e:
            logger.error(f"获取股票名称失败: {e}")
        return names

    def run_analysis(self):
        """执行综合分析"""
        logger.info("🚀 开始筛选业绩好、PE低估的大市值主板股票...")
        
        # 1. 获取估值符合条件的股票
        # 市值 > 500亿, PE < 25 (放宽一点以便筛选)
        df_valuation = self.get_market_valuations(min_mv=5000000, max_pe=25)
        
        if df_valuation.empty:
            logger.warning("没有找到符合估值条件的股票")
            return
            
        target_codes = df_valuation['ts_code'].tolist()
        
        # 2. 获取股票名称
        stock_names = self.get_stock_names(target_codes)
        
        # 3. 获取业绩增长数据
        growth_data = self.get_financial_growth(target_codes)
        
        # 4. 综合筛选
        final_results = []
        
        for _, row in df_valuation.iterrows():
            ts_code = row['ts_code']
            
            # 如果没有业绩数据，跳过
            if ts_code not in growth_data:
                continue
                
            growth_info = growth_data[ts_code]
            growth_rate = growth_info['growth_rate']
            
            # 筛选条件：业绩增长 > 10%
            if growth_rate > 10:
                final_results.append({
                    '代码': ts_code,
                    '名称': stock_names.get(ts_code, ts_code),
                    '市值(亿)': row['total_mv'] / 10000,
                    'PE(TTM)': row['pe_ttm'],
                    'PB': row['pb'],
                    '现价': row['close'],
                    '业绩增速(%)': growth_rate,
                    '净利润(亿)': growth_info['current_profit'] / 100000000,
                    '报告期': growth_info['current_period']
                })
        
        # 5. 输出结果
        if not final_results:
            logger.warning("没有找到同时满足估值和业绩要求的股票")
            return
            
        # 按业绩增速排序
        final_df = pd.DataFrame(final_results)
        final_df = final_df.sort_values('业绩增速(%)', ascending=False)
        
        logger.info(f"\n🎉 筛选结果 (市值>500亿, PE<25, 业绩增长>10%): 共有 {len(final_df)} 只")
        logger.info("=" * 120)
        logger.info(f"{'代码':<10} {'名称':<10} {'市值(亿)':<10} {'PE(TTM)':<10} {'PB':<8} {'业绩增速%':<12} {'净利润(亿)':<12} {'报告期':<12}")
        logger.info("-" * 120)
        
        for _, row in final_df.iterrows():
            logger.info(f"{row['代码']:<10} {row['名称']:<10} {row['市值(亿)']:<10.1f} {row['PE(TTM)']:<10.2f} {row['PB']:<8.2f} {row['业绩增速(%)']:<12.2f} {row['净利润(亿)']:<12.2f} {row['报告期']:<12}")
        logger.info("=" * 120)
        
        # 保存结果
        output_file = f"undervalued_growth_stocks_{datetime.now().strftime('%Y%m%d')}.csv"
        final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        logger.info(f"\n💾 结果已保存至: {output_file}")


if __name__ == "__main__":
    analyzer = UndervaluedGrowthAnalyzer()
    analyzer.run_analysis()
