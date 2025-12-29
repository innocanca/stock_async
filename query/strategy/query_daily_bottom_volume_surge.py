#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询日线级别放巨量、处于250日线下方、底部区域的主板股票
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, List, Dict

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)

class DailyBottomVolumeSurgeAnalyzer:
    """日线底部放巨量分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()
        
    def get_analysis_results(self, vol_ratio_threshold: float = 3.0, price_pos_threshold: float = 0.2) -> List[Dict]:
        """
        获取分析结果
        
        Args:
            vol_ratio_threshold: 成交量放大倍数阈值，默认 3.0 (巨量)
            price_pos_threshold: 价格位置阈值，默认 0.2 (处于过去250天波动的低位20%)
            
        Returns:
            List[Dict]: 符合条件的股票列表
        """
        try:
            if not self.db.connect():
                logger.error("数据库连接失败")
                return []
                
            # 1. 获取所有主板股票基础信息
            basic_df = self.fetcher.get_stock_basic()
            if basic_df is None or basic_df.empty:
                logger.error("获取股票基础信息失败")
                return []
            
            # 筛选主板
            main_board_df = self.filter_main_board_stocks(basic_df)
            ts_codes = main_board_df['ts_code'].tolist()
            
            # 2. 获取最近交易日
            latest_date = self.get_latest_trade_date()
            if not latest_date:
                logger.error("未找到最近交易日")
                return []
            
            logger.info(f"开始分析 {latest_date} 的数据，筛选主板股票 {len(ts_codes)} 只...")
            
            results = []
            
            # 为了性能，我们可能需要分批处理或者使用更高效的SQL查询
            # 这里先采用循环处理，如果性能有问题再优化
            count = 0
            for ts_code in ts_codes:
                count += 1
                if count % 500 == 0:
                    logger.info(f"已处理 {count}/{len(ts_codes)} 只股票...")
                
                # 获取该股最近300天的日线数据（为了计算MA250和价格位置）
                stock_data = self.get_daily_data_for_analysis(ts_code, latest_date, days=300)
                if stock_data is None or len(stock_data) < 250:
                    continue
                
                latest_record = stock_data.iloc[-1]
                
                # 策略条件 1: 250日线下方
                ma250 = stock_data['close'].tail(250).mean()
                if latest_record['close'] >= ma250:
                    continue
                
                # 策略条件 2: 底部 (价格位置在过去250天的低位)
                recent_250 = stock_data.tail(250)
                high_250 = recent_250['high'].max()
                low_250 = recent_250['low'].min()
                
                if high_250 > low_250:
                    price_pos = (latest_record['close'] - low_250) / (high_250 - low_250)
                else:
                    price_pos = 0.5
                
                if price_pos > price_pos_threshold:
                    continue
                
                # 策略条件 3: 日线级别放巨量
                # 计算过去20个交易日的平均成交量 (不含当天)
                avg_vol_20 = stock_data.iloc[-21:-1]['vol'].mean()
                if avg_vol_20 <= 0:
                    continue
                    
                vol_ratio = latest_record['vol'] / avg_vol_20
                if vol_ratio < vol_ratio_threshold:
                    continue
                
                # 符合所有条件
                stock_info = main_board_df[main_board_df['ts_code'] == ts_code].iloc[0]
                results.append({
                    'ts_code': ts_code,
                    'name': stock_info['name'],
                    'industry': stock_info['industry'],
                    'close': float(latest_record['close']),
                    'pct_chg': float(latest_record['pct_chg']),
                    'vol_ratio': float(vol_ratio),
                    'price_pos': float(price_pos),
                    'ma250': float(ma250),
                    'dist_to_ma250': float((latest_record['close'] - ma250) / ma250 * 100),
                    'trade_date': latest_record['trade_date'].strftime('%Y-%m-%d') if hasattr(latest_record['trade_date'], 'strftime') else str(latest_record['trade_date'])
                })
            
            logger.info(f"分析完成，找到 {len(results)} 只符合条件的股票")
            return sorted(results, key=lambda x: x['vol_ratio'], reverse=True)
            
        except Exception as e:
            logger.error(f"分析失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
        finally:
            self.db.disconnect()

    def filter_main_board_stocks(self, df: pd.DataFrame) -> pd.DataFrame:
        """筛选主板股票"""
        import re
        main_board_patterns = [
            r'^60[0135]\d{3}\.SH$',  # 沪市主板
            r'^00[012]\d{3}\.SZ$'   # 深市主板
        ]
        
        def is_main_board(ts_code):
            for pattern in main_board_patterns:
                if re.match(pattern, ts_code):
                    return True
            return False
            
        return df[df['ts_code'].apply(is_main_board)].copy()

    def get_latest_trade_date(self):
        """获取数据库中最新的交易日期"""
        try:
            sql = "SELECT MAX(trade_date) FROM daily_data"
            with self.db.connection.cursor() as cursor:
                cursor.execute(sql)
                result = cursor.fetchone()
                return result[0] if result else None
        except Exception as e:
            logger.error(f"获取最新交易日失败: {e}")
            return None

    def get_daily_data_for_analysis(self, ts_code: str, latest_date, days: int = 300) -> Optional[pd.DataFrame]:
        """获取单只股票的日线历史数据"""
        try:
            sql = """
            SELECT trade_date, close, high, low, vol, pct_chg
            FROM daily_data
            WHERE ts_code = %s AND trade_date <= %s
            ORDER BY trade_date DESC
            LIMIT %s
            """
            df = pd.read_sql(sql, self.db.connection, params=[ts_code, latest_date, days])
            if df.empty:
                return None
            return df.sort_values('trade_date')
        except Exception as e:
            # logger.warning(f"获取 {ts_code} 日线数据失败: {e}")
            return None

if __name__ == "__main__":
    analyzer = DailyBottomVolumeSurgeAnalyzer()
    results = analyzer.get_analysis_results()
    print(f"找到 {len(results)} 只股票")
    for res in results[:10]:
        print(res)

