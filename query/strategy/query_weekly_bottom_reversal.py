#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询周线连续下跌后，最近一周放量反转的主板股票

筛选条件：
1. 市场板块：主板 (60xxxx.SH / 00xxxx.SZ)
2. 市值：总市值 > 100亿 (total_mv >= 1,000,000 万元)
3. 前期走势：此前连续至少 3 周周线收阴（或收盘价持续下跌）
4. 反转信号：最近一周周线收阳，且成交量显著放大（最近一周成交量 > 过去 3 周平均成交量 * 1.5）
"""

import sys
import os
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)

class WeeklyBottomReversalAnalyzer:
    """周线底部放量反转分析器"""

    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()

    def get_market_valuations(self, min_mv: float = 1000000) -> pd.DataFrame:
        """获取主板大市值股票"""
        logger.info(f"📊 获取主板市值 > {min_mv/10000:.0f} 亿的股票列表...")
        try:
            df = None
            for i in range(5):
                trade_date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
                try:
                    df = self.fetcher.pro.daily_basic(
                        trade_date=trade_date,
                        fields="ts_code,trade_date,close,pe_ttm,total_mv"
                    )
                    if df is not None and not df.empty:
                        break
                except:
                    continue

            if df is None or df.empty:
                return pd.DataFrame()

            # 主板过滤
            df = df[df["ts_code"].str.match(r"^(60|00)\d{4}\.(SH|SZ)$")]
            # 市值过滤
            df = df[df["total_mv"] >= min_mv]
            
            return df
        except Exception as e:
            logger.error(f"获取估值数据失败: {e}")
            return pd.DataFrame()

    def get_analysis_results(
        self, 
        min_mv: float = 1000000, 
        min_drop_weeks: int = 3, 
        vol_ratio: float = 1.5
    ) -> List[Dict]:
        """获取分析结果列表"""
        df_valuation = self.get_market_valuations(min_mv=min_mv)
        if df_valuation.empty:
            return []

        stock_codes = df_valuation["ts_code"].tolist()
        
        try:
            with self.db:
                cursor = self.db.connection.cursor()
                cursor.execute("SELECT MAX(trade_date) FROM weekly_data")
                latest_week = cursor.fetchone()[0]
                if not latest_week:
                    return []

                # 取最近 12 周数据
                placeholders = ",".join(["%s"] * len(stock_codes))
                sql = f"""
                SELECT ts_code, trade_date, open, close, vol, pct_chg
                FROM weekly_data
                WHERE ts_code IN ({placeholders})
                  AND trade_date >= DATE_SUB(%s, INTERVAL 90 DAY)
                ORDER BY ts_code, trade_date ASC
                """
                df_weekly = pd.read_sql(sql, self.db.connection, params=stock_codes + [latest_week])

            if df_weekly.empty:
                return []

            results = []
            # 获取名称
            from query.strategy.query_low_pe_volume_surge import LowPEVolumeSurgeAnalyzer
            stock_names = LowPEVolumeSurgeAnalyzer().get_stock_names(stock_codes)

            for ts_code, g in df_weekly.groupby("ts_code"):
                if len(g) < min_drop_weeks + 1:
                    continue
                
                rows = g.tail(min_drop_weeks + 1)
                latest_row = rows.iloc[-1]
                prev_rows = rows.iloc[:-1]
                
                # 1. 前期连续下跌判断 (收盘价连续低于前一周收盘价)
                # 需要至少 min_drop_weeks + 2 条历史记录 (1条当前周 + min_drop_weeks条下跌周 + 1条起始对比周)
                if len(g) < min_drop_weeks + 2:
                    continue
                
                # 我们判断倒数第 2 周到倒数第 min_drop_weeks + 1 周是否都在下跌
                # 倒数第 1 周是当前分析周 (反转周)
                is_dropping = True
                for i in range(1, min_drop_weeks + 1):
                    # 检查点：倒数第 (i+1) 周 vs 倒数第 (i+2) 周
                    curr_prev = g.iloc[-(i+1)]
                    prev_prev = g.iloc[-(i+2)]
                    if curr_prev['close'] >= prev_prev['close']:
                        is_dropping = False
                        break
                
                if not is_dropping:
                    continue

                # 2. 最近一周反转判断 (阳线且收盘价上涨)
                is_reversal = latest_row['close'] > latest_row['open'] and latest_row['pct_chg'] > 0
                if not is_reversal:
                    continue

                # 3. 放量判断 (成交量 > 过去 N 周平均成交量的 vol_ratio 倍)
                avg_vol = prev_rows['vol'].mean()
                if avg_vol <= 0:
                    continue
                
                actual_ratio = latest_row['vol'] / avg_vol
                if actual_ratio < vol_ratio:
                    continue

                # 命中！
                valuation = df_valuation[df_valuation['ts_code'] == ts_code].iloc[0]
                results.append({
                    "ts_code": ts_code,
                    "代码": ts_code,
                    "名称": stock_names.get(ts_code, ts_code),
                    "市值(亿)": float(valuation["total_mv"] / 10000),
                    "现价": float(latest_row["close"]),
                    "本周涨幅%": float(latest_row["pct_chg"]),
                    "放量倍数": float(actual_ratio),
                    "连续下跌周数": min_drop_weeks,
                    "最近周线日期": str(latest_row["trade_date"]),
                })

            # 排序：按放量倍数从高到低
            results.sort(key=lambda x: x["放量倍数"], reverse=True)
            return results

        except Exception as e:
            logger.error(f"分析周线反转失败: {e}")
            return []

    def run_analysis(self):
        """执行分析并打印结果"""
        results = self.get_analysis_results()
        if not results:
            logger.info("未找到符合周线放量反转条件的股票。")
            return

        print(f"\n🚀 周线放量反转筛选结果 (市值>100亿, 主板, 连续下跌>{results[0]['连续下跌周数']}周): 共 {len(results)} 只")
        print("=" * 100)
        print(f"{'代码':<10} {'名称':<10} {'市值(亿)':<10} {'现价':<10} {'涨幅%':<10} {'放量倍数':<10}")
        print("-" * 100)
        for r in results:
            print(f"{r['代码']:<10} {r['名称']:<10} {r['市值(亿)']:<10.1f} {r['现价']:<10.2f} {r['本周涨幅%']:<10.2f} {r['放量倍数']:<10.2f}")
        print("=" * 100)

if __name__ == "__main__":
    analyzer = WeeklyBottomReversalAnalyzer()
    analyzer.run_analysis()

