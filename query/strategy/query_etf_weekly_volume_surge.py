#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询周线明显放量的ETF列表

定义：
1. 使用 `etf_daily` 表的日线数据，按周聚合成交量（自然周，YEARWEEK(trade_date, 3)）
2. 对每只ETF计算：
   - 最近一周的周成交量 last_week_vol
   - 过去 N 周（默认3周）中的最大周成交量 max_prev_vol
   - 放量倍数 volume_ratio = last_week_vol / max_prev_vol
3. “明显放量”默认定义为：volume_ratio >= 1.5

使用方法：
    python query_etf_weekly_volume_surge.py

依赖：
    - 已初始化 etf_daily（日线行情）和 etf_basic（ETF基础信息）
"""

import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import StockDatabase
from log_config import get_logger

logger = get_logger(__name__)


class ETFWeeklyVolumeSurgeAnalyzer:
    """ETF 周线放量筛选器"""

    def __init__(self):
        self.db = StockDatabase()

    def _get_latest_trade_date(self) -> datetime:
        """获取 etf_daily 中最近的交易日期"""
        cursor = self.db.connection.cursor()
        cursor.execute("SELECT MAX(trade_date) FROM etf_daily")
        result = cursor.fetchone()
        if not result or not result[0]:
            return None
        return result[0]

    def get_weekly_aggregated_volumes(
        self,
        lookback_days: int = 80,
    ) -> pd.DataFrame:
        """
        从 etf_daily 聚合得到按周统计的成交量

        Args:
            lookback_days: 回溯的自然日范围，用于截取一定窗口内的周线数据
        """
        logger.info("📊 从 etf_daily 聚合周成交量 ...")

        latest_trade_date = self._get_latest_trade_date()
        if latest_trade_date is None:
            logger.error("❌ etf_daily 表中没有任何数据")
            return pd.DataFrame()

        logger.info(f"   最近交易日: {latest_trade_date}")

        cursor = self.db.connection.cursor()
        query_sql = """
        SELECT
            ts_code,
            YEARWEEK(trade_date, 3) AS year_week,
            MAX(trade_date) AS week_end_date,
            SUM(vol) AS week_vol,
            SUM(amount) AS week_amount
        FROM etf_daily
        WHERE trade_date >= DATE_SUB(%s, INTERVAL %s DAY)
        GROUP BY ts_code, YEARWEEK(trade_date, 3)
        ORDER BY ts_code, week_end_date;
        """

        params = [latest_trade_date, lookback_days]
        df = pd.read_sql(query_sql, self.db.connection, params=params)

        if df.empty:
            logger.warning("⚠️ 未聚合出任何周线ETF成交量数据")
            return pd.DataFrame()

        # 确保日期列为 datetime
        df["week_end_date"] = pd.to_datetime(df["week_end_date"])
        logger.info(f"   聚合得到 {len(df)} 条 ETF 周线成交量记录")
        return df

    def get_etf_names(self, ts_codes: List[str]) -> Dict[str, str]:
        """从 etf_basic 获取 ETF 名称（extname）"""
        names: Dict[str, str] = {}
        if not ts_codes:
            return names

        try:
            with StockDatabase() as db:
                cursor = db.connection.cursor()
                placeholders = ",".join(["%s"] * len(ts_codes))
                sql = f"""
                SELECT ts_code, COALESCE(extname, ts_code) AS name
                FROM etf_basic
                WHERE ts_code IN ({placeholders})
                """
                cursor.execute(sql, ts_codes)
                for ts_code, name in cursor.fetchall():
                    names[ts_code] = name
        except Exception as e:
            logger.error(f"获取ETF名称失败: {e}")

        return names

    def find_weekly_volume_surge_etfs(
        self,
        min_ratio: float = 1.5,
        lookback_weeks: int = 3,
        min_last_week_amount_yi: float = 1.0,
    ) -> pd.DataFrame:
        """
        查找最近一周周线明显放量的ETF

        Args:
            min_ratio: 最小放量倍数（例如 1.5）
            lookback_weeks: 回看周数，用于计算历史最大周成交量
        """
        logger.info(
            f"📈 筛选ETF周线放量：最近1周周成交量 > 过去{lookback_weeks}周最大周成交量 × {min_ratio} "
            f"且最近一周成交额 ≥ {min_last_week_amount_yi} 亿元..."
        )

        weekly_df = self.get_weekly_aggregated_volumes(lookback_days=80)
        if weekly_df.empty:
            return pd.DataFrame()

        results = []
        # 金额单位换算：etf_daily.amount 为“千元”，1亿元 = 100000 千元
        min_last_week_amount_qianyuan = min_last_week_amount_yi * 100000
        for ts_code, g in weekly_df.groupby("ts_code"):
            g = g.sort_values("week_end_date")
            if len(g) < lookback_weeks + 1:
                continue

            last_rows = g.tail(lookback_weeks + 1)
            if len(last_rows) < lookback_weeks + 1:
                continue

            last_week_row = last_rows.iloc[-1]
            prev_weeks = last_rows.iloc[:-1]

            prev_max_vol = prev_weeks["week_vol"].max()
            last_vol = last_week_row["week_vol"]
            last_amount = last_week_row.get("week_amount", 0.0)
            if prev_max_vol is None or prev_max_vol <= 0:
                continue

            ratio = last_vol / prev_max_vol
            if ratio < min_ratio:
                continue

            # 最近一周成交额不足阈值（默认 < 1 亿元）的跳过
            if pd.isna(last_amount) or float(last_amount) < min_last_week_amount_qianyuan:
                continue

            results.append(
                {
                    "ts_code": ts_code,
                    "latest_week_end": last_week_row["week_end_date"],
                    "last_week_vol": float(last_vol),
                    "max_prev_vol": float(prev_max_vol),
                        "last_week_amount": float(last_amount),
                        "volume_ratio": float(ratio),
                }
            )

        surge_df = pd.DataFrame(results)
        logger.info(
            f"   满足周线放量>= {min_ratio} 倍且最近一周成交额≥ {min_last_week_amount_yi} 亿元的ETF数量: {len(surge_df)}"
        )
        return surge_df

    def get_analysis_results(
        self,
        min_ratio: float = 1.5,
        lookback_weeks: int = 3,
        min_last_week_amount_yi: float = 1.0,
    ) -> List[Dict]:
        """
        获取分析结果列表，供 API 调用。
        """
        try:
            with self.db:
                surge_df = self.find_weekly_volume_surge_etfs(
                    min_ratio=min_ratio,
                    lookback_weeks=lookback_weeks,
                    min_last_week_amount_yi=min_last_week_amount_yi,
                )

            if surge_df.empty:
                return []

            # 补充ETF名称
            etf_names = self.get_etf_names(surge_df["ts_code"].tolist())

            final_rows = []
            for _, row in surge_df.iterrows():
                ts_code = row["ts_code"]
                final_rows.append(
                    {
                        "ts_code": ts_code,
                        "代码": ts_code,
                        "名称": etf_names.get(ts_code, ts_code),
                        "最近周线截止日": str(row["latest_week_end"])[:10],
                        "最近一周成交量(手)": float(row["last_week_vol"]),
                        "最近一周成交额(亿元)": float(row["last_week_amount"] / 100000.0),
                        "过去3周最大周成交量(手)": float(row["max_prev_vol"]),
                        "周放量倍数": float(row["volume_ratio"]),
                    }
                )

            # 排序
            final_rows.sort(
                key=lambda x: (x["周放量倍数"], x["最近一周成交额(亿元)"]),
                reverse=True
            )
            
            return final_rows
        except Exception as e:
            logger.error(f"获取分析结果失败: {e}")
            return []

    def run(self):
        """执行ETF周线放量查询并打印结果"""
        results = self.get_analysis_results()
        
        if not results:
            logger.warning("没有找到周线明显放量的ETF")
            return

        logger.info(
            f"\n🎉 周线明显放量的ETF列表 (最近1周 > 过去3周最大周成交量 × 1.5 且 最近一周成交额≥1亿元): 共 {len(results)} 只"
        )
        logger.info("=" * 120)
        logger.info(
            f"{'代码':<12} {'名称':<20} {'最近周线截止日':<12} "
            f"{'最近一周成交量(手)':<18} {'最近一周成交额(亿元)':<18} "
            f"{'过去3周最大周成交量(手)':<22} {'周放量倍数':<10}"
        )
        logger.info("-" * 120)

        for r in results:
            logger.info(
                f"{r['代码']:<12} {r['名称']:<20} "
                f"{r['最近周线截止日']:<12} "
                f"{r['最近一周成交量(手)']:<18.0f} "
                f"{r['最近一周成交额(亿元)']:<18.2f} "
                f"{r['过去3周最大周成交量(手)']:<22.0f} "
                f"{r['周放量倍数']:<10.2f}"
            )

        logger.info("=" * 120)

        


if __name__ == "__main__":
    analyzer = ETFWeeklyVolumeSurgeAnalyzer()
    analyzer.run()


