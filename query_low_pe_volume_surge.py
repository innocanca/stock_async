#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询低估值且周线放量的主板大市值股票

筛选条件：
1. 市场板块：主板（60xxxx.SH / 00xxxx.SZ）
2. 总市值：> 500亿（total_mv >= 5,000,000 万元）
3. 估值指标：PE(TTM) <= 30
4. 成交量：最近一周周线成交量 > 过去3周所有周最大成交量 × 1.3

使用方法：
    python query_low_pe_volume_surge.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict

import pandas as pd

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


class LowPEVolumeSurgeAnalyzer:
    """低PE + 周线放量 筛选器"""

    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()

    def get_market_valuations(self, min_mv: float = 5000000, max_pe: float = 30) -> pd.DataFrame:
        """
        获取市场估值数据 (市值、PE)，并筛选主板 + 大市值 + 低PE

        Args:
            min_mv: 最小总市值（万元），500亿 = 5000000 万元
            max_pe: 最大市盈率（TTM）
        """
        logger.info("📊 获取全市场估值数据(daily_basic)...")

        try:
            df = None
            # 往前最多回溯 5 天，找到最近一个有 daily_basic 数据的交易日
            for i in range(5):
                trade_date = (datetime.now() - timedelta(days=i)).strftime('%Y%m%d')
                logger.info(f"   尝试获取 {trade_date} 的估值数据...")
                try:
                    df = self.fetcher.pro.daily_basic(
                        trade_date=trade_date,
                        fields="ts_code,trade_date,close,pe_ttm,pb,total_mv"
                    )
                    if df is not None and not df.empty:
                        logger.info(f"   ✅ 成功获取 {len(df)} 条估值数据")
                        break
                except Exception as e:
                    logger.warning(f"   ⚠️ 获取 {trade_date} 数据失败: {e}")

            if df is None or df.empty:
                logger.error("❌ 无法获取估值数据，可能是权限不足或连续非交易日")
                return pd.DataFrame()

            # 主板过滤：60xxxx.SH / 00xxxx.SZ
            df = df[df["ts_code"].str.match(r"^(60|00)\d{4}\.(SH|SZ)$")]
            logger.info(f"   主板股票数量: {len(df)}")

            # 市值过滤（单位：万元）
            df = df[df["total_mv"] >= min_mv]
            logger.info(f"   市值>{min_mv/10000:.0f}亿的股票数量: {len(df)}")

            # PE 过滤：0 < PE <= max_pe
            df = df[(df["pe_ttm"] > 0) & (df["pe_ttm"] <= max_pe)]
            logger.info(f"   PE(TTM)<={max_pe} 的股票数量: {len(df)}")

            return df

        except Exception as e:
            logger.error(f"获取估值数据失败: {e}")
            return pd.DataFrame()

    def get_weekly_volume_surge(
        self,
        stock_codes: List[str],
        min_ratio: float = 1.3,
        lookback_weeks: int = 3,
    ) -> pd.DataFrame:
        """
        计算周线放量情况 + 判断是否“刚启动”：
        - 放量：最近一周成交量 / 过去 N 周「最大成交量」
        - 刚启动（启动车逻辑）粗略定义：
            * 前 3 周累计涨跌幅 < 10%（之前以震荡/整理为主）
            * 过去一年价格位置仍在区间下半部（未大幅拉升，position_1y <= 0.5）

        Args:
            stock_codes: 待检测股票列表
            min_ratio: 最小放量倍数，例如 1.3 表示最近一周 > 过去N周最大成交量的1.3倍
            lookback_weeks: 回看周数，用于计算历史最大成交量
        """
        logger.info(
            f"📈 计算周线放量：最近一周 vs 过去{lookback_weeks}周最大成交量，阈值 {min_ratio} 倍..."
        )

        if not stock_codes:
            return pd.DataFrame()

        try:
            # 获取最近一周的 trade_date
            cursor = self.db.connection.cursor()
            cursor.execute("SELECT MAX(trade_date) FROM weekly_data")
            result = cursor.fetchone()
            latest_week = result[0]

            if not latest_week:
                logger.error("❌ weekly_data 表中没有任何周线数据")
                return pd.DataFrame()

            logger.info(f"   最近周线日期: {latest_week}")

            # 从最近周线往前抓一段窗口（约1年），用于放量和“刚启动”判断
            placeholders = ",".join(["%s"] * len(stock_codes))
            query_sql = f"""
            SELECT ts_code, trade_date, vol, close, pct_chg, high, low
            FROM weekly_data
            WHERE trade_date <= %s
              AND trade_date >= DATE_SUB(%s, INTERVAL 365 DAY)
              AND ts_code IN ({placeholders})
            ORDER BY ts_code, trade_date
            """

            params = [latest_week, latest_week] + stock_codes
            df = pd.read_sql(query_sql, self.db.connection, params=params)

            if df.empty:
                logger.warning("⚠️ 未查询到任何周线数据")
                return pd.DataFrame()

            results = []
            for ts_code, g in df.groupby("ts_code"):
                g = g.sort_values("trade_date")
                if len(g) < lookback_weeks + 1:
                    continue

                last_rows = g.tail(lookback_weeks + 1)
                if len(last_rows) < lookback_weeks + 1:
                    continue

                last_week_row = last_rows.iloc[-1]
                prev_weeks = last_rows.iloc[:-1]

                # 使用过去 N 周中的「最大成交量」作为对比基准
                prev_max_vol = prev_weeks["vol"].max()
                last_vol = last_week_row["vol"]
                if prev_max_vol is None or prev_max_vol <= 0:
                    continue

                ratio = last_vol / prev_max_vol

                # ===== “刚启动”判断逻辑 =====
                last_pct = float(last_week_row.get("pct_chg") or 0)
                prev3_sum_pct = float(prev_weeks["pct_chg"].sum() if "pct_chg" in prev_weeks.columns else 0)

                # 过去一年价格区间位置（基于当前查询窗口内的周线数据）
                window_1y = g  # 已经按SQL限制在一年内
                high_1y = window_1y["high"].max()
                low_1y = window_1y["low"].min()
                pos_1y = None
                if (
                    high_1y is not None
                    and low_1y is not None
                    and pd.notna(high_1y)
                    and pd.notna(low_1y)
                    and high_1y > low_1y
                ):
                    pos_1y = (float(last_week_row["close"]) - float(low_1y)) / (float(high_1y) - float(low_1y))

                is_startup = (
                    ratio >= min_ratio
                    and prev3_sum_pct < 10.0
                    and pos_1y is not None
                    and pos_1y <= 0.5
                )

                if ratio >= min_ratio:
                    results.append(
                        {
                            "ts_code": ts_code,
                            "latest_week": last_week_row["trade_date"],
                            "last_week_vol": float(last_vol),
                            "max_prev_vol": float(prev_max_vol),
                            "volume_ratio": float(ratio),
                            "last_week_pct_chg": last_pct,
                            "prev3_sum_pct_chg": prev3_sum_pct,
                            "position_1y": float(pos_1y) if pos_1y is not None else None,
                            "is_startup": bool(is_startup),
                        }
                    )

            surge_df = pd.DataFrame(results)
            logger.info(f"   周线放量>= {min_ratio} 倍的股票数量: {len(surge_df)}")
            return surge_df

        except Exception as e:
            logger.error(f"计算周线放量失败: {e}")
            return pd.DataFrame()

    def get_stock_names(self, stock_codes: List[str]) -> Dict[str, str]:
        """从本地 stock_basic 表获取股票名称"""
        names: Dict[str, str] = {}
        if not stock_codes:
            return names
        
        try:
            # 单独开启一个数据库连接，避免依赖外部上下文的连接状态
            from database import StockDatabase as _DB  # 避免类型检查干扰
            with _DB() as db:
                cursor = db.connection.cursor()
                placeholders = ",".join(["%s"] * len(stock_codes))
                sql = f"SELECT ts_code, name FROM stock_basic WHERE ts_code IN ({placeholders})"
                cursor.execute(sql, stock_codes)
                for ts_code, name in cursor.fetchall():
                    names[ts_code] = name
        except Exception as e:
            logger.error(f"获取股票名称失败: {e}")

        return names

    def run_analysis(self):
        """执行综合筛选：PE + 市值 + 周线放量"""
        logger.info(
            "🚀 开始筛选：主板、市值>500亿、PE<=30 且最近一周周线放量>=2倍 的股票..."
        )

        # 1. 先从估值维度筛选出主板+大市值+低PE
        df_valuation = self.get_market_valuations(min_mv=5000000, max_pe=30)
        if df_valuation.empty:
            logger.warning("没有找到符合估值条件的股票")
            return

        target_codes = df_valuation["ts_code"].tolist()

        with self.db:
            # 2. 在估值合格的股票里，再筛选周线放量
            surge_df = self.get_weekly_volume_surge(
                stock_codes=target_codes, min_ratio=1.3, lookback_weeks=3
            )
            if surge_df.empty:
                logger.warning("在符合估值条件的股票中，没有找到满足「最近一周 > 过去3周最大成交量×1.3」的标的")
                return

        # 3. 合并估值 + 周线放量信息
        merged = pd.merge(df_valuation, surge_df, on="ts_code", how="inner")
        if merged.empty:
            logger.warning("估值数据与周线放量数据合并后为空")
            return

        # 3.1 只保留“一年内区间位置在下半部”的标的
        if "position_1y" in merged.columns:
            before_cnt = len(merged)
            merged = merged[merged["position_1y"].notna() & (merged["position_1y"] <= 0.5)].copy()
            logger.info(f"   按一年区间下半部过滤: {before_cnt} -> {len(merged)} 只")
            if merged.empty:
                logger.warning("当前没有满足“一年内区间位置在下半部”的标的")
                return

        # 4. 获取股票名称
        stock_names = self.get_stock_names(merged["ts_code"].tolist())

        # 5. 组织最终结果
        final_rows = []
        for _, row in merged.iterrows():
            ts_code = row["ts_code"]
            final_rows.append(
                {
                    "代码": ts_code,
                    "名称": stock_names.get(ts_code, ts_code),
                    "市值(亿)": row["total_mv"] / 10000,
                    "PE(TTM)": row["pe_ttm"],
                    "PB": row["pb"],
                    "现价": row["close"],
                    "最近周线日期": row["latest_week"],
                    "最近一周成交量": row["last_week_vol"],
                    "过去3周最大成交量": row["max_prev_vol"],
                    "周放量倍数": row["volume_ratio"],
                    "是否刚启动": bool(row.get("is_startup", False)),
                    "最近周涨跌幅%": row.get("last_week_pct_chg"),
                    "前三周累计涨跌幅%": row.get("prev3_sum_pct_chg"),
                    "一年区间位置": row.get("position_1y"),
                }
            )

        if not final_rows:
            logger.warning("没有最终结果")
            return

        final_df = pd.DataFrame(final_rows)
        # 优先按“刚启动”标记排序，其次按放量倍数 + 最近周涨幅，再按市值从大到小
        final_df = final_df.sort_values(
            by=["是否刚启动", "周放量倍数", "最近周涨跌幅%", "市值(亿)"],
            ascending=[False, False, False, False],
        )

        logger.info(
            f"\n🎉 筛选结果 (主板, 市值>500亿, PE<=20, 最近一周 > 过去3周最大成交量×1.3): 共 {len(final_df)} 只"
        )
        logger.info("=" * 140)
        logger.info(
            f"{'代码':<10} {'名称':<10} {'市值(亿)':<10} {'PE(TTM)':<10} {'PB':<8} "
            f"{'现价':<8} {'周放量倍数':<12} {'刚启动':<8} {'最近周涨幅%':<12} {'最近周线':<12}"
        )
        logger.info("-" * 140)

        for _, r in final_df.iterrows():
            startup_flag = "是" if r.get("是否刚启动") else "否"
            logger.info(
                f"{r['代码']:<10} {r['名称']:<10} "
                f"{r['市值(亿)']:<10.1f} {r['PE(TTM)']:<10.2f} {r['PB']:<8.2f} "
                f"{r['现价']:<8.2f} {r['周放量倍数']:<12.2f} "
                f"{startup_flag:<8} {(r['最近周涨跌幅%'] or 0):<12.2f} {str(r['最近周线日期'])[:10]:<12}"
            )

        logger.info("=" * 140)

        # 6. 保存到 CSV
        output_file = (
            f"low_pe_volume_surge_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        )
        final_df.to_csv(output_file, index=False, encoding="utf-8-sig")
        logger.info(f"\n💾 结果已保存至: {output_file}")


if __name__ == "__main__":
    analyzer = LowPEVolumeSurgeAnalyzer()
    analyzer.run_analysis()


