#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询周线放量的主板股票（市值>200亿，不限制 PE）

筛选条件（当前版本）：
1. 市场板块：主板（60xxxx.SH / 00xxxx.SZ）
2. 市值：总市值 > 200亿（total_mv >= 2,000,000 万元）
3. 成交量：最近一周周线成交量 > 过去3周所有周最大成交量 × 1.3，且放量当周为上涨周

使用方法：
    python query_low_pe_volume_surge.py
"""

import sys
import os
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


class LowPEVolumeSurgeAnalyzer:
    """低PE + 周线放量 筛选器"""

    def __init__(self):
        self.db = StockDatabase()
        self.fetcher = StockDataFetcher()

    def get_market_valuations(
        self,
        min_mv: Optional[float] = 5000000,
        max_pe: Optional[float] = 30,
    ) -> pd.DataFrame:
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
            if min_mv is not None:
                df = df[df["total_mv"] >= min_mv]
                logger.info(f"   市值>{min_mv/10000:.0f}亿的股票数量: {len(df)}")
            else:
                logger.info("   不限制总市值")

            # PE 过滤：0 < PE <= max_pe
            if max_pe is not None:
                df = df[(df["pe_ttm"] > 0) & (df["pe_ttm"] <= max_pe)]
                logger.info(f"   PE(TTM)<={max_pe} 的股票数量: {len(df)}")
            else:
                logger.info("   不限制 PE(TTM)")

            return df

        except Exception as e:
            logger.error(f"获取估值数据失败: {e}")
            return pd.DataFrame()

    def list_stocks_by_market_cap(
        self,
        min_mv: float = 10000000,
        main_board_only: bool = False,
    ) -> List[Dict]:
        """
        按总市值筛选股票（基于 Tushare daily_basic 最新交易日数据）。

        Args:
            min_mv: 最小总市值（万元），1000 亿 = 10,000,000 万元
            main_board_only: 为 True 时仅保留沪市/深市主板（60/00 开头）

        Returns:
            字典列表，按总市值从高到低排序
        """
        logger.info(f"📊 按市值筛选：total_mv>={min_mv} 万元，主板仅={main_board_only}...")
        df: Optional[pd.DataFrame] = None
        try:
            for i in range(5):
                trade_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
                try:
                    df = self.fetcher.pro.daily_basic(
                        trade_date=trade_date,
                        fields="ts_code,trade_date,close,pe_ttm,pb,total_mv",
                    )
                    if df is not None and not df.empty:
                        logger.info(f"   使用 {trade_date} 的 daily_basic，共 {len(df)} 条")
                        break
                except Exception as e:
                    logger.warning(f"   获取 {trade_date} daily_basic 失败: {e}")

            if df is None or df.empty:
                logger.error("无法获取 daily_basic 估值数据")
                return []

            if main_board_only:
                df = df[df["ts_code"].str.match(r"^(60|00)\d{4}\.(SH|SZ)$")]
                logger.info(f"   主板过滤后: {len(df)} 条")

            df = df[df["total_mv"].notna() & (df["total_mv"] >= min_mv)].copy()
            df.sort_values(by="total_mv", ascending=False, inplace=True)

            codes = df["ts_code"].tolist()
            names = self.get_stock_names(codes)

            rows: List[Dict] = []
            for _, row in df.iterrows():
                tc = row["ts_code"]
                tmv = float(row["total_mv"])
                rows.append(
                    {
                        "ts_code": tc,
                        "name": names.get(tc, ""),
                        "trade_date": str(row["trade_date"]) if pd.notna(row["trade_date"]) else None,
                        "close": float(row["close"]) if pd.notna(row["close"]) else None,
                        "total_mv": tmv,
                        "total_mv_10k": tmv,
                        "市值(亿)": round(tmv / 10000, 2),
                        "pe_ttm": float(row["pe_ttm"]) if pd.notna(row["pe_ttm"]) else None,
                        "pb": float(row["pb"]) if pd.notna(row["pb"]) else None,
                    }
                )
            return rows
        except Exception as e:
            logger.error(f"按市值筛选失败: {e}")
            return []

    def get_weekly_volume_surge(
        self,
        stock_codes: List[str],
        min_ratio: float = 1.3,
        lookback_weeks: int = 3,
    ) -> pd.DataFrame:
        """
        计算周线放量情况 + 判断是否“刚启动”：
        - 放量：最近一周成交量 / 过去 N 周「最大成交量」
        - 放量当周要求是上涨周：最近一周周涨跌幅 > 0
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

                # 只保留“放量且上涨”的周线：放量满足阈值，且当周涨跌幅为正
                if ratio >= min_ratio and last_pct > 0:
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

    def query_large_cap_below_1y_avg_price(
        self,
        min_mv: float = 10000000,
        max_pe: float = 30.0,
    ) -> pd.DataFrame:
        """
        查询市值大于指定阈值且当前价格低于最近1年平均价、PE 不超过上限的股票列表。

        条件：
        1. 市值：total_mv >= min_mv（单位：万元），默认 1000 亿 = 10,000,000 万元
        2. 市盈率：0 < PE(TTM) <= max_pe，默认 30
        3. 价格：最新收盘价 < 最近 1 年（按周线）收盘价平均值

        返回字段（示例）：
        - ts_code: 代码
        - name: 名称
        - total_mv: 总市值（万元）
        - pe_ttm: 滚动市盈率
        - close: 当前收盘价（daily_basic 中最新一日）
        - avg_close_1y: 最近 1 年周线收盘价的平均值
        """
        logger.info(
            f"🔍 查询市值>{min_mv/10000:.0f}亿、PE<={max_pe}，且当前价格低于最近1年平均价的股票列表..."
        )

        # 1. 先从 daily_basic 估值数据中筛选出市值 + PE 约束
        df_valuation = self.get_market_valuations(min_mv=min_mv, max_pe=max_pe)
        if df_valuation.empty:
            logger.warning("没有找到符合市值与 PE 条件的股票")
            return pd.DataFrame()

        target_codes = df_valuation["ts_code"].tolist()

        try:
            with self.db:
                cursor = self.db.connection.cursor()
                # 获取 weekly_data 中最近一条周线日期，作为 1 年窗口的截止
                cursor.execute("SELECT MAX(trade_date) FROM weekly_data")
                result = cursor.fetchone()
                latest_week = result[0] if result else None

                if not latest_week:
                    logger.error("weekly_data 表中没有周线数据，无法计算最近1年的平均价格")
                    return pd.DataFrame()

                logger.info(f"   最近周线日期: {latest_week}")

                placeholders = ",".join(["%s"] * len(target_codes))
                sql = f"""
                SELECT ts_code, trade_date, close
                FROM weekly_data
                WHERE trade_date <= %s
                  AND trade_date >= DATE_SUB(%s, INTERVAL 365 DAY)
                  AND ts_code IN ({placeholders})
                ORDER BY ts_code, trade_date
                """
                params = [latest_week, latest_week] + target_codes
                df_weekly = pd.read_sql(sql, self.db.connection, params=params)

            if df_weekly.empty:
                logger.warning("没有查询到用于计算最近1年平均价格的周线数据")
                return pd.DataFrame()

            logger.info("   开始按股票计算最近1年周线收盘价平均值...")
            stats_rows = []
            for ts_code, g in df_weekly.groupby("ts_code"):
                g = g.sort_values("trade_date")
                if g.empty:
                    continue

                last_close = float(g.iloc[-1]["close"])
                avg_close_1y = float(g["close"].mean())
                if avg_close_1y <= 0:
                    continue

                if last_close < avg_close_1y:
                    stats_rows.append(
                        {
                            "ts_code": ts_code,
                            "avg_close_1y": avg_close_1y,
                            "weekly_last_close": last_close,
                        }
                    )

            if not stats_rows:
                logger.warning("没有股票满足“当前价格低于最近1年平均价”的条件")
                return pd.DataFrame()

            df_stats = pd.DataFrame(stats_rows)

            # 2. 将 1 年均价信息与估值数据合并
            merged = pd.merge(df_valuation, df_stats, on="ts_code", how="inner")
            if merged.empty:
                logger.warning("估值数据与1年平均价数据合并后为空")
                return pd.DataFrame()

            # 3. 获取名称，并整理输出
            stock_names = self.get_stock_names(merged["ts_code"].tolist())

            merged["name"] = merged["ts_code"].map(lambda c: stock_names.get(c, c))
            merged.rename(
                columns={
                    "close": "current_close",
                    "total_mv": "total_mv_10k",
                },
                inplace=True,
            )

            # 只保留关心的列，并做一点排序：按市值从大到小
            output = merged[
                [
                    "ts_code",
                    "name",
                    "total_mv_10k",
                    "pe_ttm",
                    "current_close",
                    "weekly_last_close",
                    "avg_close_1y",
                ]
            ].copy()

            output.sort_values(by=["total_mv_10k"], ascending=False, inplace=True)

            logger.info(
                f"✅ 最终满足条件的股票数量: {len(output)}（市值>{min_mv/10000:.0f}亿、PE<={max_pe}、现价低于最近1年平均价）"
            )

            return output

        except Exception as e:
            logger.error(f"查询市值/PE/一年均价组合条件失败: {e}")
            return pd.DataFrame()

    def run_analysis(self, min_mv: float = 2000000, max_pe: Optional[float] = None, min_ratio: float = 1.3):
        """执行综合筛选：主板 + 市值>200亿 + 周线放量（不限制 PE）"""
        results = self.get_analysis_results(min_mv=min_mv, max_pe=max_pe, min_ratio=min_ratio)
        if not results:
            return

        final_df = pd.DataFrame(results)
        logger.info(
            f"\n🎉 筛选结果 (主板, 最近一周 > 过去3周最大成交量×{min_ratio} 且放量周为上涨周): 共 {len(final_df)} 只"
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

    def get_analysis_results(
        self,
        min_mv: float = 2000000,
        max_pe: Optional[float] = None,
        min_ratio: float = 1.3,
        lookback_weeks: int = 3
    ) -> List[Dict]:
        """
        获取综合筛选结果列表，主要供 API 调用。
        """
        logger.info(
            f"🚀 开始综合筛选：市值>{min_mv/10000:.0f}亿, 放量阈值>{min_ratio}倍..."
        )
        
        # 1. 先从估值维度获取主板股票，并限制市值>200亿（PE 仅作为展示字段，不作过滤）
        df_valuation = self.get_market_valuations(min_mv=min_mv, max_pe=max_pe)
        if df_valuation.empty:
            logger.warning("没有找到符合估值条件的股票")
            return []

        target_codes = df_valuation["ts_code"].tolist()

        with self.db:
            # 2. 在估值合格的股票里，再筛选周线放量
            surge_df = self.get_weekly_volume_surge(
                stock_codes=target_codes, min_ratio=min_ratio, lookback_weeks=lookback_weeks
            )
            if surge_df.empty:
                logger.warning(f"在符合估值条件的股票中，没有找到满足「最近一周 > 过去{lookback_weeks}周最大成交量×{min_ratio}」的标的")
                return []

        # 3. 合并估值 + 周线放量信息
        merged = pd.merge(df_valuation, surge_df, on="ts_code", how="inner")
        if merged.empty:
            logger.warning("估值数据与周线放量数据合并后为空")
            return []

        # 3.1 只保留“一年内区间位置在下半部”的标的
        if "position_1y" in merged.columns:
            before_cnt = len(merged)
            merged = merged[merged["position_1y"].notna() & (merged["position_1y"] <= 0.5)].copy()
            logger.info(f"   按一年区间下半部过滤: {before_cnt} -> {len(merged)} 只")
            if merged.empty:
                logger.warning("当前没有满足“一年内区间位置在下半部”的标的")
                return []

        # 4. 获取股票名称
        stock_names = self.get_stock_names(merged["ts_code"].tolist())

        # 5. 组织最终结果
        final_rows = []
        for _, row in merged.iterrows():
            ts_code = row["ts_code"]
            final_rows.append(
                {
                    "ts_code": ts_code,
                    "代码": ts_code,
                    "名称": stock_names.get(ts_code, ts_code),
                    "市值(亿)": float(row["total_mv"] / 10000),
                    "total_mv": float(row["total_mv"]),
                    "pe_ttm": float(row["pe_ttm"]) if pd.notna(row["pe_ttm"]) else None,
                    "PE(TTM)": float(row["pe_ttm"]) if pd.notna(row["pe_ttm"]) else None,
                    "pb": float(row["pb"]) if pd.notna(row["pb"]) else None,
                    "PB": float(row["pb"]) if pd.notna(row["pb"]) else None,
                    "close": float(row["close"]),
                    "现价": float(row["close"]),
                    "latest_week": str(row["latest_week"]),
                    "最近周线日期": str(row["latest_week"]),
                    "volume_ratio": float(row["volume_ratio"]),
                    "周放量倍数": float(row["volume_ratio"]),
                    "is_startup": bool(row.get("is_startup", False)),
                    "是否刚启动": bool(row.get("is_startup", False)),
                    "last_week_pct_chg": float(row.get("last_week_pct_chg", 0)),
                    "最近周涨跌幅%": float(row.get("last_week_pct_chg", 0)),
                    "position_1y": float(row.get("position_1y", 0)) if pd.notna(row.get("position_1y")) else None,
                    "一年区间位置": float(row.get("position_1y", 0)) if pd.notna(row.get("position_1y")) else None,
                }
            )

        # 排序
        final_rows.sort(
            key=lambda x: (x["是否刚启动"], x["周放量倍数"], x["最近周涨跌幅%"], x["市值(亿)"]),
            reverse=True
        )
        
        return final_rows


if __name__ == "__main__":
    analyzer = LowPEVolumeSurgeAnalyzer()
    analyzer.run_analysis()




