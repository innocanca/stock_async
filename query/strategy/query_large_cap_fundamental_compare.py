#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大市值股票池 + 财务指标（fina_indicator）合并，计算池内 / 行业内分位与简易质量综合分。
"""

import os
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)

# 用于分位与综合分的主要指标列（higher_is_better 为 True 表示值越大越好）
METRIC_SPECS: List[Tuple[str, str, bool]] = [
    ("roe", "roe", True),
    ("grossprofit_margin", "grossprofit_margin", True),
    ("netprofit_margin", "netprofit_margin", True),
    ("tr_yoy", "tr_yoy", True),
    ("netprofit_yoy", "netprofit_yoy", True),
    ("debt_to_assets", "debt_to_assets", False),
    ("ocf_to_or", "ocf_to_or", True),
]


class LargeCapFundamentalAnalyzer:
    """大票基本面多维度比较（估值 + 财务指标 + 分位）。"""

    def __init__(self) -> None:
        self.fetcher = StockDataFetcher()

    def _get_stock_basic_maps(
        self, ts_codes: List[str]
    ) -> Tuple[Dict[str, str], Dict[str, Optional[str]]]:
        names: Dict[str, str] = {}
        industries: Dict[str, Optional[str]] = {}
        if not ts_codes:
            return names, industries
        try:
            with StockDatabase() as db:
                cursor = db.connection.cursor()
                placeholders = ",".join(["%s"] * len(ts_codes))
                sql = f"SELECT ts_code, name, industry FROM stock_basic WHERE ts_code IN ({placeholders})"
                cursor.execute(sql, ts_codes)
                for ts_code, name, industry in cursor.fetchall():
                    names[ts_code] = name or ""
                    industries[ts_code] = industry if industry else None
        except Exception as e:
            logger.error(f"读取 stock_basic 失败: {e}")
        return names, industries

    def _valuation_frame(
        self,
        min_mv: float,
        main_board_only: bool,
        explicit_codes: Optional[List[str]],
    ) -> Tuple[pd.DataFrame, Optional[str]]:
        """返回 daily_basic 合并结果与所用 trade_date（字符串）。"""
        df: Optional[pd.DataFrame] = None
        trade_date_used: Optional[str] = None
        for i in range(8):
            trade_date = (datetime.now() - timedelta(days=i)).strftime("%Y%m%d")
            try:
                df = self.fetcher.pro.daily_basic(
                    trade_date=trade_date,
                    fields="ts_code,trade_date,close,pe_ttm,pb,total_mv",
                )
                if df is not None and not df.empty:
                    trade_date_used = trade_date
                    break
            except Exception as e:
                logger.warning(f"daily_basic {trade_date}: {e}")

        if df is None or df.empty:
            return pd.DataFrame(), None

        if explicit_codes:
            codes_set = set(explicit_codes)
            df = df[df["ts_code"].isin(codes_set)].copy()
        else:
            if main_board_only:
                df = df[df["ts_code"].str.match(r"^(60|00)\d{4}\.(SH|SZ)$")]
            df = df[df["total_mv"].notna() & (df["total_mv"] >= min_mv)].copy()

        df.sort_values(by="total_mv", ascending=False, inplace=True)
        return df, trade_date_used

    @staticmethod
    def _pool_percentiles(df: pd.DataFrame) -> pd.DataFrame:
        """池内分位：higher_is_better 用 rank pct；资产负债率用 1-pct。"""
        out = df.copy()
        for key, col, higher in METRIC_SPECS:
            if col not in out.columns:
                continue
            s = pd.to_numeric(out[col], errors="coerce")
            valid = s.notna()
            if valid.sum() == 0:
                out[f"pct_pool_{key}"] = np.nan
                continue
            rk = s.rank(pct=True, ascending=True)
            if higher:
                out[f"pct_pool_{key}"] = rk
            else:
                out[f"pct_pool_{key}"] = 1.0 - rk
        return out

    @staticmethod
    def _industry_percentiles(df: pd.DataFrame) -> pd.DataFrame:
        out = df.copy()
        if "industry" not in out.columns:
            return out

        for key, col, higher in METRIC_SPECS:
            if col not in out.columns:
                continue
            pct_col = f"pct_industry_{key}"
            series_out = pd.Series(np.nan, index=out.index)
            for _, grp in out.groupby("industry"):
                if len(grp) < 2:
                    continue
                val = pd.to_numeric(grp[col], errors="coerce")
                rk = val.rank(pct=True, ascending=True)
                if higher:
                    series_out.loc[grp.index] = rk
                else:
                    series_out.loc[grp.index] = 1.0 - rk
            out[pct_col] = series_out
        return out

    @staticmethod
    def _quality_score(df: pd.DataFrame) -> pd.Series:
        pool_cols = [f"pct_pool_{key}" for key, _, _ in METRIC_SPECS]
        cols = [c for c in pool_cols if c in df.columns]
        if not cols:
            return pd.Series(np.nan, index=df.index)
        return df[cols].mean(axis=1, skipna=True) * 100.0

    def compare(
        self,
        min_mv: float = 10000000,
        main_board_only: bool = False,
        ts_codes: Optional[List[str]] = None,
        fetch_fina: bool = True,
        fina_delay: float = 0.35,
        limit: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Returns:
            DataFrame of merged metrics + meta dict (trade_date, warnings, etc.)
        """
        explicit = [c.strip() for c in ts_codes] if ts_codes else None
        val_df, td = self._valuation_frame(min_mv, main_board_only, explicit)

        meta: Dict[str, Any] = {
            "trade_date_valuation": td,
            "min_mv_wan_yuan": min_mv,
            "main_board_only": main_board_only,
            "explicit_ts_codes": bool(explicit),
            "fina_note": (
                "财务指标来自 Tushare fina_indicator，各行 end_date 为该股最新财报期；"
                "同行业分位在行业内不少于 2 只股票时有效。"
            ),
        }

        if val_df.empty:
            meta["error"] = "无符合条件的估值数据（daily_basic）"
            return pd.DataFrame(), meta

        codes = val_df["ts_code"].tolist()
        if limit is not None and limit > 0:
            codes = codes[:limit]
            val_df = val_df[val_df["ts_code"].isin(codes)].copy()

        names, industries = self._get_stock_basic_maps(codes)
        val_df["name"] = val_df["ts_code"].map(lambda x: names.get(x, ""))
        val_df["industry"] = val_df["ts_code"].map(
            lambda x: (industries.get(x) or None) or "未知"
        )

        fina_latest = pd.DataFrame()
        if fetch_fina:
            logger.info(f"拉取 fina_indicator，共 {len(codes)} 只…")
            fina_latest = self.fetcher.get_fina_indicator_latest_batch(
                codes, delay=fina_delay
            )
            meta["fina_rows"] = len(fina_latest)
        else:
            meta["fina_rows"] = 0

        if fina_latest is not None and not fina_latest.empty:
            keep_cols = ["ts_code"] + [
                c
                for c in [
                    "ann_date",
                    "end_date",
                    "roe",
                    "grossprofit_margin",
                    "netprofit_margin",
                    "debt_to_assets",
                    "tr_yoy",
                    "netprofit_yoy",
                    "or_yoy",
                    "ocf_to_or",
                ]
                if c in fina_latest.columns
            ]
            fina_latest = fina_latest[[c for c in keep_cols if c in fina_latest.columns]]
            merged = val_df.merge(fina_latest, on="ts_code", how="left")
        else:
            merged = val_df.copy()
            for c in [
                "ann_date",
                "end_date",
                "roe",
                "grossprofit_margin",
                "netprofit_margin",
                "debt_to_assets",
                "tr_yoy",
                "netprofit_yoy",
                "or_yoy",
                "ocf_to_or",
            ]:
                merged[c] = np.nan

        merged = self._pool_percentiles(merged)
        merged = self._industry_percentiles(merged)
        merged["quality_score"] = self._quality_score(merged)

        # 市值亿元
        merged["total_mv_yi"] = merged["total_mv"] / 10000.0

        meta["count"] = len(merged)
        return merged, meta

    def compare_to_records(
        self,
        **kwargs: Any,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        df, meta = self.compare(**kwargs)
        if df.empty:
            return [], meta

        # JSON 友好：NaN -> None，float 适度 round
        def _clean_row(r: Dict[str, Any]) -> Dict[str, Any]:
            out: Dict[str, Any] = {}
            for k, v in r.items():
                if pd.isna(v):
                    out[k] = None
                elif isinstance(v, (np.floating, float)):
                    if k.startswith("pct_") or k in ("quality_score",):
                        out[k] = round(float(v), 4) if v == v else None
                    else:
                        out[k] = round(float(v), 6) if v == v else None
                elif isinstance(v, (np.integer, int)):
                    out[k] = int(v)
                else:
                    out[k] = v
            return out

        records = [_clean_row(x) for x in df.to_dict(orient="records")]
        records.sort(
            key=lambda x: (x.get("quality_score") is None, -(x.get("quality_score") or 0.0))
        )
        return records, meta

    def by_industry_from_records(
        self, records: List[Dict[str, Any]]
    ) -> Dict[str, List[Dict[str, Any]]]:
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for r in records:
            ind = r.get("industry") or "未知"
            buckets.setdefault(ind, []).append(r)
        for ind in buckets:
            buckets[ind].sort(
                key=lambda x: (x.get("quality_score") is None, -(x.get("quality_score") or 0))
            )
        return dict(sorted(buckets.items(), key=lambda x: x[0]))
