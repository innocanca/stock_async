#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
与股票相关的 HTTP 接口。
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from datetime import datetime, timedelta
from typing import List, Dict, Optional

from database import StockDatabase
from query.strategy.query_low_pe_volume_surge import LowPEVolumeSurgeAnalyzer
from query.strategy.query_consecutive_yang_lines import ConsecutiveYangLinesAnalyzer
from query.strategy.query_weekly_bottom_reversal import WeeklyBottomReversalAnalyzer
from query.strategy.query_etf_weekly_volume_surge import ETFWeeklyVolumeSurgeAnalyzer

router = APIRouter()


@router.get("/large_cap_below_1y_avg_price")
def api_large_cap_below_1y_avg_price(
    min_mv: float = 10000000,
    max_pe: float = 30.0,
):
    """
    查询市值大于 min_mv（万元）、PE 不超过 max_pe，且当前价格低于最近 1 年平均价的股票列表。

    - 默认市值阈值：1000 亿（10,000,000 万元）
    - 默认 PE 上限：30
    """
    analyzer = LowPEVolumeSurgeAnalyzer()
    df = analyzer.query_large_cap_below_1y_avg_price(min_mv=min_mv, max_pe=max_pe)

    if df is None or df.empty:
        return JSONResponse(
            content={"count": 0, "data": []},
            status_code=200,
        )

    records = df.to_dict(orient="records")
    return {
        "count": len(records),
        "data": records,
    }


@router.get("/low_pe_volume_surge")
def api_low_pe_volume_surge(
    min_mv: float = 2000000,
    max_pe: Optional[float] = None,
    min_ratio: float = 1.3,
):
    """
    查询主板放量上涨股票（策略：低PE + 周线放量）。

    - min_mv: 最小总市值（万元），默认 200 亿 (2,000,000)
    - max_pe: 最大市盈率 (TTM)，默认不限制
    - min_ratio: 周线放量倍数阈值，默认 1.3
    """
    analyzer = LowPEVolumeSurgeAnalyzer()
    results = analyzer.get_analysis_results(
        min_mv=min_mv, 
        max_pe=max_pe, 
        min_ratio=min_ratio
    )

    return {
        "count": len(results),
        "data": results,
    }


@router.get("/consecutive_yang_lines")
def api_consecutive_yang_lines(min_consecutive: int = 3):
    """
    查询周线连续阳线的千亿市值股票。

    - min_consecutive: 最少连续阳线周数，默认 3 周。
    """
    analyzer = ConsecutiveYangLinesAnalyzer()
    results = analyzer.get_analysis_results(min_consecutive=min_consecutive)

    return {
        "count": len(results),
        "data": results,
    }


    return {
        "count": len(results),
        "data": results,
    }


@router.get("/etf_weekly_volume_surge")
def api_etf_weekly_volume_surge(
    min_ratio: float = 1.5,
    lookback_weeks: int = 3,
    min_last_week_amount_yi: float = 1.0,
):
    """
    查询周线明显放量的 ETF。

    - min_ratio: 最小放量倍数，默认 1.5
    - lookback_weeks: 回看周数，默认 3 周
    - min_last_week_amount_yi: 最近一周成交额阈值（亿元），默认 1.0
    """
    analyzer = ETFWeeklyVolumeSurgeAnalyzer()
    results = analyzer.get_analysis_results(
        min_ratio=min_ratio,
        lookback_weeks=lookback_weeks,
        min_last_week_amount_yi=min_last_week_amount_yi,
    )

    return {
        "count": len(results),
        "data": results,
    }


@router.get("/price_volume_1y")
def api_price_volume_1y(ts_code: str):
    """
    根据指定股票 ts_code 查询最近 1 年的日线价格和成交量数据（从本地数据库 daily_data 表读取）。

    返回字段示例：
    - trade_date: 交易日期（YYYY-MM-DD）
    - open/high/low/close: 当日价格
    - vol: 当日成交量
    - amount: 当日成交额
    """
    # 计算最近 1 年的起止日期（按自然日向前推 365 天）
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=365)

    db = StockDatabase()
    if not db.connect():
        return JSONResponse(
            content={"ts_code": ts_code, "count": 0, "data": [], "error": "database_connect_failed"},
            status_code=500,
        )

    try:
        import pandas as pd

        sql = """
        SELECT trade_date, open, high, low, close, vol, amount
        FROM daily_data
        WHERE ts_code = %s
          AND trade_date >= %s
          AND trade_date <= %s
        ORDER BY trade_date ASC
        """
        params = [ts_code, start_date, end_date]
        df = pd.read_sql(sql, db.connection, params=params)
    finally:
        db.disconnect()

    if df is None or df.empty:
        return JSONResponse(
            content={"ts_code": ts_code, "count": 0, "data": []},
            status_code=200,
        )

    # 将日期转为字符串，方便前端使用
    df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y-%m-%d")
    records = df.to_dict(orient="records")
    return {
        "ts_code": ts_code,
        "count": len(records),
        "data": records,
    }


