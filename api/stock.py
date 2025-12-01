#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
与股票相关的 HTTP 接口。
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from query_low_pe_volume_surge import LowPEVolumeSurgeAnalyzer

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



