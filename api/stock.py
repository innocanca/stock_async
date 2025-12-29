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
from query.strategy.query_smart_portfolio import SmartPortfolioAnalyzer
from query.strategy.query_daily_bottom_volume_surge import DailyBottomVolumeSurgeAnalyzer

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

@router.get("/weekly_bottom_reversal")
def api_weekly_bottom_reversal(
    min_mv: float = 1000000,
    min_drop_weeks: int = 3,
    vol_ratio: float = 1.5
):
    """
    查询周线底部放量反转的主板股票。

    - min_mv: 最小总市值（万元），默认 100 亿 (1,000,000)
    - min_drop_weeks: 反转前最少连续下跌周数，默认 3 周
    - vol_ratio: 本周成交量相对于前几周平均成交量的放大倍数，默认 1.5 倍
    """
    analyzer = WeeklyBottomReversalAnalyzer()
    results = analyzer.get_analysis_results(
        min_mv=min_mv,
        min_drop_weeks=min_drop_weeks,
        vol_ratio=vol_ratio
    )

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


@router.get("/smart_portfolio")
def api_smart_portfolio(limit: int = 5):
    """
    智能投资组合推荐策略。
    
    聚合多个策略结果，自动进行行业去重，精选 3-5 只优质标的。
    """
    analyzer = SmartPortfolioAnalyzer()
    results = analyzer.get_portfolio_recommendation(limit=limit)
    return results


@router.get("/daily_bottom_volume_surge")
def api_daily_bottom_volume_surge(
    vol_ratio: float = 3.0,
    price_pos: float = 0.2
):
    """
    日线级别放巨量，250日线下方，底部，主板。
    
    - vol_ratio: 成交量放大倍数阈值，默认 3.0
    - price_pos: 价格位置阈值 (0-1)，默认 0.2 (处于过去250天波动的低位20%)
    """
    analyzer = DailyBottomVolumeSurgeAnalyzer()
    results = analyzer.get_analysis_results(
        vol_ratio_threshold=vol_ratio,
        price_pos_threshold=price_pos
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


@router.get("/top_active_stocks")
def api_top_active_stocks(limit: int = 20, sort_by: str = "amount"):
    """
    获取市场最活跃（成交额或成交量最大）的前 N 只股票。

    - limit: 返回数量，默认 20
    - sort_by: 排序字段，可选 'amount' (成交额，默认) 或 'vol' (成交量)
    """
    if sort_by not in ["amount", "vol"]:
        sort_by = "amount"

    db = StockDatabase()
    if not db.connect():
        return JSONResponse(
            content={"error": "database_connect_failed"},
            status_code=500,
        )

    try:
        with db.connection.cursor() as cursor:
            # 1. 获取最新交易日
            cursor.execute("SELECT MAX(trade_date) FROM daily_data")
            latest_date = cursor.fetchone()[0]
            if not latest_date:
                return {"count": 0, "data": [], "message": "No data found"}

            # 2. 查询成交额最大的前 N 只股票，并关联 stock_basic 获取名称
            sql = f"""
            SELECT 
                d.ts_code, 
                b.name, 
                d.trade_date, 
                d.close as '现价', 
                d.change_pct as '涨跌幅', 
                d.vol as '成交量(手)', 
                d.amount / 10000 as '成交额(亿元)'
            FROM daily_data d
            LEFT JOIN stock_basic b ON d.ts_code = b.ts_code
            WHERE d.trade_date = %s
            ORDER BY d.{sort_by} DESC
            LIMIT %s
            """
            cursor.execute(sql, (latest_date, limit))
            
            # 获取结果并转换格式
            columns = [col[0] for col in cursor.description]
            records = []
            for row in cursor.fetchall():
                record = dict(zip(columns, row))
                # 处理日期和数值类型
                if record["trade_date"]:
                    record["trade_date"] = record["trade_date"].strftime("%Y-%m-%d")
                for k, v in record.items():
                    if hasattr(v, "to_eng_string"):  # 处理 Decimal
                        record[k] = float(v)
                records.append(record)

            return {
                "latest_trade_date": latest_date.strftime("%Y-%m-%d"),
                "count": len(records),
                "data": records,
            }
    except Exception as e:
        return JSONResponse(
            content={"error": str(e)},
            status_code=500,
        )
    finally:
        db.disconnect()


