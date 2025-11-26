#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日增量更新脚本

目标：每天执行一次，完成核心数据的增量更新（可直接丢到 crontab / systemd 里运行）

包含内容：
1. 股票基础信息 `stock_basic`（全量刷新一次，数据库里做 UPSERT）
2. 最新交易日的日线数据 `daily_data`（调用已有的增量同步逻辑）
3. 最近若干周的周线数据 `weekly_data`（重复插入是幂等的）
4. 最近 1 年的利润表 / 现金流量表 / 最近 2 年的分红送股（按主板股票全量拉取，依赖表的唯一键做 UPSERT，等价于“软增量”）

使用方法：
    python sync_daily_incremental.py
"""

import os
import sys
from datetime import datetime
from typing import List

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from log_config import get_logger
from fetcher import StockDataFetcher
from database import StockDatabase
from scheduler import DailyDataSyncer, StockDataScheduler

logger = get_logger(__name__)


def sync_stock_basic() -> bool:
    """刷新股票基础信息（上市状态、名称等）到 `stock_basic` 表。"""
    logger.info("🔄 开始同步股票基础信息 stock_basic ...")
    try:
        fetcher = StockDataFetcher()
        df = fetcher.get_stock_basic(list_status="L")
        if df is None or df.empty:
            logger.warning("⚠️ 未从 Tushare 获取到股票基础信息，跳过。")
            return False

        with StockDatabase() as db:
            ok = db.insert_stock_basic(df)

        if ok:
            logger.info(f"✅ 股票基础信息同步完成，记录数：{len(df)}")
        else:
            logger.error("❌ 股票基础信息插入数据库失败")
        return ok
    except Exception as e:
        logger.error(f"同步股票基础信息失败: {e}")
        return False


def sync_daily_data() -> bool:
    """
    同步最新交易日的日线数据 `daily_data`。
    内部复用 `scheduler.DailyDataSyncer` 的增量逻辑（只补缺失的股票）。
    """
    logger.info("🔄 开始增量同步最新交易日的日线数据 daily_data ...")
    ok = DailyDataSyncer.sync_today()
    if ok:
        logger.info("✅ 日线数据增量同步完成")
    else:
        logger.error("❌ 日线数据增量同步失败")
    return ok


def sync_weekly_data(weeks_back: int = 8) -> bool:
    """
    同步最近若干周的周线数据 `weekly_data`。
    - 使用最近 N 周的时间窗口反复拉取并 UPSERT，幂等。
    - 建议每天跑一次即可保持周线基本实时。
    """
    logger.info(f"🔄 开始同步最近 {weeks_back} 周的周线数据 weekly_data ...")
    scheduler = StockDataScheduler()
    ok = scheduler.sync_weekly_data(weeks_back=weeks_back)
    if ok:
        logger.info("✅ 周线数据同步完成")
    else:
        logger.error("❌ 周线数据同步失败")
    return ok


def get_main_board_stocks_from_db(db: StockDatabase) -> List[str]:
    """
    从数据库中获取主板股票列表（和综合财务初始化脚本保持一致的口径）。
    """
    cursor = db.connection.cursor()
    cursor.execute(
        """
        SELECT DISTINCT ts_code 
        FROM stock_basic 
        WHERE list_status = 'L'
          AND name NOT LIKE '%ST%'
          AND name NOT LIKE '%退%'
          AND name NOT LIKE '%暂停%'
          AND (ts_code LIKE '60____.SH' OR ts_code LIKE '00____.SZ')
        ORDER BY ts_code
        """
    )
    rows = cursor.fetchall()
    if not rows:
        logger.warning("⚠️ 数据库中未找到主板股票，将使用备用主板列表。")
        fetcher = StockDataFetcher()
        return fetcher.get_main_board_stocks()
    codes = [r[0] for r in rows]
    logger.info(f"📈 从数据库读取到 {len(codes)} 只主板股票用于财务增量更新")
    return codes


def sync_financial_data(years_back_income_cashflow: int = 1, years_back_dividend: int = 2) -> bool:
    """
    增量同步财务数据：
    - 利润表 income_data：最近 N 年
    - 现金流量表 cashflow_data：最近 N 年
    - 分红送股 dividend_data：最近 M 年

    实现方式：
    - 对主板股票全量拉取最近 N 年数据
    - 依赖数据库表上的 UNIQUE KEY + ON DUPLICATE KEY UPDATE 做 UPSERT
      => 已有记录会被更新，新记录自动插入，相当于“软增量”。
    """
    logger.info("🔄 开始增量同步财务数据（利润表 / 现金流量表 / 分红送股）...")

    fetcher = StockDataFetcher()
    overall_ok = True

    with StockDatabase() as db:
        stock_codes = get_main_board_stocks_from_db(db)
        if not stock_codes:
            logger.error("❌ 无法获取主板股票列表，财务数据增量更新终止。")
            return False

        # 1. 利润表
        logger.info(f"📈 同步利润表 income_data，最近 {years_back_income_cashflow} 年 ...")
        income_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type="income",
            years_back=years_back_income_cashflow,
            batch_size=20,
            delay=0.5,
        )
        if income_df is not None and not income_df.empty:
            if db.insert_income_data(income_df):
                logger.info(
                    f"✅ 利润表增量更新完成：股票 {income_df['ts_code'].nunique()} 只，记录 {len(income_df)} 条"
                )
            else:
                logger.error("❌ 利润表增量插入失败")
                overall_ok = False
        else:
            logger.warning("⚠️ 未获取到任何新的利润表数据")

        # 2. 现金流量表
        logger.info(f"💰 同步现金流量表 cashflow_data，最近 {years_back_income_cashflow} 年 ...")
        cashflow_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type="cashflow",
            years_back=years_back_income_cashflow,
            batch_size=20,
            delay=0.5,
        )
        if cashflow_df is not None and not cashflow_df.empty:
            if db.insert_cashflow_data(cashflow_df):
                logger.info(
                    f"✅ 现金流量表增量更新完成：股票 {cashflow_df['ts_code'].nunique()} 只，记录 {len(cashflow_df)} 条"
                )
            else:
                logger.error("❌ 现金流量表增量插入失败")
                overall_ok = False
        else:
            logger.warning("⚠️ 未获取到任何新的现金流量表数据")

        # 3. 分红送股
        logger.info(f"🎁 同步分红送股 dividend_data，最近 {years_back_dividend} 年 ...")
        dividend_df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type="dividend",
            years_back=years_back_dividend,
            batch_size=30,
            delay=0.3,
        )
        if dividend_df is not None and not dividend_df.empty:
            if db.insert_dividend_data(dividend_df):
                logger.info(
                    f"✅ 分红送股增量更新完成：股票 {dividend_df['ts_code'].nunique()} 只，记录 {len(dividend_df)} 条"
                )
            else:
                logger.error("❌ 分红送股增量插入失败")
                overall_ok = False
        else:
            logger.warning("⚠️ 未获取到任何新的分红送股数据")

    return overall_ok


def main() -> bool:
    logger.info("🚀 每日增量更新开始 ...")
    start_time = datetime.now()

    ok_basic = sync_stock_basic()
    ok_daily = sync_daily_data()
    ok_weekly = sync_weekly_data(weeks_back=8)
    ok_fin = sync_financial_data()

    total_ok = ok_basic and ok_daily and ok_weekly and ok_fin

    duration = datetime.now() - start_time
    logger.info("==============================================")
    logger.info("📊 每日增量更新汇总：")
    logger.info(f"   股票基础信息   : {'✅' if ok_basic else '❌'}")
    logger.info(f"   日线行情 daily : {'✅' if ok_daily else '❌'}")
    logger.info(f"   周线行情 weekly: {'✅' if ok_weekly else '❌'}")
    logger.info(f"   财务&分红数据  : {'✅' if ok_fin else '❌'}")
    logger.info(f"   总耗时         : {duration}")

    if total_ok:
        logger.info("🎉 每日增量更新全部成功！")
    else:
        logger.error("⚠️ 每日增量更新存在失败项，请检查日志。")

    return total_ok


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)


