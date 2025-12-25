#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日全量增量同步脚本

目标：每天定时执行，同步股票、指数、ETF、财务等核心数据。
包含内容：
1. 股票基础信息 (stock_basic)
2. 股票日线/周线行情 (daily_data, weekly_data)
3. 指数基础信息 (index_basic)
4. 指数日线/周线行情 (index_daily, index_weekly)
5. 指数成分和权重 (index_weight)
6. ETF基础信息/日线行情 (etf_basic, etf_daily)
7. 同花顺概念指数及成分 (ths_index, ths_member)
8. 财务数据 (income, cashflow, dividend)
"""

import os
import sys
from datetime import datetime, timedelta
from typing import List

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from log_config import get_logger
from fetcher import StockDataFetcher
from database import StockDatabase
from scheduler import StockDataScheduler

logger = get_logger(__name__)

def sync_stock_basic() -> bool:
    """刷新股票基础信息"""
    logger.info("🔄 同步股票基础信息 (stock_basic)...")
    try:
        fetcher = StockDataFetcher()
        df = fetcher.get_stock_basic(list_status="L")
        if df is None or df.empty:
            logger.warning("⚠️ 未能获取到股票基础信息")
            return False
        with StockDatabase() as db:
            return db.insert_stock_basic(df)
    except Exception as e:
        logger.error(f"❌ 同步股票基础信息失败: {e}")
        return False

def sync_stock_daily(days_back: int = 5) -> bool:
    """同步股票日线行情"""
    logger.info(f"🔄 同步股票日线行情 (最近 {days_back} 天)...")
    try:
        fetcher = StockDataFetcher()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        with StockDatabase() as db:
            stats = fetcher.get_all_market_data_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                db_instance=db,
                batch_days=5
            )
            return stats.get('total_records', 0) > 0
    except Exception as e:
        logger.error(f"❌ 同步股票日线行情失败: {e}")
        return False

def sync_stock_weekly(weeks_back: int = 4) -> bool:
    """同步股票周线行情"""
    logger.info(f"🔄 同步股票周线行情 (最近 {weeks_back} 周)...")
    try:
        scheduler = StockDataScheduler()
        return scheduler.sync_weekly_data(weeks_back=weeks_back)
    except Exception as e:
        logger.error(f"❌ 同步股票周线行情失败: {e}")
        return False

def sync_index_data(days_back: int = 7) -> bool:
    """同步指数基本信息、日线、周线、权重"""
    logger.info("🔄 同步指数数据 (basic, daily, weekly, weight)...")
    try:
        fetcher = StockDataFetcher()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        with StockDatabase() as db:
            # 1. 基础信息
            basic_df = fetcher.get_all_index_basic_data()
            if basic_df is not None:
                db.insert_index_basic(basic_df)
            
            # 2. 日线
            fetcher.get_all_index_daily_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                db_instance=db
            )
            
            # 3. 周线
            fetcher.get_all_index_weekly_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                db_instance=db
            )
            
            # 4. 权重 (仅同步主要指数)
            major_indexes = ['000001.SH', '000300.SH', '000905.SH', '399001.SZ', '399006.SZ']
            for code in major_indexes:
                weight_df = fetcher.get_index_weight(index_code=code, start_date=start_date, end_date=end_date)
                if weight_df is not None:
                    db.insert_index_weight(weight_df)
                    
        return True
    except Exception as e:
        logger.error(f"❌ 同步指数数据失败: {e}")
        return False

def sync_etf_data(days_back: int = 7) -> bool:
    """同步ETF基础信息和日线行情"""
    logger.info("🔄 同步ETF数据 (basic, daily)...")
    try:
        fetcher = StockDataFetcher()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        with StockDatabase() as db:
            # 基础信息
            etf_basic = fetcher.get_etf_basic()
            if etf_basic is not None:
                db.insert_etf_basic(etf_basic)
            
            # 日线
            fetcher.get_all_etf_daily_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                db_instance=db
            )
        return True
    except Exception as e:
        logger.error(f"❌ 同步ETF数据失败: {e}")
        return False

def sync_ths_data() -> bool:
    """同步同花顺概念指数及成分"""
    logger.info("🔄 同步同花顺数据 (index, member)...")
    try:
        fetcher = StockDataFetcher()
        with StockDatabase() as db:
            # 指数信息
            ths_index = fetcher.get_all_ths_index_data()
            if ths_index is not None:
                db.insert_ths_index(ths_index)
                
            # 成分股 (由于数量巨大，每日仅同步概念指数 N 的成分)
            fetcher.get_concept_members_batch_with_db_insert(db_instance=db)
        return True
    except Exception as e:
        logger.error(f"❌ 同步同花顺数据失败: {e}")
        return False

def sync_index_dailybasic(days_back: int = 7) -> bool:
    """同步大盘指数每日指标 (PE, PB, turnover等)"""
    logger.info(f"🔄 同步指数每日指标 (最近 {days_back} 天)...")
    try:
        fetcher = StockDataFetcher()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        major_indexes = ['000001.SH', '000300.SH', '000905.SH', '000016.SH', '399001.SZ', '399006.SZ']
        success_count = 0
        with StockDatabase() as db:
            db.create_index_dailybasic_table()
            for code in major_indexes:
                df = fetcher.get_index_dailybasic(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    if db.insert_index_dailybasic(df):
                        success_count += 1
        return success_count > 0
    except Exception as e:
        logger.error(f"❌ 同步指数每日指标失败: {e}")
        return False

def sync_ths_daily(days_back: int = 3) -> bool:
    """同步同花顺概念/行业指数日线行情"""
    logger.info(f"🔄 同步同花顺指数日线行情 (最近 {days_back} 天)...")
    try:
        fetcher = StockDataFetcher()
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        with StockDatabase() as db:
            db.create_ths_daily_table()
            # 获取已有的指数列表
            cursor = db.connection.cursor()
            cursor.execute("SELECT ts_code FROM ths_index WHERE type IN ('N', 'I')")
            ths_codes = [r[0] for r in cursor.fetchall()]
            
            if not ths_codes:
                return False
                
            success_count = 0
            for code in ths_codes:
                df = fetcher.get_ths_daily(ts_code=code, start_date=start_date, end_date=end_date)
                if df is not None and not df.empty:
                    if db.insert_ths_daily(df):
                        success_count += 1
                import time
                time.sleep(0.2)
        return success_count > 0
    except Exception as e:
        logger.error(f"❌ 同步同花顺指数日线行情失败: {e}")
        return False

def sync_financial_data(years_back: int = 1) -> bool:
    """同步财务数据 (利润表, 现金流量表, 分红)"""
    logger.info(f"🔄 同步财务数据 (最近 {years_back} 年)...")
    try:
        fetcher = StockDataFetcher()
        with StockDatabase() as db:
            # 获取主板股票列表
            cursor = db.connection.cursor()
            cursor.execute("SELECT ts_code FROM stock_basic WHERE list_status = 'L' AND (ts_code LIKE '60____.SH' OR ts_code LIKE '00____.SZ')")
            stock_codes = [r[0] for r in cursor.fetchall()]
            
            if not stock_codes:
                logger.warning("⚠️ 未能在数据库中找到主板股票，跳过财务同步")
                return False

            # 利润表
            income_df = fetcher.get_multiple_stocks_financial_data(stock_codes, data_type='income', years_back=years_back)
            if income_df is not None:
                db.insert_income_data(income_df)
                
            # 现金流量表
            cashflow_df = fetcher.get_multiple_stocks_financial_data(stock_codes, data_type='cashflow', years_back=years_back)
            if cashflow_df is not None:
                db.insert_cashflow_data(cashflow_df)
                
            # 分红
            dividend_df = fetcher.get_multiple_stocks_financial_data(stock_codes, data_type='dividend', years_back=years_back + 1)
            if dividend_df is not None:
                db.insert_dividend_data(dividend_df)
                
        return True
    except Exception as e:
        logger.error(f"❌ 同步财务数据失败: {e}")
        return False

def main():
    logger.info("🚀 开始每日同步任务...")
    start_time = datetime.now()
    
    results = {
        "Stock Basic": sync_stock_basic(),
        "Stock Daily": sync_stock_daily(),
        "Stock Weekly": sync_stock_weekly(),
        "Index Data": sync_index_data(),
        "ETF Data": sync_etf_data(),
        "THS Data": sync_ths_data(),
        # "Financial Data": sync_financial_data() # 财务数据量大，建议按需开启
    }
    
    logger.info("=" * 60)
    logger.info("📊 同步任务汇总：")
    for name, success in results.items():
        logger.info(f"   {name:<15}: {'✅ 成功' if success else '❌ 失败'}")
    
    duration = datetime.now() - start_time
    logger.info(f"⏱️ 总耗时: {duration}")
    logger.info("=" * 60)

if __name__ == "__main__":
    main()

