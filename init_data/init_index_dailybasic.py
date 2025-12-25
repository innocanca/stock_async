#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
大盘指数每日指标初始化脚本
对应 Tushare 文档: https://tushare.pro/document/2?doc_id=128
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)

def main():
    logger.info("🚀 大盘指数每日指标初始化开始...")
    
    fetcher = StockDataFetcher()
    db = StockDatabase()
    
    if not db.connect():
        logger.error("❌ 无法连接数据库")
        return
        
    if not db.create_index_dailybasic_table():
        logger.error("❌ 无法创建 index_dailybasic 表")
        return

    # 主要指数列表
    major_indexes = [
        '000001.SH',  # 上证综指
        '000300.SH',  # 沪深300
        '000905.SH',  # 中证500
        '000016.SH',  # 上证50
        '399001.SZ',  # 深证成指
        '399006.SZ',  # 创业板指
    ]
    
    # 默认初始化最近 3 年数据
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365 * 3)).strftime('%Y%m%d')
    
    total_records = 0
    for ts_code in major_indexes:
        logger.info(f"📊 正在获取 {ts_code} 的每日指标...")
        df = fetcher.get_index_dailybasic(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            if db.insert_index_dailybasic(df):
                total_records += len(df)
                logger.info(f"✅ 成功插入 {ts_code} 的 {len(df)} 条记录")
            else:
                logger.error(f"❌ 插入 {ts_code} 的记录失败")
        else:
            logger.warning(f"⚠️ 未获取到 {ts_code} 的指标数据")
            
    logger.info(f"🎉 初始化完成，总计插入 {total_records} 条记录")

if __name__ == "__main__":
    main()

