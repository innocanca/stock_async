#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念和行业指数行情初始化脚本
对应 Tushare 文档: https://tushare.pro/document/2?doc_id=327
"""

import sys
import os
from datetime import datetime, timedelta
import pandas as pd
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)

def main():
    logger.info("🚀 同花顺概念和行业指数行情初始化开始...")
    
    fetcher = StockDataFetcher()
    db = StockDatabase()
    
    if not db.connect():
        logger.error("❌ 无法连接数据库")
        return
        
    if not db.create_ths_daily_table():
        logger.error("❌ 无法创建 ths_daily 表")
        return

    # 从数据库获取已有的同花顺指数代码
    try:
        with db.connection.cursor() as cursor:
            cursor.execute("SELECT ts_code, name FROM ths_index WHERE type IN ('N', 'I')")
            rows = cursor.fetchall()
            ths_indexes = [{'ts_code': r[0], 'name': r[1]} for r in rows]
    except Exception as e:
        logger.error(f"❌ 获取指数列表失败: {e}")
        return

    if not ths_indexes:
        logger.warning("⚠️ 数据库中没有同花顺指数基础信息，请先运行 init_ths_index.py")
        return

    logger.info(f"📋 共发现 {len(ths_indexes)} 个概念/行业指数")

    # 默认初始化最近 1 年数据 (THS 数据量大，积分消耗多，先取 1 年)
    end_date = datetime.now().strftime('%Y%m%d')
    start_date = (datetime.now() - timedelta(days=365)).strftime('%Y%m%d')
    
    total_records = 0
    # 由于接口限制，分批获取
    for i, item in enumerate(ths_indexes, 1):
        ts_code = item['ts_code']
        name = item['name']
        logger.info(f"📊 [{i}/{len(ths_indexes)}] 正在获取 {name}({ts_code}) 的行情...")
        
        df = fetcher.get_ths_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            if db.insert_ths_daily(df):
                total_records += len(df)
                logger.info(f"✅ 成功插入 {len(df)} 条记录")
            else:
                logger.error(f"❌ 插入记录失败")
        else:
            logger.warning(f"⚠️ 未获取到行情数据")
            
        # 避免触发 API 限制
        time.sleep(0.5)
            
    logger.info(f"🎉 初始化完成，总计插入 {total_records} 条记录")

if __name__ == "__main__":
    main()

