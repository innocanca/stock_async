#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF日线行情数据初始化脚本

功能：
1. 使用 Tushare fund_daily 接口获取ETF日线行情数据
2. 创建 etf_daily 表
3. 按交易日循环批量写入数据库，支持较长时间区间

使用方法：
    python init_data/init_etf_daily.py
    或带参数：
    python init_data/init_etf_daily.py --start-date 20180101 --end-date 20251231

对应Tushare文档：
    ETF日线行情 fund_daily: https://tushare.pro/document/2?doc_id=127
"""

import sys
import os
from datetime import datetime, timedelta
import argparse

# 添加父目录到Python路径，以便导入 database 和 fetcher 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


def create_database_tables(db: StockDatabase) -> bool:
    """
    创建ETF日线相关数据库表
    """
    logger.info("🔧 开始创建ETF日线相关数据库表...")

    try:
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False

        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False

        if not db.create_etf_daily_table():
            logger.error("❌ 创建ETF日线行情表失败")
            return False

        logger.info("✅ ETF日线相关数据库表创建成功")
        return True

    except Exception as e:
        logger.error(f"❌ 创建ETF日线数据库表时发生错误: {e}")
        return False


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="ETF日线行情初始化脚本")
    parser.add_argument("--start-date", type=str, help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYYMMDD)")
    parser.add_argument(
        "--days-back",
        type=int,
        default=None,
        help="向前回溯的天数（与 start/end-date 互斥，优先使用 start/end-date）",
    )
    parser.add_argument(
        "--batch-days",
        type=int,
        default=10,
        help="每批插入的交易日数量，默认10天一批",
    )
    return parser.parse_args()


def main() -> bool:
    logger.info("🚀 ETF日线行情数据初始化开始...")
    logger.info("=" * 60)

    args = parse_arguments()
    start_time = datetime.now()

    # 处理日期参数
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        # 如果未指定，默认回溯2年
        days_back = args.days_back if args.days_back is not None else 365 * 2
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days_back)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    logger.info(f"📊 ETF日线初始化区间: {start_date} ~ {end_date}")

    try:
        # 初始化数据获取器
        logger.info("🔧 初始化数据获取器...")
        fetcher = StockDataFetcher()
        logger.info("✅ 数据获取器初始化成功")

        # 初始化数据库
        logger.info("🔧 初始化数据库连接...")
        with StockDatabase() as db:
            # 创建表
            if not create_database_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False

            # 按交易日循环获取并分批插入
            stats = fetcher.get_all_etf_daily_by_dates_with_batch_insert(
                start_date=start_date,
                end_date=end_date,
                delay=0.5,
                exchange="SSE",
                db_instance=db,
                batch_days=args.batch_days,
            )

            if not stats:
                logger.error("❌ ETF日线数据获取/插入过程返回空统计，可能执行失败")
                return False

            logger.info("\n" + "=" * 60)
            logger.info("📊 ETF日线初始化统计：")
            logger.info(f"   📅 总交易日: {stats.get('total_trading_days', 0)} 天")
            logger.info(f"   ✅ 成功获取: {stats.get('successful_days', 0)} 天")
            logger.info(f"   📊 总插入记录: {stats.get('total_records', 0):,} 条")
            logger.info(f"   📦 插入批次: {stats.get('total_batches', 0)} 次")
            logger.info(
                f"   💾 插入成功批次: {stats.get('batch_insert_success', 0)}/"
                f"{stats.get('total_batches', 0)}"
            )

            if stats.get("total_records", 0) > 0:
                logger.info("🎉 ETF日线行情数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 表名: etf_daily")
                logger.info("   - 字段: ts_code, trade_date, open, high, low, close, pre_close, change_amount, change_pct, vol, amount")
                return True
            else:
                logger.error("❌ ETF日线行情数据初始化未插入任何记录")
                return False

    except KeyboardInterrupt:
        logger.warning("⚠️ 用户中断程序执行")
        return False
    except Exception as e:
        logger.error(f"❌ 程序执行出现异常: {e}")
        return False
    finally:
        end_time = datetime.now()
        total_duration = end_time - start_time
        logger.info(f"\n⏰ 程序总执行时间: {total_duration}")


if __name__ == "__main__":
    try:
        ok = main()
        sys.exit(0 if ok else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)


