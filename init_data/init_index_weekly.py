#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数周线行情数据初始化脚本

功能：
1. 获取Tushare指数周线行情数据(index_weekly接口)
2. 创建指数周线行情数据库表结构
3. 将数据初始化到数据库中

使用方法：
    # 默认：最近1年的主要指数周线行情
    python init_data/init_index_weekly.py

    # 指定时间范围
    python init_data/init_index_weekly.py --start-date 20220101 --end-date 20251231

对应Tushare文档：
    - 指数周线行情: https://tushare.pro/document/2?doc_id=171
"""

import sys
import os
from datetime import datetime, timedelta
import argparse

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


def create_database_tables(db: StockDatabase) -> bool:
    """
    创建必要的数据库表
    """
    logger.info("🔧 开始创建指数周线数据库表...")

    try:
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False

        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False

        if not db.create_index_weekly_table():
            logger.error("❌ 创建指数周线行情表失败")
            return False

        logger.info("✅ 指数周线行情表创建成功")
        return True

    except Exception as e:
        logger.error(f"❌ 创建指数周线数据库表时发生错误: {e}")
        return False


def fetch_and_store_index_weekly_data(
    fetcher: StockDataFetcher,
    db: StockDatabase,
    start_date: str,
    end_date: str,
) -> dict:
    """
    获取并存储“全部指数”的周线行情数据（按周线日期全市场抓取）
    """
    stats = {
        "total_records": 0,
        "total_indexes": 0,
        "successful_insert": False,
        "date_range": {},
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None,
    }

    logger.info(f"📊 开始获取【全部指数】周线行情数据 ({start_date} 到 {end_date})...")

    try:
        week_stats = fetcher.get_all_index_weekly_by_dates_with_batch_insert(
            start_date=start_date,
            end_date=end_date,
            delay=0.5,
            exchange="SSE",
            db_instance=db,
            batch_weeks=10,
        )

        if not week_stats:
            logger.warning("⚠️ 未获取到任何指数周线行情数据")
            return stats

        stats["total_records"] = week_stats.get("total_records", 0)
        stats["successful_insert"] = stats["total_records"] > 0

    except Exception as e:
        logger.error(f"❌ 获取和存储指数周线行情时发生错误: {e}")

    finally:
        stats["end_time"] = datetime.now()
        stats["duration"] = stats["end_time"] - stats["start_time"]

    return stats


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="指数周线行情初始化脚本")
    parser.add_argument("--start-date", type=str, help="开始日期 (YYYYMMDD格式)")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYYMMDD格式)")
    parser.add_argument(
        "--years-back",
        type=int,
        default=1,
        help="未指定起止日期时的默认回溯年数，默认1年",
    )
    return parser.parse_args()


def main() -> bool:
    logger.info("🚀 指数周线行情数据初始化开始...")
    logger.info("=" * 60)

    args = parse_arguments()
    start_time = datetime.now()

    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=365 * args.years_back)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    logger.info(f"📊 指数周线初始化区间: {start_date} ~ {end_date}")

    try:
        logger.info("🔧 初始化数据获取器...")
        fetcher = StockDataFetcher()
        logger.info("✅ 数据获取器初始化成功")

        logger.info("🔧 初始化数据库连接...")
        with StockDatabase() as db:
            if not create_database_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False

            stats = fetch_and_store_index_weekly_data(fetcher, db, start_date, end_date)

            logger.info("\n" + "=" * 60)
            logger.info("📊 指数周线初始化统计：")
            logger.info(f"   📈 记录总数: {stats.get('total_records', 0)} 条")
            logger.info(f"   📊 涉及指数: {stats.get('total_indexes', 0)} 个")
            logger.info(
                f"   💾 数据插入状态: {'成功' if stats.get('successful_insert') else '失败'}"
            )
            logger.info(f"   ⏱️  总耗时: {stats.get('duration')}")

            if stats.get("successful_insert"):
                logger.info("🎉 指数周线行情数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 表名: index_weekly")
                logger.info("   - 查询可参考: index_daily 的查询方式，自行写 SQL 或封装接口")
                return True
            else:
                logger.error("❌ 指数周线行情数据初始化失败")
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
        success = main()
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"程序异常退出: {e}")
        sys.exit(1)


