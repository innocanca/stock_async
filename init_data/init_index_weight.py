#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数成分和权重数据初始化脚本

功能：
1. 调用 Tushare index_weight 接口，获取指数成分及其权重
2. 创建 index_weight 表
3. 将数据写入数据库，支持按指数代码或按日期区间初始化

使用方法：
    # 默认：最近1年的所有指数权重（按 trade_date 区间抓取）
    python init_data/init_index_weight.py

    # 指定时间范围
    python init_data/init_index_weight.py --start-date 20220101 --end-date 20251231

    # 仅初始化某个指数的权重历史
    python init_data/init_index_weight.py --index-code 000300.SH --start-date 20220101 --end-date 20251231

对应Tushare文档：
    - 指数基本信息: https://tushare.pro/document/2?doc_id=94
    - 指数成分和权重: https://tushare.pro/document/2?doc_id=171
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
    """创建 index_weight 所需表结构"""
    logger.info("🔧 开始创建指数成分和权重相关数据库表...")

    try:
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False

        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False

        if not db.create_index_weight_table():
            logger.error("❌ 创建 index_weight 表失败")
            return False

        logger.info("✅ 指数成分和权重相关表创建成功")
        return True

    except Exception as e:
        logger.error(f"❌ 创建指数成分和权重表时发生错误: {e}")
        return False


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="指数成分和权重初始化脚本")
    parser.add_argument("--index-code", type=str, help="指数代码，如 000300.SH；不填则抓取所有指数")
    parser.add_argument("--start-date", type=str, help="开始日期 (YYYYMMDD)")
    parser.add_argument("--end-date", type=str, help="结束日期 (YYYYMMDD)")
    parser.add_argument(
        "--years-back",
        type=int,
        default=1,
        help="默认回溯年数（在未指定 start/end 时使用，默认1年）",
    )
    return parser.parse_args()


def fetch_and_store_index_weight_data(
    fetcher: StockDataFetcher,
    db: StockDatabase,
    index_code: str = None,
    start_date: str = None,
    end_date: str = None,
) -> dict:
    """
    获取并存储指数成分权重数据
    """
    stats = {
        "total_records": 0,
        "successful_insert": False,
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None,
    }

    logger.info("📊 开始获取指数成分和权重数据...")

    try:
        df = fetcher.get_index_weight(
            index_code=index_code,
            start_date=start_date,
            end_date=end_date,
        )

        if df is None or df.empty:
            logger.warning("⚠️ 未获取到任何指数成分和权重数据")
            return stats

        stats["total_records"] = len(df)
        logger.info(f"📈 成功获取 {stats['total_records']} 条指数成分和权重数据")

        logger.info("💾 开始插入指数成分和权重数据到数据库...")
        if db.insert_index_weight(df):
            stats["successful_insert"] = True
            logger.info("✅ 指数成分和权重数据插入成功！")
        else:
            logger.error("❌ 指数成分和权重数据插入失败")

    except Exception as e:
        logger.error(f"❌ 获取和存储指数成分和权重数据时发生错误: {e}")

    finally:
        stats["end_time"] = datetime.now()
        stats["duration"] = stats["end_time"] - stats["start_time"]

    return stats


def main() -> bool:
    logger.info("🚀 指数成分和权重数据初始化开始...")
    logger.info("=" * 60)

    args = parse_arguments()
    start_time = datetime.now()

    # 处理日期参数
    if args.start_date and args.end_date:
        start_date = args.start_date
        end_date = args.end_date
    else:
        # 默认按 years_back 回溯
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=365 * args.years_back)
        start_date = start_dt.strftime("%Y%m%d")
        end_date = end_dt.strftime("%Y%m%d")

    logger.info(
        f"📊 指数成分权重初始化区间: {start_date} ~ {end_date}, "
        f"指数: {args.index_code or '全部可用指数'}"
    )

    try:
        # 初始化数据获取器
        logger.info("🔧 初始化数据获取器...")
        fetcher = StockDataFetcher()
        logger.info("✅ 数据获取器初始化成功")

        # 初始化数据库
        logger.info("🔧 初始化数据库连接...")
        with StockDatabase() as db:
            if not create_database_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False

            stats = fetch_and_store_index_weight_data(
                fetcher,
                db,
                index_code=args.index_code,
                start_date=start_date,
                end_date=end_date,
            )

            logger.info("\n" + "=" * 60)
            logger.info("📊 指数成分权重初始化统计：")
            logger.info(f"   📊 记录数: {stats.get('total_records', 0):,} 条")
            logger.info(
                f"   💾 数据插入状态: {'成功' if stats.get('successful_insert') else '失败'}"
            )
            logger.info(f"   ⏱️  耗时: {stats.get('duration')}")

            if stats.get("successful_insert"):
                logger.info("🎉 指数成分和权重数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 表名: index_weight")
                logger.info(
                    "   - 主要字段: index_code, trade_date, con_code, con_name, weight, i_weight, is_new"
                )
                logger.info("   - 可结合 index_basic / index_daily 做指数成分分析")
                return True
            else:
                logger.error("❌ 指数成分和权重数据初始化失败")
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









