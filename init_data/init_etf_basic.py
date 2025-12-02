#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF基础信息数据初始化脚本

功能：
1. 调用Tushare etf_basic 接口获取ETF基础信息
2. 创建 etf_basic 表
3. 将ETF基础信息初始化写入数据库

使用方法：
    python init_data/init_etf_basic.py

对应Tushare文档：
    https://tushare.pro/document/2?doc_id=385
"""

import sys
import os
from datetime import datetime

# 添加父目录到Python路径，以便导入 database 和 fetcher 模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger

logger = get_logger(__name__)


def create_database_tables(db: StockDatabase) -> bool:
    """
    创建必要的数据库表

    Args:
        db: 数据库实例

    Returns:
        bool: 创建是否成功
    """
    logger.info("🔧 开始创建ETF相关数据库表...")

    try:
        # 创建数据库（如果不存在）
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False

        # 连接数据库
        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False

        # 创建ETF基础信息表
        if not db.create_etf_basic_table():
            logger.error("❌ 创建ETF基础信息表失败")
            return False

        logger.info("✅ ETF相关数据库表创建成功")
        return True

    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def fetch_and_store_etf_basic(fetcher: StockDataFetcher, db: StockDatabase) -> dict:
    """
    获取并存储ETF基础信息数据

    Args:
        fetcher: 数据获取器实例
        db: 数据库实例

    Returns:
        dict: 统计信息
    """
    stats = {
        "total_etf": 0,
        "successful_insert": False,
        "exchange_distribution": {},
        "start_time": datetime.now(),
        "end_time": None,
        "duration": None,
    }

    logger.info("📊 开始获取ETF基础信息数据...")

    try:
        # 只取在市ETF
        df = fetcher.get_etf_basic(list_status="L")

        if df is None or df.empty:
            logger.error("❌ 未获取到任何ETF基础信息数据")
            return stats

        stats["total_etf"] = len(df)

        if "exchange" in df.columns:
            stats["exchange_distribution"] = df["exchange"].value_counts().to_dict()

        logger.info(f"📈 成功获取 {len(df)} 只ETF基础信息")

        # 插入数据库
        logger.info("💾 开始插入ETF基础信息到数据库...")

        if db.insert_etf_basic(df):
            stats["successful_insert"] = True
            logger.info("✅ ETF基础信息插入成功！")

            logger.info("📊 数据统计：")
            logger.info(f"   总ETF数量: {stats['total_etf']} 只")
            if stats["exchange_distribution"]:
                logger.info("   按交易所分布：")
                for exch, count in stats["exchange_distribution"].items():
                    exch_name = "上交所" if exch == "SSE" else ("深交所" if exch == "SZSE" else exch)
                    logger.info(f"     {exch_name}({exch}): {count} 只")
        else:
            logger.error("❌ ETF基础信息插入失败")

    except Exception as e:
        logger.error(f"❌ 获取和存储ETF基础信息时发生错误: {e}")

    finally:
        stats["end_time"] = datetime.now()
        stats["duration"] = stats["end_time"] - stats["start_time"]

    return stats


def query_and_display_data(db: StockDatabase) -> None:
    """
    简单查询和展示部分ETF基础信息，验证插入结果
    """
    import pandas as pd

    logger.info("🔍 验证数据库中的ETF基础信息...")

    try:
        if not db.connection:
            if not db.connect():
                logger.error("❌ 重新连接数据库失败，无法查询ETF数据")
                return

        query_sql = """
        SELECT ts_code, extname, index_code, index_name, exchange, etf_type,
               list_date, list_status, mgr_name, updated_at
        FROM etf_basic
        ORDER BY exchange, ts_code
        LIMIT 20
        """
        df = pd.read_sql(query_sql, db.connection)

        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有ETF基础信息数据")
            return

        logger.info(f"📋 数据库中示例ETF记录数: {len(df)} 条 (仅展示前20条)")
        for i, (_, row) in enumerate(df.iterrows(), 1):
            logger.info(
                f"   {i:2d}. {row.get('extname') or ''} ({row.get('ts_code')}) "
                f"- 交易所:{row.get('exchange')} - 指数:{row.get('index_name') or ''} "
                f"- 管理人:{row.get('mgr_name') or ''}"
            )

    except Exception as e:
        logger.error(f"❌ 查询ETF基础信息时发生错误: {e}")


def main() -> bool:
    """主函数"""
    logger.info("🚀 ETF基础信息数据初始化开始...")
    logger.info("=" * 60)

    start_time = datetime.now()

    try:
        # 初始化数据获取器
        logger.info("🔧 初始化数据获取器...")
        fetcher = StockDataFetcher()
        logger.info("✅ 数据获取器初始化成功")

        # 初始化数据库
        logger.info("🔧 初始化数据库连接...")
        with StockDatabase() as db:
            # 创建数据库表
            if not create_database_tables(db):
                logger.error("❌ 数据库表创建失败，退出程序")
                return False

            # 获取并存储ETF基础信息
            stats = fetch_and_store_etf_basic(fetcher, db)

            # 查询并显示部分数据（验证插入结果）
            if stats["successful_insert"]:
                query_and_display_data(db)

            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 ETF基础信息初始化统计：")
            logger.info(f"   📈 获取ETF总数: {stats['total_etf']} 只")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")

            if stats["successful_insert"]:
                logger.info("🎉 ETF基础信息数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 数据表名: etf_basic")
                logger.info("   - 字段: ts_code, extname, index_code, index_name, exchange, etf_type, list_date, list_status, delist_date, mgr_name")
                return True
            else:
                logger.error("❌ ETF基础信息数据初始化失败")
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



