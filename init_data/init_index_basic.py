#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数基本信息数据初始化脚本

功能：
1. 获取Tushare指数基本信息数据(index_basic接口)
2. 创建指数基本信息数据库表结构
3. 将数据初始化到数据库中
4. 提供数据查询和统计功能

使用方法：
python init_index_basic.py

对应Tushare文档：
https://tushare.pro/document/2?doc_id=94
"""

import logging
import sys
import os
from datetime import datetime

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher

# 配置日志
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
    logger.info("🔧 开始创建数据库表...")
    
    try:
        # 创建数据库（如果不存在）
        if not db.create_database():
            logger.error("❌ 创建数据库失败")
            return False
        
        # 连接数据库
        if not db.connect():
            logger.error("❌ 连接数据库失败")
            return False
            
        # 创建指数基本信息表
        if not db.create_index_basic_table():
            logger.error("❌ 创建指数基本信息表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def fetch_and_store_index_basic_data(fetcher: StockDataFetcher, db: StockDatabase) -> dict:
    """
    获取并存储指数基本信息数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_indexes': 0,
        'successful_insert': False,
        'market_distribution': {},
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    logger.info("📊 开始获取指数基本信息数据...")
    
    try:
        # 获取所有指数基本信息数据
        df = fetcher.get_all_index_basic_data()
        
        if df is None or df.empty:
            logger.error("❌ 未获取到任何指数基本信息数据")
            return stats
        
        stats['total_indexes'] = len(df)
        
        # 统计各市场数量
        if 'market' in df.columns:
            stats['market_distribution'] = df['market'].value_counts().to_dict()
        
        logger.info(f"📈 成功获取 {len(df)} 个指数的基本信息")
        
        # 插入数据库
        logger.info("💾 开始插入数据到数据库...")
        
        if db.insert_index_basic(df):
            stats['successful_insert'] = True
            logger.info("✅ 数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   总指数数量: {stats['total_indexes']} 个")
            
            if stats['market_distribution']:
                logger.info("   市场分布：")
                for market, count in stats['market_distribution'].items():
                    market_name = _get_market_name(market)
                    logger.info(f"     {market_name}({market}): {count} 个")
        else:
            logger.error("❌ 数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def _get_market_name(market: str) -> str:
    """
    获取市场中文名称
    
    Args:
        market: 市场代码
        
    Returns:
        str: 中文名称
    """
    market_mapping = {
        'SSE': '上交所指数',
        'SZSE': '深交所指数',
        'MSCI': 'MSCI指数',
        'CSI': '中证指数',
        'CICC': '中金指数',
        'SW': '申万指数',
        'OTH': '其他指数'
    }
    return market_mapping.get(market, '未知市场')


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的指数基本信息数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的指数基本信息数据...")
    
    try:
        # 查询所有数据
        df = db.query_index_basic(limit=10)
        
        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有指数基本信息数据")
            return
        
        logger.info(f"📋 数据库中共有 {len(df)} 条指数基本信息记录")
        logger.info("📖 前10条记录示例：")
        
        for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
            logger.info(f"   {i:2d}. {row.get('name', 'N/A')} ({row.get('ts_code', 'N/A')}) "
                       f"- 市场:{row.get('market', 'N/A')} - 发布方:{row.get('publisher', 'N/A')}")
        
        # 按市场统计
        logger.info("\n📈 按市场统计：")
        market_counts = df['market'].value_counts() if 'market' in df.columns else {}
        for market, count in market_counts.items():
            market_name = _get_market_name(market)
            logger.info(f"   {market_name}({market}): {count} 个")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    logger.info("🚀 指数基本信息数据初始化开始...")
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
            
            # 获取并存储数据
            stats = fetch_and_store_index_basic_data(fetcher, db)
            
            # 查询并显示数据（验证插入结果）
            if stats['successful_insert']:
                query_and_display_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成统计：")
            logger.info(f"   📈 获取指数总数: {stats['total_indexes']} 个")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")
            
            if stats['successful_insert']:
                logger.info("🎉 指数基本信息数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_index_basic() 方法查询数据")
                logger.info("   - 支持按市场类型、发布商等条件筛选")
                logger.info("   - 数据表名: index_basic")
                return True
            else:
                logger.error("❌ 指数基本信息数据初始化失败")
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
