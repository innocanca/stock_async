#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数数据初始化脚本（整合版）

功能：
1. 获取Tushare指数基本信息数据(index_basic接口 - doc_id=94)
2. 获取Tushare指数日线行情数据(index_daily接口 - doc_id=96)
3. 创建相关数据库表结构
4. 将数据初始化到数据库中
5. 提供数据查询和统计功能

使用方法：
python init_index_data.py

对应Tushare文档：
- 指数基本信息: https://tushare.pro/document/2?doc_id=94
- 指数日线行情: https://tushare.pro/document/2?doc_id=96
"""

import logging
import sys
import os
from datetime import datetime, timedelta
import argparse

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
            
        # 创建指数日线行情表
        if not db.create_index_daily_table():
            logger.error("❌ 创建指数日线行情表失败")
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
            logger.warning("⚠️ 未获取到任何指数基本信息数据")
            return stats
        
        stats['total_indexes'] = len(df)
        
        # 统计各市场数量
        if 'market' in df.columns:
            stats['market_distribution'] = df['market'].value_counts().to_dict()
        
        logger.info(f"📈 成功获取 {len(df)} 个指数的基本信息")
        
        # 插入数据库
        logger.info("💾 开始插入指数基本信息到数据库...")
        
        if db.insert_index_basic(df):
            stats['successful_insert'] = True
            logger.info("✅ 指数基本信息数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 指数基本信息统计：")
            logger.info(f"   总指数数量: {stats['total_indexes']} 个")
            
            if stats['market_distribution']:
                logger.info("   市场分布：")
                for market, count in stats['market_distribution'].items():
                    market_name = _get_market_name(market)
                    logger.info(f"     {market_name}({market}): {count} 个")
        else:
            logger.error("❌ 指数基本信息数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储指数基本信息时发生错误: {e}")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def fetch_and_store_index_daily_data(fetcher: StockDataFetcher, db: StockDatabase, 
                                    start_date: str = None, end_date: str = None) -> dict:
    """
    获取并存储“全部指数”的日线行情数据（按交易日全市场抓取）
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        start_date: 开始日期 (YYYYMMDD格式)
        end_date: 结束日期 (YYYYMMDD格式)
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_records': 0,
        'total_indexes': 0,
        'successful_insert': False,
        'date_range': {},
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    # 默认获取最近3个月的数据
    if not start_date or not end_date:
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=90)
        start_date = start_dt.strftime('%Y%m%d')
        end_date = end_dt.strftime('%Y%m%d')
    
    logger.info(f"📊 开始获取【全部指数】日线行情数据 ({start_date} 到 {end_date})...")
    
    try:
        # 按交易日循环获取所有指数日线并分批插库
        index_stats = fetcher.get_all_index_daily_by_dates_with_batch_insert(
            start_date=start_date,
            end_date=end_date,
            delay=0.5,
            exchange='SSE',
            db_instance=db,
            batch_days=10,
        )
        
        if not index_stats:
            logger.warning("⚠️ 未获取到任何指数日线行情数据")
            return stats
        
        stats['total_records'] = index_stats.get('total_records', 0)
        stats['successful_insert'] = stats['total_records'] > 0
        # total_indexes 无法直接从批量统计拿到，这里留空或后续按需查询
        
    except Exception as e:
        logger.error(f"❌ 获取和存储指数日线行情时发生错误: {e}")
    
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


def _get_index_name(ts_code: str) -> str:
    """
    获取指数中文名称
    
    Args:
        ts_code: 指数代码
        
    Returns:
        str: 中文名称
    """
    index_mapping = {
        '000001.SH': '上证综指',
        '000300.SH': '沪深300',
        '000905.SH': '中证500',
        '000016.SH': '上证50',
        '399001.SZ': '深证成指',
        '399006.SZ': '创业板指',
        '399303.SZ': '国证2000',
        '000852.SH': '中证1000',
        '000688.SH': '科创50',
    }
    return index_mapping.get(ts_code, '未知指数')


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的指数数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的指数数据...")
    
    try:
        # 查询指数基本信息
        basic_df = db.query_index_basic(limit=5)
        if basic_df is not None and not basic_df.empty:
            logger.info(f"📋 指数基本信息: 共 {len(basic_df)} 条记录")
            logger.info("📖 前5条指数基本信息示例：")
            for i, (_, row) in enumerate(basic_df.head(5).iterrows(), 1):
                logger.info(f"   {i}. {row.get('name', 'N/A')} ({row.get('ts_code', 'N/A')}) "
                           f"- 市场:{row.get('market', 'N/A')}")
        
        # 查询指数日线行情
        daily_df = db.query_index_daily(limit=5)
        if daily_df is not None and not daily_df.empty:
            logger.info(f"\n📋 指数日线行情: 共 {len(daily_df)} 条记录")
            logger.info("📖 最新5条日线行情示例：")
            for i, (_, row) in enumerate(daily_df.head(5).iterrows(), 1):
                index_name = _get_index_name(row.get('ts_code', 'N/A'))
                logger.info(f"   {i}. {index_name}({row.get('ts_code', 'N/A')}) "
                           f"{row.get('trade_date', 'N/A')} - 收盘:{row.get('close', 'N/A')}")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='指数数据初始化脚本')
    parser.add_argument('--basic-only', action='store_true', help='只初始化指数基本信息')
    parser.add_argument('--daily-only', action='store_true', help='只初始化指数日线行情')
    parser.add_argument('--start-date', type=str, help='开始日期 (YYYYMMDD格式)')
    parser.add_argument('--end-date', type=str, help='结束日期 (YYYYMMDD格式)')
    
    return parser.parse_args()


def main():
    """主函数"""
    logger.info("🚀 指数数据初始化开始...")
    logger.info("=" * 60)
    
    # 解析命令行参数
    args = parse_arguments()
    
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
            
            # 统计信息
            overall_stats = {
                'basic_success': False,
                'daily_success': False,
                'basic_stats': None,
                'daily_stats': None
            }
            
            # 获取并存储指数基本信息数据
            if not args.daily_only:
                basic_stats = fetch_and_store_index_basic_data(fetcher, db)
                overall_stats['basic_stats'] = basic_stats
                overall_stats['basic_success'] = basic_stats['successful_insert']
            
            # 获取并存储指数日线行情数据
            if not args.basic_only:
                daily_stats = fetch_and_store_index_daily_data(
                    fetcher, db, args.start_date, args.end_date
                )
                overall_stats['daily_stats'] = daily_stats
                overall_stats['daily_success'] = daily_stats['successful_insert']
            
            # 查询并显示数据（验证插入结果）
            if overall_stats['basic_success'] or overall_stats['daily_success']:
                query_and_display_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成总体统计：")
            
            if overall_stats['basic_stats']:
                logger.info(f"   📈 指数基本信息: {overall_stats['basic_stats']['total_indexes']} 个指数")
                logger.info(f"   💾 基本信息插入: {'成功' if overall_stats['basic_success'] else '失败'}")
            
            if overall_stats['daily_stats']:
                logger.info(f"   📊 指数日线行情: {overall_stats['daily_stats']['total_records']} 条记录")
                logger.info(f"   💾 行情数据插入: {'成功' if overall_stats['daily_success'] else '失败'}")
            
            success = overall_stats['basic_success'] or overall_stats['daily_success']
            
            if success:
                logger.info("🎉 指数数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_index_basic() 查询指数基本信息")
                logger.info("   - 可以使用 database.py 中的 query_index_daily() 查询指数日线行情")
                logger.info("   - 数据表名: index_basic, index_daily")
                logger.info("   - 支持多种筛选条件和统计分析")
                return True
            else:
                logger.error("❌ 指数数据初始化失败")
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
