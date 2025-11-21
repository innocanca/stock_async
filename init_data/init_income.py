#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
利润表数据初始化脚本

功能：
1. 获取所有主板股票的利润表数据
2. 创建数据库表结构
3. 将最近3年的财务数据初始化到数据库中
4. 提供数据查询和统计功能

使用方法：
python init_income.py
"""

import logging
import sys
import os
from datetime import datetime, timedelta
from typing import List

# 添加父目录到Python路径，以便导入database和fetcher模块
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
            
        # 创建利润表数据表
        if not db.create_income_table():
            logger.error("❌ 创建利润表数据表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def get_stock_list(db: StockDatabase) -> List[str]:
    """
    从数据库获取股票列表（全量）
    
    Args:
        db: 数据库实例
        
    Returns:
        List[str]: 股票代码列表
    """
    logger.info("📋 从数据库获取股票列表（全量）...")
    
    try:
        # 从数据库的stock_basic表获取主板股票列表
        cursor = db.connection.cursor()
        
        # 查询主板股票，排除ST、退市等 - 全量获取
        query_sql = """
        SELECT DISTINCT ts_code 
        FROM stock_basic 
        WHERE list_status = 'L'
          AND name NOT LIKE '%ST%'
          AND name NOT LIKE '%退%'
          AND name NOT LIKE '%暂停%'
          AND (ts_code LIKE '60____.SH' OR ts_code LIKE '00____.SZ')
        ORDER BY ts_code
        """
        
        cursor.execute(query_sql)
        results = cursor.fetchall()
        
        if not results:
            logger.warning("⚠️ 数据库中未找到股票基础信息，使用备用列表")
            # 使用测试通过的知名股票
            return [
                '000001.SZ', '000002.SZ', '000063.SZ', '000333.SZ', '000858.SZ',
                '600000.SH', '600036.SH', '600519.SH', '600887.SH', '601318.SH',
                '601398.SH', '601939.SH'
            ]
        
        stock_codes = [result[0] for result in results]
        logger.info(f"📈 从数据库获取到 {len(stock_codes)} 只主板股票（全量）")
        
        return stock_codes
        
    except Exception as e:
        logger.error(f"❌ 从数据库获取股票列表失败: {e}")
        # 使用备用的知名股票列表
        logger.info("使用备用股票列表")
        return [
            '000001.SZ', '000002.SZ', '000063.SZ', '000333.SZ', '000858.SZ',
            '600000.SH', '600036.SH', '600519.SH', '600887.SH', '601318.SH',
            '601398.SH', '601939.SH'
        ]


def calculate_date_range(years_back: int = 3) -> tuple:
    """
    计算财务数据的日期范围
    
    Args:
        years_back: 回溯年数
        
    Returns:
        tuple: (start_date, end_date) 格式为 YYYYMMDD
    """
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * years_back)
    
    start_date_str = start_date.strftime('%Y%m%d')
    end_date_str = end_date.strftime('%Y%m%d')
    
    logger.info(f"📅 数据获取时间范围: {start_date_str} 至 {end_date_str} (最近{years_back}年)")
    return start_date_str, end_date_str


def fetch_and_store_income_data(fetcher: StockDataFetcher, db: StockDatabase, 
                               stock_codes: List[str], start_date: str, 
                               end_date: str) -> dict:
    """
    获取并存储利润表数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        stock_codes: 股票代码列表
        start_date: 开始日期
        end_date: 结束日期
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_stocks': len(stock_codes),
        'successful_stocks': 0,
        'total_records': 0,
        'successful_insert': False,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    logger.info("📊 开始获取利润表数据...")
    logger.info(f"   股票数量: {len(stock_codes)} 只")
    logger.info(f"   时间范围: {start_date} 至 {end_date}")
    
    try:
        # 批量获取利润表数据
        df = fetcher.get_multiple_stocks_financial_data(
            stock_codes=stock_codes,
            data_type='income',
            years_back=3,
            batch_size=20,
            delay=0.5  # 财务数据API调用间隔稍长
        )
        
        if df is None or df.empty:
            logger.error("❌ 未获取到任何利润表数据")
            return stats
        
        stats['total_records'] = len(df)
        stats['successful_stocks'] = df['ts_code'].nunique()
        
        logger.info(f"📈 成功获取 {stats['successful_stocks']} 只股票的 {stats['total_records']} 条利润表数据")
        
        # 插入数据库
        logger.info("💾 开始插入数据到数据库...")
        
        if db.insert_income_data(df):
            stats['successful_insert'] = True
            logger.info("✅ 数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   成功股票数量: {stats['successful_stocks']} / {stats['total_stocks']} 只")
            logger.info(f"   总记录数: {stats['total_records']} 条")
            logger.info(f"   成功率: {stats['successful_stocks']/stats['total_stocks']*100:.1f}%")
            
            # 分析报告期分布
            if 'end_date' in df.columns:
                period_counts = df['end_date'].dt.year.value_counts().sort_index()
                logger.info("   报告期分布:")
                for year, count in period_counts.items():
                    logger.info(f"     {year}年: {count} 条")
        else:
            logger.error("❌ 数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
        
        # 检查是否是权限问题
        if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
            logger.error("💡 提示：财务数据接口需要相应积分权限")
            logger.error("   请检查您的Tushare账户积分或升级账户权限")
            logger.error("   访问 https://tushare.pro/ 查看积分和权限说明")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的利润表数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的利润表数据...")
    
    try:
        # 简单查询最新数据
        with db:
            cursor = db.connection.cursor()
            cursor.execute("""
                SELECT ts_code, end_date, basic_eps, diluted_eps, total_revenue, 
                       n_income, n_income_attr_p, operate_profit 
                FROM income_data 
                ORDER BY end_date DESC, ts_code 
                LIMIT 10
            """)
            
            results = cursor.fetchall()
            
            if not results:
                logger.warning("⚠️ 数据库中没有利润表数据")
                return
            
            logger.info(f"📋 数据库中利润表数据示例（前10条）：")
            logger.info("=" * 120)
            logger.info(f"{'股票代码':<12} {'报告期':<12} {'基本EPS':<10} {'营业收入':<15} {'净利润':<15} {'营业利润':<15}")
            logger.info("=" * 120)
            
            for result in results:
                ts_code, end_date, basic_eps, diluted_eps, total_revenue, n_income, n_income_attr_p, operate_profit = result
                end_date_str = end_date.strftime('%Y-%m-%d') if end_date else 'N/A'
                
                logger.info(f"{ts_code:<12} {end_date_str:<12} "
                           f"{basic_eps or 'N/A':<10} "
                           f"{(total_revenue/100000000 if total_revenue else 0):<15.2f} "
                           f"{(n_income_attr_p/100000000 if n_income_attr_p else 0):<15.2f} "
                           f"{(operate_profit/100000000 if operate_profit else 0):<15.2f}")
            
            # 统计信息
            cursor.execute("SELECT COUNT(DISTINCT ts_code) FROM income_data")
            stock_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(*) FROM income_data")
            record_count = cursor.fetchone()[0]
            
            logger.info("=" * 120)
            logger.info(f"📊 总计: {stock_count} 只股票, {record_count} 条利润表记录")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    logger.info("🚀 利润表数据初始化开始...")
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
            
            # 获取股票列表
            stock_codes = get_stock_list(db)
            if not stock_codes:
                logger.error("❌ 获取股票列表失败，退出程序")
                return False
            
            # 计算日期范围
            start_date, end_date = calculate_date_range(years_back=3)
            
            # 获取并存储利润表数据
            stats = fetch_and_store_income_data(fetcher, db, stock_codes, start_date, end_date)
            
            # 查询并显示数据（验证插入结果）
            if stats['successful_insert']:
                query_and_display_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 60)
            logger.info("📊 初始化完成统计：")
            logger.info(f"   📈 目标股票数量: {stats['total_stocks']} 只")
            logger.info(f"   ✅ 成功股票数量: {stats['successful_stocks']} 只")
            logger.info(f"   📊 总记录数: {stats['total_records']} 条")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")
            
            if stats['successful_insert']:
                logger.info("🎉 利润表数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 数据表名: income_data")
                logger.info("   - 包含基本每股收益、营业收入、净利润等关键指标")
                logger.info("   - 支持按股票代码、报告期等条件查询")
                logger.info("   - 建议定期更新最新的财务数据")
                return True
            else:
                logger.error("❌ 利润表数据初始化失败")
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
