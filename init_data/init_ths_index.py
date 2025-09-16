#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念和行业指数数据初始化脚本

功能：
1. 获取所有同花顺概念和行业指数数据
2. 创建数据库表结构
3. 将数据初始化到数据库中
4. 提供数据查询和统计功能

使用方法：
python init_ths_index.py
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
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('init_ths_index.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


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
            
        # 创建同花顺概念指数表
        if not db.create_ths_index_table():
            logger.error("❌ 创建同花顺概念指数表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def fetch_and_store_ths_index_data(fetcher: StockDataFetcher, db: StockDatabase) -> dict:
    """
    获取并存储同花顺概念指数数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'total_indexes': 0,
        'successful_insert': False,
        'type_distribution': {},
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    logger.info("📊 开始获取同花顺概念和行业指数数据...")
    
    try:
        # 获取所有同花顺指数数据
        df = fetcher.get_all_ths_index_data()
        
        if df is None or df.empty:
            logger.error("❌ 未获取到任何同花顺指数数据")
            return stats
        
        stats['total_indexes'] = len(df)
        
        # 统计各类型数量
        if 'type' in df.columns:
            stats['type_distribution'] = df['type'].value_counts().to_dict()
        
        logger.info(f"📈 成功获取 {len(df)} 个同花顺指数")
        
        # 插入数据库
        logger.info("💾 开始插入数据到数据库...")
        
        if db.insert_ths_index(df):
            stats['successful_insert'] = True
            logger.info("✅ 数据插入成功！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   总指数数量: {stats['total_indexes']} 个")
            
            if stats['type_distribution']:
                logger.info("   指数类型分布：")
                for idx_type, count in stats['type_distribution'].items():
                    type_name = fetcher._get_index_type_name(idx_type)
                    logger.info(f"     {type_name}({idx_type}): {count} 个")
        else:
            logger.error("❌ 数据插入失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
        
        # 检查是否是权限问题
        if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
            logger.error("💡 提示：同花顺概念指数接口需要5000积分权限")
            logger.error("   请检查您的Tushare账户积分或升级账户权限")
            logger.error("   访问 https://tushare.pro/ 查看积分和权限说明")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def query_and_display_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的同花顺指数数据
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的同花顺指数数据...")
    
    try:
        # 查询所有数据
        df = db.query_ths_index(limit=10)
        
        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有同花顺指数数据")
            return
        
        logger.info(f"📋 数据库中共有 {len(df)} 条同花顺指数记录")
        logger.info("📖 前10条记录示例：")
        
        for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
            logger.info(f"   {i:2d}. {row.get('name', 'N/A')} ({row.get('ts_code', 'N/A')}) "
                       f"- 类型:{row.get('type', 'N/A')} - 成分股:{row.get('count', 'N/A')}个")
        
        # 按类型统计
        logger.info("\n📈 按类型统计：")
        type_counts = df['type'].value_counts() if 'type' in df.columns else {}
        for idx_type, count in type_counts.items():
            # 这里无法调用fetcher的方法，直接使用映射
            type_mapping = {
                'N': '概念指数',
                'I': '行业指数', 
                'R': '地域指数',
                'S': '同花顺特色指数',
                'ST': '同花顺风格指数',
                'TH': '同花顺主题指数',
                'BB': '同花顺宽基指数'
            }
            type_name = type_mapping.get(idx_type, '未知类型')
            logger.info(f"   {type_name}({idx_type}): {count} 个")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    logger.info("🚀 同花顺概念和行业指数数据初始化开始...")
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
            stats = fetch_and_store_ths_index_data(fetcher, db)
            
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
                logger.info("🎉 同花顺概念和行业指数数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_ths_index() 方法查询数据")
                logger.info("   - 支持按指数类型、交易所等条件筛选")
                logger.info("   - 数据表名: ths_index")
                return True
            else:
                logger.error("❌ 同花顺概念和行业指数数据初始化失败")
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
