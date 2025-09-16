#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同花顺概念指数成分股数据初始化脚本

功能：
1. 获取所有概念指数的成分股数据
2. 创建数据库表结构
3. 将成分股数据初始化到数据库中
4. 提供数据查询和统计功能

使用方法：
python3 init_ths_member.py [选项]

选项：
--limit N      仅处理前N个概念指数（用于测试）
--batch-size N 每批插入的指数数量（默认20）
--delay N      API调用延迟秒数（默认0.3）

示例：
python3 init_ths_member.py --limit 10  # 仅测试前10个概念指数
python3 init_ths_member.py              # 获取所有概念指数成分股
"""

import argparse
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
        logging.FileHandler('init_ths_member.log', encoding='utf-8'),
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
        
        # 创建同花顺概念指数表（如果不存在）
        if not db.create_ths_index_table():
            logger.error("❌ 创建同花顺概念指数表失败")
            return False
            
        # 创建同花顺概念指数成分股表
        if not db.create_ths_member_table():
            logger.error("❌ 创建同花顺概念指数成分股表失败")
            return False
            
        logger.info("✅ 数据库表创建成功")
        return True
        
    except Exception as e:
        logger.error(f"❌ 创建数据库表时发生错误: {e}")
        return False


def fetch_and_store_concept_members(fetcher: StockDataFetcher, db: StockDatabase, 
                                  limit: int = None, batch_size: int = 20,
                                  batch_delay: float = 0.3) -> dict:
    """
    获取并存储概念指数成分股数据
    
    Args:
        fetcher: 数据获取器实例
        db: 数据库实例
        limit: 限制处理的概念指数数量（用于测试）
        batch_size: 每批插入的指数数量
        batch_delay: API调用延迟
        
    Returns:
        dict: 统计信息
    """
    stats = {
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None,
        'total_indexes': 0,
        'processed_indexes': 0,
        'successful_indexes': 0,
        'total_members': 0,
        'successful_insert': False
    }
    
    logger.info("📊 开始获取概念指数成分股数据...")
    
    try:
        # 获取概念指数列表
        concept_df = db.query_ths_index(index_type='N')
        
        if concept_df is None or concept_df.empty:
            logger.error("❌ 数据库中没有概念指数数据，请先运行 init_ths_index.py")
            return stats
        
        concept_indexes = concept_df['ts_code'].tolist()
        stats['total_indexes'] = len(concept_indexes)
        
        # 如果设置了限制，只处理前N个
        if limit and limit < len(concept_indexes):
            concept_indexes = concept_indexes[:limit]
            stats['processed_indexes'] = limit
            logger.info(f"⚠️ 测试模式：仅处理前 {limit} 个概念指数")
        else:
            stats['processed_indexes'] = len(concept_indexes)
        
        logger.info(f"📈 准备处理 {stats['processed_indexes']} 个概念指数的成分股")
        
        # 使用批量获取和插入方法
        batch_stats = fetcher.get_concept_members_batch_with_db_insert(
            db_instance=db,
            concept_indexes=concept_indexes,
            batch_delay=batch_delay,
            batch_size=batch_size
        )
        
        if batch_stats:
            stats['successful_indexes'] = batch_stats.get('successful_indexes', 0)
            stats['total_members'] = batch_stats.get('total_members', 0)
            stats['successful_insert'] = batch_stats.get('successful_batches', 0) > 0
            
            logger.info("✅ 概念指数成分股数据获取和插入完成！")
            
            # 显示统计信息
            logger.info("📊 数据统计：")
            logger.info(f"   处理概念指数: {stats['processed_indexes']} 个")
            logger.info(f"   成功获取指数: {stats['successful_indexes']} 个")
            logger.info(f"   总成分股记录: {stats['total_members']:,} 条")
        else:
            logger.error("❌ 批量获取成分股数据失败")
            
    except Exception as e:
        logger.error(f"❌ 获取和存储数据时发生错误: {e}")
        
        # 检查是否是权限问题
        if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
            logger.error("💡 提示：同花顺概念指数成分股接口需要5000积分权限")
            logger.error("   请检查您的Tushare账户积分或升级账户权限")
            logger.error("   访问 https://tushare.pro/ 查看积分和权限说明")
    
    finally:
        stats['end_time'] = datetime.now()
        stats['duration'] = stats['end_time'] - stats['start_time']
    
    return stats


def query_and_display_sample_data(db: StockDatabase) -> None:
    """
    查询并显示数据库中的成分股数据样例
    
    Args:
        db: 数据库实例
    """
    logger.info("🔍 查询数据库中的成分股数据样例...")
    
    try:
        # 查询样例数据
        df = db.query_ths_member(limit=20)
        
        if df is None or df.empty:
            logger.warning("⚠️ 数据库中没有成分股数据")
            return
        
        logger.info(f"📋 数据库中共有成分股记录（显示前20条）：")
        
        for i, (_, row) in enumerate(df.head(20).iterrows(), 1):
            index_name = row.get('index_name', 'N/A')
            con_name = row.get('con_name', 'N/A')
            con_code = row.get('con_code', 'N/A')
            logger.info(f"   {i:2d}. {index_name} - {con_name}({con_code})")
        
        # 统计信息
        logger.info("\n📊 数据库统计：")
        
        # 查询总记录数
        all_data = db.query_ths_member()
        if all_data is not None and not all_data.empty:
            total_records = len(all_data)
            unique_indexes = all_data['ts_code'].nunique()
            unique_stocks = all_data['con_code'].nunique()
            
            logger.info(f"   总成分股记录: {total_records:,} 条")
            logger.info(f"   涉及指数数量: {unique_indexes} 个")
            logger.info(f"   不重复股票数: {unique_stocks} 只")
            
            # 显示成分股数量最多的指数
            if 'index_name' in all_data.columns:
                top_indexes = all_data.groupby(['ts_code', 'index_name']).size().reset_index(name='member_count')
                top_indexes = top_indexes.sort_values('member_count', ascending=False).head(5)
                
                logger.info("\n🏆 成分股数量最多的指数TOP5：")
                for i, (_, row) in enumerate(top_indexes.iterrows(), 1):
                    logger.info(f"   {i}. {row['index_name']} ({row['ts_code']}): {row['member_count']} 只成分股")
        
    except Exception as e:
        logger.error(f"❌ 查询数据时发生错误: {e}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='同花顺概念指数成分股数据初始化')
    parser.add_argument('--limit', type=int, help='仅处理前N个概念指数（用于测试）')
    parser.add_argument('--batch-size', type=int, default=20, help='每批插入的指数数量（默认20）')
    parser.add_argument('--delay', type=float, default=0.3, help='API调用延迟秒数（默认0.3）')
    
    args = parser.parse_args()
    
    logger.info("🚀 同花顺概念指数成分股数据初始化开始...")
    logger.info("=" * 70)
    
    if args.limit:
        logger.info(f"🧪 测试模式：仅处理前 {args.limit} 个概念指数")
    
    logger.info(f"⚙️ 配置参数：批次大小={args.batch_size}, API延迟={args.delay}秒")
    
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
            
            # 获取并存储成分股数据
            stats = fetch_and_store_concept_members(
                fetcher, db, 
                limit=args.limit,
                batch_size=args.batch_size,
                batch_delay=args.delay
            )
            
            # 查询并显示数据（验证插入结果）
            if stats['successful_insert']:
                query_and_display_sample_data(db)
            
            # 显示总体统计
            logger.info("\n" + "=" * 70)
            logger.info("📊 初始化完成统计：")
            logger.info(f"   📈 处理概念指数: {stats['processed_indexes']} 个")
            logger.info(f"   ✅ 成功获取指数: {stats['successful_indexes']} 个")
            logger.info(f"   📊 总成分股记录: {stats['total_members']:,} 条")
            logger.info(f"   💾 数据插入状态: {'成功' if stats['successful_insert'] else '失败'}")
            logger.info(f"   ⏱️  总耗时: {stats['duration']}")
            
            if stats['successful_insert']:
                logger.info("🎉 同花顺概念指数成分股数据初始化成功！")
                logger.info("\n💡 使用提示：")
                logger.info("   - 可以使用 database.py 中的 query_ths_member() 方法查询数据")
                logger.info("   - 支持按指数代码、股票代码、股票名称等条件筛选")
                logger.info("   - 数据表名: ths_member")
                logger.info("   - 可使用外键关联查询指数和成分股信息")
                return True
            else:
                logger.error("❌ 同花顺概念指数成分股数据初始化失败")
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
