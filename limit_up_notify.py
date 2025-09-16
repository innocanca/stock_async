# -*- coding: utf-8 -*-
"""
涨停股票查询与推送脚本
从MySQL数据库查询最近一个交易日的涨停股票，并通过企业微信机器人发送markdown格式的消息
"""

import logging
import sys
from datetime import datetime
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('limit_up_notify.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


def format_stock_code(ts_code: str) -> str:
    """格式化股票代码，去掉交易所后缀"""
    return ts_code.split('.')[0] if '.' in ts_code else ts_code


def get_stock_market(ts_code: str) -> str:
    """根据股票代码获取市场名称"""
    if '.SH' in ts_code:
        return '上交所'
    elif '.SZ' in ts_code:
        return '深交所'
    elif '.BJ' in ts_code:
        return '北交所'
    else:
        return '其他'


def create_limit_up_markdown(df, trade_date: str, top_sector: str = None, sector_stats: dict = None) -> str:
    """
    创建涨停股票的markdown格式消息（聚焦最热板块）
    
    Args:
        df: 涨停股票数据DataFrame（最热板块的股票）
        trade_date: 交易日期
        top_sector: 最热板块名称
        sector_stats: 各板块涨停统计
        
    Returns:
        str: markdown格式的消息内容
    """
    if df.empty:
        return f"""# 📊 涨停股票播报

**交易日期**: {trade_date}

> 今日无涨停股票

---
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    # 统计信息
    sector_count = len(df)
    avg_pct = df['change_pct'].mean()
    total_amount = df['amount'].sum() / 100000  # 转换为亿元
    
    # 计算全市场涨停总数
    total_limit_up = sum(sector_stats.values()) if sector_stats else sector_count
    
    # 构建markdown消息
    markdown_content = f"""# 🔥 最热板块涨停播报

**交易日期**: {trade_date}  
**最热板块**: {top_sector if top_sector else '未知'}  
**该板块涨停数**: {sector_count}只 / 全市场{total_limit_up}只  
**板块平均涨幅**: {avg_pct:.2f}%  
**板块总成交额**: {total_amount:.2f}亿元

## 🏆 {top_sector if top_sector else '涨停'} 板块榜单

| 排名 | 股票名称 | 代码 | 涨幅(%) | 成交额(亿元) |
|------|----------|------|---------|-------------|"""

    # 添加股票信息
    for idx, (_, row) in enumerate(df.iterrows(), 1):
        stock_code = format_stock_code(row['ts_code'])
        stock_name = row.get('name', '未知')  # 获取股票名称
        change_pct = row['change_pct']
        amount = row['amount'] / 100000  # 转换为亿元（原单位：千元）
        
        # 截断过长的股票名称
        if len(stock_name) > 10:
            stock_name = stock_name[:9] + '...'
        
        markdown_content += f"\n| {idx} | {stock_name} | {stock_code} | {change_pct:.2f} | {amount:.2f} |"
        
        # 限制显示前20只
        if idx >= 20:
            remaining = sector_count - 20
            if remaining > 0:
                markdown_content += f"\n| ... | ... | ... | ... | 还有{remaining}只 |"
            break

    # 添加板块分布统计
    if sector_stats and len(sector_stats) > 1:
        markdown_content += f"""

## 📊 各板块涨停分布

"""
        # 按涨停数量排序显示前10个板块
        sorted_sectors = sorted(sector_stats.items(), key=lambda x: x[1], reverse=True)[:10]
        for idx, (sector, count) in enumerate(sorted_sectors, 1):
            emoji = "🔥" if idx == 1 else "🚀" if idx <= 3 else "📈"
            markdown_content += f"- {emoji} **{sector}**: {count}只\n"
        
        if len(sector_stats) > 10:
            markdown_content += f"- 📋 其他板块: {len(sector_stats) - 10}个\n"

    # 市场分布统计
    if not df.empty:
        markdown_content += f"""

## 📊 {top_sector if top_sector else '涨停股票'} 板块市场分布

"""
        market_stats = df['ts_code'].apply(get_stock_market).value_counts()
        for market, count in market_stats.items():
            markdown_content += f"- **{market}**: {count}只\n"

        # 涨幅分布统计
        pct_10_plus = len(df[df['change_pct'] >= 10])
        pct_9_5_10 = len(df[(df['change_pct'] >= 9.5) & (df['change_pct'] < 10)])
        
        markdown_content += f"""

## 📋 {top_sector if top_sector else '涨停股票'} 板块涨幅分布

- **涨停(≥10%)**: {pct_10_plus}只
- **准涨停(9.5%-10%)**: {pct_9_5_10}只"""

    markdown_content += f"""

---
💡 **聚焦策略**: 本次播报聚焦涨停家数最多的热点板块  
*数据来源: Tushare*  
*发送时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*"""

    return markdown_content


def get_top_sector_stocks(df):
    """
    获取涨停家数最多的板块及其股票
    
    Args:
        df: 涨停股票数据DataFrame
        
    Returns:
        tuple: (最热板块名称, 该板块的股票DataFrame, 各板块统计)
    """
    if df.empty:
        return None, df, {}
    
    # 按行业分组统计涨停家数
    sector_counts = df['industry'].value_counts()
    
    # 过滤掉"未知"行业
    if '未知' in sector_counts.index:
        known_sectors = sector_counts[sector_counts.index != '未知']
        if not known_sectors.empty:
            sector_counts = known_sectors
    
    if sector_counts.empty:
        return '未知', df, {'未知': len(df)}
    
    # 获取涨停家数最多的板块
    top_sector = sector_counts.index[0]
    top_sector_count = sector_counts.iloc[0]
    
    # 获取该板块的所有股票
    top_sector_stocks = df[df['industry'] == top_sector].copy()
    
    # 按成交额排序
    top_sector_stocks = top_sector_stocks.sort_values(['amount', 'change_pct'], ascending=[False, False])
    
    logger.info(f"最热板块: {top_sector}，涨停家数: {top_sector_count}")
    
    return top_sector, top_sector_stocks, sector_counts.to_dict()


def main():
    """主函数"""
    logger.info("开始查询涨停股票...")
    
    try:
        # 连接数据库
        with StockDatabase() as db:
            # 获取最近交易日期
            latest_date = db.get_latest_trading_date()
            if not latest_date:
                logger.error("无法获取最近交易日期，请检查数据库中是否有数据")
                return 1
            
            logger.info(f"查询日期: {latest_date}")
            
            # 查询涨停股票 - 使用自动判断涨停条件
            limit_up_df = db.get_limit_up_stocks(trade_date=latest_date)
            
            if limit_up_df is None:
                logger.error("查询涨停股票失败")
                return 1
            
            if limit_up_df.empty:
                logger.info("今日无涨停股票")
                markdown_msg = create_limit_up_markdown(limit_up_df, latest_date, None, {})
            else:
                # 获取涨停家数最多的板块
                top_sector, top_sector_stocks, sector_stats = get_top_sector_stocks(limit_up_df)
                
                # 创建markdown消息 - 只显示最热板块的股票
                markdown_msg = create_limit_up_markdown(top_sector_stocks, latest_date, top_sector, sector_stats)
            
            # 发送消息
            logger.info("准备发送涨停股票消息...")
            send_markdown_message(markdown_msg)
            
            if not limit_up_df.empty:
                top_sector, top_sector_stocks, _ = get_top_sector_stocks(limit_up_df)
                logger.info(f"涨停股票查询完成，最热板块: {top_sector}，该板块涨停数: {len(top_sector_stocks)}")
            else:
                logger.info("涨停股票查询完成，今日无涨停股票")
            
    except Exception as e:
        logger.error(f"程序执行失败: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())
