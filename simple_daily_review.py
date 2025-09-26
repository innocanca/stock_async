#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简化版每日市场复盘工具
快速生成专业的市场复盘报告
"""

import logging
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import sys
import os

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import StockDatabase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class SimpleMarketReviewer:
    """简化版市场复盘分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
    
    def get_today_market_data(self, target_date: str = None) -> pd.DataFrame:
        """获取今日市场数据"""
        with self.db:
            if target_date is None:
                # 获取最新交易日数据
                latest_data = self.db.query_data(limit=1)
                if latest_data is None or latest_data.empty:
                    return pd.DataFrame()
                target_date = latest_data.iloc[0]['trade_date'].strftime('%Y-%m-%d')
            
            # 获取当日所有主板股票数据
            today_data = self.db.query_data(start_date=target_date, end_date=target_date)
            
            # 过滤主板股票
            if today_data is not None and not today_data.empty:
                mainboard_data = today_data[
                    (~today_data['ts_code'].str.startswith('300')) &  # 排除创业板
                    (~today_data['ts_code'].str.startswith('688')) &  # 排除科创板
                    (~today_data['ts_code'].str.startswith('830')) &  # 排除北交所
                    (~today_data['ts_code'].str.startswith('430'))    # 排除北交所
                ]
                
                logger.info(f"获取 {target_date} 主板股票数据: {len(mainboard_data)} 只")
                return mainboard_data
            
            return pd.DataFrame()
    
    def analyze_market_overview(self, today_data: pd.DataFrame, target_date: str) -> dict:
        """分析市场概况"""
        if today_data.empty:
            return {}
        
        # 基础统计
        total_stocks = len(today_data)
        up_stocks = len(today_data[today_data['change_pct'] > 0])
        down_stocks = len(today_data[today_data['change_pct'] < 0])
        flat_stocks = total_stocks - up_stocks - down_stocks
        
        # 涨跌分布
        up_ratio = up_stocks / total_stocks * 100
        down_ratio = down_stocks / total_stocks * 100
        
        # 涨停跌停统计
        limit_up_stocks = today_data[today_data['change_pct'] >= 9.8]
        limit_down_stocks = today_data[today_data['change_pct'] <= -9.8]
        
        # 成交额统计
        total_amount = today_data['amount'].sum() / 1000000  # 转换为万亿
        
        # 大涨大跌统计
        big_up = len(today_data[today_data['change_pct'] >= 5])
        big_down = len(today_data[today_data['change_pct'] <= -5])
        
        return {
            'date': target_date,
            'total_stocks': total_stocks,
            'up_stocks': up_stocks,
            'down_stocks': down_stocks,
            'flat_stocks': flat_stocks,
            'up_ratio': up_ratio,
            'down_ratio': down_ratio,
            'limit_up_count': len(limit_up_stocks),
            'limit_down_count': len(limit_down_stocks),
            'big_up_count': big_up,
            'big_down_count': big_down,
            'total_amount': total_amount,
            'limit_up_stocks': limit_up_stocks
        }
    
    def get_top_performers(self, today_data: pd.DataFrame, top_n: int = 20) -> dict:
        """获取表现最佳和最差的股票"""
        if today_data.empty:
            return {}
        
        # 涨幅榜
        top_gainers = today_data.nlargest(top_n, 'change_pct')
        
        # 跌幅榜  
        top_losers = today_data.nsmallest(top_n, 'change_pct')
        
        # 成交额榜
        top_volume = today_data.nlargest(top_n, 'amount')
        
        return {
            'top_gainers': top_gainers,
            'top_losers': top_losers,
            'top_volume': top_volume
        }
    
    def analyze_industry_performance(self, today_data: pd.DataFrame) -> list:
        """分析行业表现"""
        if today_data.empty:
            return []
        
        try:
            with self.db:
                # 获取行业数据
                industry_query = f"""
                SELECT d.ts_code, d.change_pct, d.vol, d.amount, 
                       s.name as stock_name, s.industry
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
                WHERE d.trade_date = '{today_data.iloc[0]['trade_date'].strftime('%Y-%m-%d')}'
                AND s.industry IS NOT NULL
                AND d.ts_code NOT LIKE '300%'
                AND d.ts_code NOT LIKE '688%'
                ORDER BY d.change_pct DESC
                """
                
                industry_data = pd.read_sql(industry_query, self.db.connection)
                
                if industry_data.empty:
                    return []
                
                # 按行业分组统计
                industry_stats = []
                
                for industry, group in industry_data.groupby('industry'):
                    if len(group) < 10:  # 行业股票数量太少跳过
                        continue
                    
                    avg_pct = group['change_pct'].mean()
                    up_count = len(group[group['change_pct'] > 0])
                    total_count = len(group)
                    up_ratio = up_count / total_count * 100
                    
                    # 行业内涨幅最大的股票
                    top_stock = group.iloc[0]
                    
                    industry_stats.append({
                        'industry': industry,
                        'avg_pct': avg_pct,
                        'up_ratio': up_ratio,
                        'stock_count': total_count,
                        'top_stock_name': top_stock['stock_name'],
                        'top_stock_pct': top_stock['change_pct']
                    })
                
                # 按平均涨幅排序
                industry_stats.sort(key=lambda x: x['avg_pct'], reverse=True)
                
                return industry_stats[:15]  # 返回前15个行业
                
        except Exception as e:
            logger.warning(f"分析行业表现时出错: {e}")
            return []
    
    def generate_simple_review(self, target_date: str = None) -> str:
        """生成简化版复盘报告"""
        # 获取数据
        today_data = self.get_today_market_data(target_date)
        
        if today_data.empty:
            return "❌ 无法获取市场数据"
        
        # 分析数据
        market_overview = self.analyze_market_overview(today_data, target_date or today_data.iloc[0]['trade_date'].strftime('%Y-%m-%d'))
        top_performers = self.get_top_performers(today_data)
        industry_stats = self.analyze_industry_performance(today_data)
        
        # 生成报告
        report = []
        
        # 标题
        report.append(f"# 📊 每日市场复盘 ({market_overview['date']})")
        report.append("=" * 80)
        
        # 市场行情
        report.append("\n## 🎯 市场行情")
        up_ratio = market_overview['up_ratio']
        down_ratio = market_overview['down_ratio']
        total_amount = market_overview['total_amount']
        limit_up_count = market_overview['limit_up_count']
        
        if up_ratio > 70:
            market_mood = "普涨行情，市场情绪高涨"
        elif up_ratio > 50:
            market_mood = "涨跌均衡，市场情绪稳定"
        elif up_ratio > 30:
            market_mood = "涨跌分化，市场轮动明显"
        else:
            market_mood = "跌多涨少，市场情绪低迷"
        
        report.append(f"{market_mood}，主板股票上涨{market_overview['up_stocks']}只({up_ratio:.1f}%)，"
                     f"下跌{market_overview['down_stocks']}只({down_ratio:.1f}%)，"
                     f"平盘{market_overview['flat_stocks']}只。")
        
        report.append(f"市场成交额{total_amount:.2f}万亿，涨停家数{limit_up_count}只。")
        
        # 涨停分析
        if limit_up_count > 0:
            report.append("\n## 🔥 涨停分析")
            limit_up_stocks = market_overview['limit_up_stocks']
            
            # 按行业统计涨停股票
            if not limit_up_stocks.empty:
                # 获取涨停股票的行业信息
                try:
                    with self.db:
                        limit_up_codes = "','".join(limit_up_stocks['ts_code'].tolist())
                        industry_query = f"""
                        SELECT s.name, s.industry, d.change_pct, d.close
                        FROM stock_basic s
                        JOIN daily_data d ON s.ts_code = d.ts_code
                        WHERE d.trade_date = '{market_overview['date']}'
                        AND s.ts_code IN ('{limit_up_codes}')
                        ORDER BY d.change_pct DESC
                        """
                        
                        limit_up_details = pd.read_sql(industry_query, self.db.connection)
                        
                        if not limit_up_details.empty:
                            # 统计涨停板块分布
                            industry_counts = limit_up_details['industry'].value_counts()
                            
                            report.append("\n**涨停板块分布：**")
                            for industry, count in industry_counts.head(8).items():
                                if industry and industry != '未知':
                                    industry_stocks = limit_up_details[limit_up_details['industry'] == industry]
                                    top_stock = industry_stocks.iloc[0]
                                    report.append(f"- **{industry}**({count}只)：{top_stock['name']}等")
                            
                            # 涨停股票详情
                            report.append("\\n**涨停股票TOP10：**")
                            for i, (_, stock) in enumerate(limit_up_details.head(10).iterrows(), 1):
                                report.append(f"{i:>2}. {stock['name']}({stock['change_pct']:.1f}%) - {stock['industry']}")
                
                except Exception as e:
                    logger.warning(f"分析涨停详情时出错: {e}")
        
        # 行业表现
        if industry_stats:
            report.append("\\n## 🏢 板块表现")
            
            report.append("\\n### 📈 强势板块TOP8")
            for i, industry in enumerate(industry_stats[:8], 1):
                report.append(f"{i}. **{industry['industry']}**：领涨{industry['top_stock_name']}({industry['top_stock_pct']:+.1f}%)，"
                             f"板块平均{industry['avg_pct']:+.1f}%，上涨比例{industry['up_ratio']:.0f}%")
            
            report.append("\\n### 📉 弱势板块")
            for industry in industry_stats[-3:]:
                report.append(f"- **{industry['industry']}**：平均{industry['avg_pct']:+.1f}%，"
                             f"上涨比例{industry['up_ratio']:.0f}%")
        
        # 个股表现
        if top_performers:
            report.append("\\n## ⭐ 个股表现")
            
            # 涨幅榜
            top_gainers = top_performers['top_gainers']
            if not top_gainers.empty:
                report.append("\\n### 🚀 涨幅榜TOP10")
                for i, (_, stock) in enumerate(top_gainers.head(10).iterrows(), 1):
                    code = stock['ts_code'].split('.')[0]
                    amount_yi = stock['amount'] / 10000
                    report.append(f"{i:>2}. {code}：{stock['change_pct']:+.1f}%，"
                                 f"价格{stock['close']:.2f}元，成交{amount_yi:.1f}亿")
            
            # 成交额榜
            top_volume = top_performers['top_volume']
            if not top_volume.empty:
                report.append("\\n### 💰 成交额榜TOP5")
                for i, (_, stock) in enumerate(top_volume.head(5).iterrows(), 1):
                    code = stock['ts_code'].split('.')[0]
                    amount_yi = stock['amount'] / 10000
                    report.append(f"{i}. {code}：成交{amount_yi:.1f}亿，"
                                 f"涨幅{stock['change_pct']:+.1f}%")
        
        # 运行策略分析
        report.append("\\n## 🎯 策略机会")
        
        try:
            # 快速运行策略
            from notify.strong_pullback_notify import find_strong_pullback_stocks
            
            pullback_opportunities = find_strong_pullback_stocks(min_signal_strength=80.0)
            
            if not pullback_opportunities.empty:
                report.append(f"\\n### 📈 强势回调低吸机会 ({len(pullback_opportunities)}只)")
                for i, (_, stock) in enumerate(pullback_opportunities.head(5).iterrows(), 1):
                    code = stock['ts_code'].split('.')[0]
                    report.append(f"{i}. **{stock['stock_name']}**({code})：前期涨{stock['previous_surge']:.1f}%，"
                                 f"距MA5{stock['ma5_distance']:+.1f}%，信号{stock['signal_strength']:.0f}分")
            else:
                report.append("\\n暂无明显的强势回调机会。")
        
        except Exception as e:
            logger.warning(f"策略分析时出错: {e}")
            report.append("\\n策略分析暂时不可用。")
        
        # 市场总结
        report.append("\\n## 📝 市场总结")
        
        if up_ratio > 60:
            sentiment = "市场情绪积极"
            suggestion = "可适当加大仓位，关注强势品种"
        elif up_ratio > 40:
            sentiment = "市场情绪分化"
            suggestion = "精选个股，注意板块轮动"
        else:
            sentiment = "市场情绪谨慎"
            suggestion = "控制仓位，等待企稳信号"
        
        report.append(f"{sentiment}，{suggestion}。")
        
        if limit_up_count > 30:
            report.append("涨停家数较多，赚钱效应良好，可关注次日连板机会。")
        elif limit_up_count > 10:
            report.append("涨停家数一般，市场活跃度适中。")
        else:
            report.append("涨停家数较少，市场缺乏热点。")
        
        report.append("\\n**操作策略**：")
        
        if industry_stats:
            top_industry = industry_stats[0]['industry']
            report.append(f"- 重点关注 **{top_industry}** 等强势板块")
        
        if not pullback_opportunities.empty:
            report.append("- 关注强势股技术回调的低吸机会")
        
        report.append("- 严格止损，控制单股仓位")
        report.append("- 根据市场情绪调整仓位大小")
        
        report.append("\\n---")
        report.append("*数据来源：A股主板股票实时数据*")
        report.append("*分析工具：基于选手操作模式的量化策略*")
        
        return "\n".join(report)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='每日市场复盘工具')
    parser.add_argument('--date', help='指定复盘日期（YYYY-MM-DD格式）')
    parser.add_argument('--output', help='输出文件路径')
    
    args = parser.parse_args()
    
    try:
        reviewer = SimpleMarketReviewer()
        
        logger.info("开始生成每日市场复盘...")
        
        # 生成复盘报告
        review_report = reviewer.generate_simple_review(args.date)
        
        # 输出报告
        print(review_report)
        
        # 保存文件
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(review_report)
            logger.info(f"复盘报告已保存到 {args.output}")
        
        # 自动保存
        today_str = datetime.now().strftime('%Y%m%d')
        auto_file = f"daily_review_{today_str}.md"
        with open(auto_file, 'w', encoding='utf-8') as f:
            f.write(review_report)
        
        logger.info(f"✅ 复盘报告已生成并保存到 {auto_file}")
        
    except Exception as e:
        logger.error(f"生成复盘报告时出错: {e}")


if __name__ == "__main__":
    main()
