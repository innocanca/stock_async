#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日市场复盘分析工具
基于股票数据库自动生成专业的市场复盘报告
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict, Counter

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from database import StockDatabase

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DailyMarketReviewer:
    """每日市场复盘分析器"""
    
    def __init__(self):
        self.db = StockDatabase()
        self.today_data = None
        self.yesterday_data = None
        self.week_data = None
    
    def get_market_data(self, target_date: str = None) -> dict:
        """
        获取市场数据
        
        Args:
            target_date: 目标日期，None表示最新交易日
            
        Returns:
            dict: 包含今日、昨日、本周数据
        """
        with self.db:
            if target_date is None:
                # 获取最新交易日
                latest_data = self.db.query_data(limit=1)
                if latest_data is None or latest_data.empty:
                    return {}
                target_date = latest_data.iloc[0]['trade_date'].strftime('%Y-%m-%d')
            
            logger.info(f"获取 {target_date} 的市场数据...")
            
            # 获取今日数据
            today_data = self.db.query_data(
                start_date=target_date,
                end_date=target_date
            )
            
            # 获取前一交易日数据
            yesterday_date = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=3)).strftime('%Y-%m-%d')
            yesterday_data = self.db.query_data(
                start_date=yesterday_date,
                end_date=yesterday_date
            )
            
            # 获取本周数据
            week_start = (datetime.strptime(target_date, '%Y-%m-%d') - timedelta(days=7)).strftime('%Y-%m-%d')
            week_data = self.db.query_data(
                start_date=week_start,
                end_date=target_date
            )
            
            return {
                'target_date': target_date,
                'today': today_data,
                'yesterday': yesterday_data,
                'week': week_data
            }
    
    def analyze_market_overview(self, market_data: dict) -> dict:
        """
        分析市场概况
        """
        today_data = market_data.get('today')
        yesterday_data = market_data.get('yesterday')
        
        if today_data is None or today_data.empty:
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
        limit_up = len(today_data[today_data['change_pct'] >= 9.5])
        limit_down = len(today_data[today_data['change_pct'] <= -9.5])
        
        # 成交额统计
        total_amount = today_data['amount'].sum() / 10000  # 转换为万亿
        
        # 昨日对比
        yesterday_stats = {}
        if yesterday_data is not None and not yesterday_data.empty:
            yesterday_limit_up = len(yesterday_data[yesterday_data['change_pct'] >= 9.5])
            yesterday_amount = yesterday_data['amount'].sum() / 10000
            
            yesterday_stats = {
                'limit_up': yesterday_limit_up,
                'amount': yesterday_amount
            }
        
        return {
            'date': market_data['target_date'],
            'total_stocks': total_stocks,
            'up_stocks': up_stocks,
            'down_stocks': down_stocks,
            'up_ratio': up_ratio,
            'down_ratio': down_ratio,
            'limit_up': limit_up,
            'limit_down': limit_down,
            'total_amount': total_amount,
            'yesterday_stats': yesterday_stats
        }
    
    def analyze_continuous_limit_up(self, market_data: dict) -> list:
        """
        分析连板梯队
        """
        today_data = market_data.get('today')
        week_data = market_data.get('week')
        
        if today_data is None or week_data is None:
            return []
        
        # 获取连板股票
        continuous_stocks = []
        
        # 今日涨停股票
        today_limit_up = today_data[today_data['change_pct'] >= 9.5]
        
        for _, stock in today_limit_up.iterrows():
            ts_code = stock['ts_code']
            
            # 查询该股票的连板天数
            stock_week_data = week_data[week_data['ts_code'] == ts_code].sort_values('trade_date')
            
            continuous_days = 0
            for _, day_data in stock_week_data.iterrows():
                if day_data['change_pct'] >= 9.5:
                    continuous_days += 1
                else:
                    continuous_days = 0
            
            if continuous_days >= 2:  # 至少2连板
                # 获取股票基本信息
                try:
                    with self.db:
                        stock_info_query = f"""
                        SELECT name, industry FROM stock_basic WHERE ts_code = '{ts_code}'
                        """
                        stock_info = pd.read_sql(stock_info_query, self.db.connection)
                        
                        if not stock_info.empty:
                            stock_name = stock_info.iloc[0]['name']
                            industry = stock_info.iloc[0]['industry']
                        else:
                            stock_name = ts_code
                            industry = '未知'
                except:
                    stock_name = ts_code
                    industry = '未知'
                
                continuous_stocks.append({
                    'ts_code': ts_code,
                    'name': stock_name,
                    'industry': industry,
                    'continuous_days': continuous_days,
                    'close': stock['close'],
                    'pct_chg': stock['change_pct'],
                    'amount_yi': stock['amount'] / 10000
                })
        
        # 按连板天数排序
        continuous_stocks.sort(key=lambda x: x['continuous_days'], reverse=True)
        
        return continuous_stocks
    
    def analyze_sector_performance(self, market_data: dict) -> dict:
        """
        分析板块表现
        """
        today_data = market_data.get('today')
        
        if today_data is None or today_data.empty:
            return {}
        
        # 获取股票基本信息
        with self.db:
            # 联合查询获取行业信息
            sector_query = f"""
            SELECT d.ts_code, d.change_pct, d.vol, d.amount, d.close,
                   s.name, s.industry, s.area
            FROM daily_data d
            LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
            WHERE d.trade_date = '{market_data['target_date']}'
            AND s.industry IS NOT NULL
            ORDER BY d.change_pct DESC
            """
            
            sector_data = pd.read_sql(sector_query, self.db.connection)
        
        if sector_data.empty:
            return {}
        
        # 按行业分组统计
        sector_stats = []
        
        for industry, group in sector_data.groupby('industry'):
            if len(group) < 5:  # 行业股票数量太少跳过
                continue
            
            avg_pct = group['change_pct'].mean()
            up_count = len(group[group['change_pct'] > 0])
            total_count = len(group)
            up_ratio = up_count / total_count * 100
            
            # 获取行业内表现最好的股票
            top_stock = group.iloc[0]
            
            sector_stats.append({
                'industry': industry,
                'avg_pct': avg_pct,
                'up_ratio': up_ratio,
                'stock_count': total_count,
                'top_stock': {
                    'name': top_stock['name'],
                    'ts_code': top_stock['ts_code'],
                    'pct_chg': top_stock['change_pct'],
                    'close': top_stock['close']
                }
            })
        
        # 按平均涨幅排序
        sector_stats.sort(key=lambda x: x['avg_pct'], reverse=True)
        
        return {
            'sectors': sector_stats[:10],  # 前10强势板块
            'weak_sectors': sector_stats[-5:]  # 后5弱势板块
        }
    
    def analyze_hot_concepts(self, market_data: dict) -> list:
        """
        分析热点概念
        """
        today_data = market_data.get('today')
        
        if today_data is None or today_data.empty:
            return []
        
        # 获取涨幅前50的股票，分析概念分布
        top_stocks = today_data.nlargest(50, 'change_pct')
        
        hot_concepts = []
        
        try:
            with self.db:
                # 获取这些股票的概念信息
                stock_codes = "','".join(top_stocks['ts_code'].tolist())
                concept_query = f"""
                SELECT tm.con_code, ti.name as concept_name, d.change_pct, s.name as stock_name
                FROM ths_member tm
                JOIN ths_index ti ON tm.ts_code = ti.ts_code
                JOIN daily_data d ON tm.con_code = d.ts_code
                JOIN stock_basic s ON d.ts_code = s.ts_code
                WHERE d.trade_date = '{market_data['target_date']}'
                AND tm.con_code IN ('{stock_codes}')
                AND ti.type IN ('N', 'TH')
                ORDER BY d.change_pct DESC
                """
                
                concept_data = pd.read_sql(concept_query, self.db.connection)
                
                if not concept_data.empty:
                    # 统计概念热度
                    concept_counts = concept_data['concept_name'].value_counts()
                    
                    for concept, count in concept_counts.head(10).items():
                        if count >= 3:  # 至少3只股票
                            concept_stocks = concept_data[concept_data['concept_name'] == concept]
                            avg_pct = concept_stocks['change_pct'].mean()
                            
                            hot_concepts.append({
                                'concept': concept,
                                'stock_count': count,
                                'avg_pct': avg_pct,
                                'top_stocks': concept_stocks.head(3)['stock_name'].tolist()
                            })
        
        except Exception as e:
            logger.warning(f"分析热点概念时出错: {e}")
        
        return sorted(hot_concepts, key=lambda x: x['avg_pct'], reverse=True)
    
    def generate_daily_review(self, target_date: str = None) -> str:
        """
        生成每日复盘报告
        
        Args:
            target_date: 目标日期
            
        Returns:
            str: 复盘报告
        """
        # 获取市场数据
        market_data = self.get_market_data(target_date)
        
        if not market_data:
            return "❌ 无法获取市场数据"
        
        # 分析各个维度
        market_overview = self.analyze_market_overview(market_data)
        continuous_stocks = self.analyze_continuous_limit_up(market_data)
        sector_performance = self.analyze_sector_performance(market_data)
        hot_concepts = self.analyze_hot_concepts(market_data)
        
        # 生成报告
        report = []
        
        # 标题
        report.append(f"# 📊 每日市场复盘 ({market_overview.get('date', 'N/A')})")
        report.append("=" * 80)
        
        # 市场行情概述
        report.append("\\n## 🎯 市场行情")
        
        if market_overview:
            up_ratio = market_overview['up_ratio']
            down_ratio = market_overview['down_ratio']
            total_amount = market_overview['total_amount']
            limit_up = market_overview['limit_up']
            
            yesterday_limit_up = market_overview['yesterday_stats'].get('limit_up', 0)
            yesterday_amount = market_overview['yesterday_stats'].get('amount', 0)
            
            if up_ratio > 60:
                market_mood = "普涨行情，市场情绪乐观"
            elif up_ratio > 40:
                market_mood = "涨跌参半，市场分化明显"
            else:
                market_mood = "跌多涨少，市场情绪谨慎"
            
            report.append(f"今日A股{market_mood}，上涨{market_overview['up_stocks']}只({up_ratio:.1f}%)，"
                         f"下跌{market_overview['down_stocks']}只({down_ratio:.1f}%)。")
            
            report.append(f"市场成交量{total_amount:.2f}万亿，"
                         f"{'放量' if yesterday_amount > 0 and total_amount > yesterday_amount else '缩量' if yesterday_amount > 0 else '正常'}交易。")
            
            report.append(f"涨停家数{limit_up}只"
                         f"{'较昨日' + str(yesterday_limit_up) + '只有所' + ('上升' if limit_up > yesterday_limit_up else '下降' if limit_up < yesterday_limit_up else '持平') if yesterday_limit_up > 0 else ''}。")
        
        # 连板梯队分析
        report.append("\\n## 🔥 连板梯队")
        
        if continuous_stocks:
            report.append("\\n**连板梯队：**")
            
            # 按连板天数分组
            board_groups = defaultdict(list)
            for stock in continuous_stocks:
                board_groups[stock['continuous_days']].append(stock)
            
            for days in sorted(board_groups.keys(), reverse=True):
                stocks_list = board_groups[days]
                if days >= 5:
                    report.append(f"\\n**{days}板**：")
                    for stock in stocks_list:
                        industry_tag = f"（{stock['industry']}）" if stock['industry'] != '未知' else ""
                        report.append(f"- {stock['name']}{industry_tag}")
                elif days >= 2:
                    stock_names = [f"{s['name']}（{s['industry']}）" for s in stocks_list[:5]]
                    report.append(f"\\n**{days}板**：{', '.join(stock_names)}")
                    if len(stocks_list) > 5:
                        report.append(f"等{len(stocks_list)}只")
        else:
            report.append("今日无明显连板梯队。")
        
        # 板块表现分析
        report.append("\\n## 🏢 板块表现")
        
        if sector_performance and sector_performance.get('sectors'):
            # 强势板块
            report.append("\\n### 📈 强势板块TOP5")
            for i, sector in enumerate(sector_performance['sectors'][:5], 1):
                top_stock = sector['top_stock']
                report.append(f"{i}. **{sector['industry']}**：领涨{top_stock['name']}({top_stock['pct_chg']:+.1f}%)，"
                             f"板块平均{sector['avg_pct']:+.1f}%，上涨比例{sector['up_ratio']:.0f}%")
            
            # 弱势板块
            report.append("\\n### 📉 弱势板块")
            for sector in sector_performance['weak_sectors']:
                report.append(f"- **{sector['industry']}**：平均{sector['avg_pct']:+.1f}%，"
                             f"上涨比例{sector['up_ratio']:.0f}%")
        
        # 热点概念分析
        report.append("\\n## 🔥 热点概念")
        
        if hot_concepts:
            report.append("\\n### 💡 概念热度排行")
            for i, concept in enumerate(hot_concepts[:8], 1):
                top_stocks_str = '、'.join(concept['top_stocks'][:3])
                report.append(f"{i}. **{concept['concept']}**：平均{concept['avg_pct']:+.1f}%，"
                             f"活跃股票{concept['stock_count']}只，代表股{top_stocks_str}")
        else:
            report.append("今日概念分化，无明显热点集中。")
        
        # 策略机会分析
        report.append("\\n## 🎯 策略机会")
        
        try:
            # 调用已有策略分析
            from notify.strong_pullback_notify import find_strong_pullback_stocks
            from notify.breakout_follow_notify import find_breakout_follow_stocks
            from notify.volume_acceleration_notify import find_volume_acceleration_stocks
            
            pullback_opportunities = find_strong_pullback_stocks(min_signal_strength=70.0)
            breakout_opportunities = find_breakout_follow_stocks(min_signal_strength=75.0)
            volume_opportunities = find_volume_acceleration_stocks(min_signal_strength=75.0)
            
            report.append("\\n### 📊 策略筛选结果")
            report.append(f"- **强势回调低吸**：{len(pullback_opportunities)}个机会")
            report.append(f"- **高位突破跟进**：{len(breakout_opportunities)}个机会")
            report.append(f"- **放量加速突破**：{len(volume_opportunities)}个机会")
            
            # 显示部分机会
            if not pullback_opportunities.empty:
                top_pullback = pullback_opportunities.head(3)
                report.append("\\n**强势回调低吸机会**：")
                for _, stock in top_pullback.iterrows():
                    code = stock['ts_code'].split('.')[0]
                    report.append(f"- {stock['stock_name']}({code})：前期涨{stock['previous_surge']:.1f}%，"
                                 f"距MA5{stock['ma5_distance']:+.1f}%")
            
            if not breakout_opportunities.empty:
                top_breakout = breakout_opportunities.head(3)
                report.append("\\n**高位突破跟进机会**：")
                for _, stock in top_breakout.iterrows():
                    code = stock['ts_code'].split('.')[0]
                    report.append(f"- {stock['stock_name']}({code})：当日{stock['pct_1d']:+.1f}%，"
                                 f"放量{stock['vol_ratio']:.1f}倍")
        
        except Exception as e:
            logger.warning(f"分析策略机会时出错: {e}")
            report.append("\\n策略机会分析暂时不可用")
        
        # 市场总结
        report.append("\\n## 📝 市场总结")
        
        if market_overview:
            if market_overview['up_ratio'] > 60:
                market_sentiment = "市场情绪积极，做多氛围浓厚"
                suggestion = "可适当加大仓位，关注强势股回调机会"
            elif market_overview['up_ratio'] > 40:
                market_sentiment = "市场分化明显，结构性机会为主"
                suggestion = "精选个股，关注板块轮动节奏"
            else:
                market_sentiment = "市场情绪谨慎，防守为主"
                suggestion = "减少仓位，等待市场企稳信号"
            
            report.append(f"\\n{market_sentiment}。{suggestion}。")
            
            if continuous_stocks:
                max_boards = max([s['continuous_days'] for s in continuous_stocks])
                if max_boards >= 5:
                    report.append(f"连板高度{max_boards}板，市场赚钱效应较好，可关注低位补涨机会。")
                else:
                    report.append(f"连板高度{max_boards}板，市场情绪一般，注意风险控制。")
        
        report.append("\\n**操作建议**：")
        report.append("- 关注强势板块的回调低吸机会")
        report.append("- 重点跟踪连板股的持续性")
        report.append("- 严格控制仓位和止损")
        report.append("- 顺应市场节奏，不逆势操作")
        
        report.append("\\n---")
        report.append("*本复盘基于A股主板股票数据自动生成*")
        
        return "\\n".join(report)


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='每日市场复盘工具')
    parser.add_argument('--date', help='指定复盘日期（YYYY-MM-DD格式），默认为最新交易日')
    parser.add_argument('--output', help='复盘报告输出文件路径')
    parser.add_argument('--format', choices=['markdown', 'text'], default='markdown',
                       help='输出格式（默认markdown）')
    
    args = parser.parse_args()
    
    try:
        reviewer = DailyMarketReviewer()
        
        logger.info("开始生成每日市场复盘...")
        
        # 生成复盘报告
        review_report = reviewer.generate_daily_review(args.date)
        
        # 输出报告
        print(review_report)
        
        # 保存到文件
        if args.output:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(review_report)
            logger.info(f"复盘报告已保存到 {args.output}")
        
        # 自动保存到默认文件
        today_str = datetime.now().strftime('%Y%m%d')
        default_file = f"daily_review_{today_str}.md"
        with open(default_file, 'w', encoding='utf-8') as f:
            f.write(review_report)
        
        logger.info(f"复盘报告已自动保存到 {default_file}")
        
    except Exception as e:
        logger.error(f"生成复盘报告时出错: {e}")


if __name__ == "__main__":
    main()
