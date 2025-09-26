#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
每日市场复盘工具 - 集成版
自动生成专业的市场复盘报告并推送消息
"""

import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
import os

# 添加父目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('daily_market_review.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


class DailyMarketReviewer:
    """每日市场复盘工具"""
    
    def __init__(self):
        self.today_data = None
        self.all_market_data = None
        self.market_stats = {}
    
    def get_latest_market_data(self) -> pd.DataFrame:
        """获取最新交易日的市场数据"""
        try:
            with StockDatabase() as db:
                # 获取最新交易日数据
                latest_data = db.query_data(limit=1)
                if latest_data is None or latest_data.empty:
                    logger.error("无法获取最新交易日数据")
                    return pd.DataFrame()
                
                target_date = latest_data.iloc[0]['trade_date']
                target_date_str = target_date.strftime('%Y-%m-%d')
                
                logger.info(f"获取 {target_date_str} 的市场数据...")
                
                # 获取当日所有股票数据（用于计算整个市场成交额）
                all_data = db.query_data(start_date=target_date_str, end_date=target_date_str)
                
                if all_data is None or all_data.empty:
                    logger.error(f"无法获取 {target_date_str} 的数据")
                    return pd.DataFrame()
                
                # 存储全市场数据（用于成交额计算）
                self.all_market_data = all_data
                
                # 过滤主板股票（用于其他分析）
                mainboard_data = all_data[
                    (~all_data['ts_code'].str.startswith('300')) &  # 排除创业板
                    (~all_data['ts_code'].str.startswith('688')) &  # 排除科创板
                    (~all_data['ts_code'].str.startswith('830')) &  # 排除北交所
                    (~all_data['ts_code'].str.startswith('430'))    # 排除北交所
                ]
                
                logger.info(f"全市场股票: {len(all_data)} 只，主板股票: {len(mainboard_data)} 只")
                self.today_data = mainboard_data
                return mainboard_data
                
        except Exception as e:
            logger.error(f"获取市场数据时出错: {e}")
            return pd.DataFrame()
    
    def analyze_market_stats(self) -> dict:
        """分析市场统计数据"""
        if self.today_data is None or self.today_data.empty:
            return {}
        
        # 主板数据用于涨跌统计
        mainboard_data = self.today_data
        
        # 基础统计（基于主板股票）
        total_stocks = len(mainboard_data)
        up_stocks = len(mainboard_data[mainboard_data['change_pct'] > 0])
        down_stocks = len(mainboard_data[mainboard_data['change_pct'] < 0])
        flat_stocks = total_stocks - up_stocks - down_stocks
        
        up_ratio = up_stocks / total_stocks * 100
        down_ratio = down_stocks / total_stocks * 100
        
        # 涨停跌停（基于主板股票）
        limit_up = mainboard_data[mainboard_data['change_pct'] >= 9.8]
        limit_down = mainboard_data[mainboard_data['change_pct'] <= -9.8]
        
        # 大涨大跌（基于主板股票）
        big_up = mainboard_data[mainboard_data['change_pct'] >= 5]
        big_down = mainboard_data[mainboard_data['change_pct'] <= -5]
        
        # 成交额（基于全市场数据）
        if hasattr(self, 'all_market_data') and self.all_market_data is not None:
            # 全市场成交额 (Tushare的amount字段单位是万元，转换为万亿)
            total_amount = self.all_market_data['amount'].sum() / 1000000000  # 万亿 (1万亿=1,000,000,000万元)
            logger.info(f"全市场成交额: {total_amount:.2f}万亿 (基于{len(self.all_market_data)}只股票)")
        else:
            # 如果没有全市场数据，使用主板数据
            total_amount = mainboard_data['amount'].sum() / 1000000000
            logger.warning(f"使用主板成交额: {total_amount:.2f}万亿")
        
        # 获取日期
        trade_date = mainboard_data.iloc[0]['trade_date']
        if hasattr(trade_date, 'strftime'):
            date_str = trade_date.strftime('%Y-%m-%d')
        else:
            date_str = str(trade_date)
        
        self.market_stats = {
            'date': date_str,
            'total_stocks': total_stocks,
            'up_stocks': up_stocks,
            'down_stocks': down_stocks,
            'flat_stocks': flat_stocks,
            'up_ratio': up_ratio,
            'down_ratio': down_ratio,
            'limit_up_count': len(limit_up),
            'limit_down_count': len(limit_down),
            'big_up_count': len(big_up),
            'big_down_count': len(big_down),
            'total_amount': total_amount,
            'limit_up_stocks': limit_up,
            'limit_down_stocks': limit_down
        }
        
        return self.market_stats
    
    def analyze_sector_performance(self) -> list:
        """分析板块表现"""
        if self.today_data is None or self.today_data.empty:
            return []
        
        try:
            with StockDatabase() as db:
                # 联合查询获取行业数据
                date_str = self.market_stats.get('date', '')
                
                sector_query = f"""
                SELECT d.ts_code, d.change_pct, d.amount, d.close,
                       s.name as stock_name, s.industry
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
                WHERE d.trade_date = '{date_str}'
                AND s.industry IS NOT NULL
                AND d.ts_code NOT LIKE '300%'
                AND d.ts_code NOT LIKE '688%'
                ORDER BY d.change_pct DESC
                """
                
                sector_data = pd.read_sql(sector_query, db.connection)
                
                if sector_data.empty:
                    return []
                
                # 按行业统计
                sector_stats = []
                
                for industry, group in sector_data.groupby('industry'):
                    if len(group) < 8:  # 行业股票数太少跳过
                        continue
                    
                    avg_pct = group['change_pct'].mean()
                    up_count = len(group[group['change_pct'] > 0])
                    total_count = len(group)
                    up_ratio = up_count / total_count * 100
                    
                    # 行业龙头
                    top_stock = group.iloc[0]
                    
                    sector_stats.append({
                        'industry': industry,
                        'avg_pct': avg_pct,
                        'up_ratio': up_ratio,
                        'stock_count': total_count,
                        'top_stock_name': top_stock['stock_name'],
                        'top_stock_pct': top_stock['change_pct']
                    })
                
                # 按平均涨幅排序
                sector_stats.sort(key=lambda x: x['avg_pct'], reverse=True)
                
                return sector_stats
                
        except Exception as e:
            logger.warning(f"分析板块表现时出错: {e}")
            return []
    
    def get_continuous_limit_up_analysis(self) -> dict:
        """分析连板梯队"""
        if not self.market_stats or self.market_stats.get('limit_up_count', 0) == 0:
            return {}
        
        try:
            target_date = self.market_stats['date']
            
            with StockDatabase() as db:
                # 获取今日涨停股票
                today_limit_up_query = f"""
                SELECT d.ts_code, s.name, s.industry, d.change_pct, d.close, d.amount
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
                WHERE d.trade_date = '{target_date}'
                AND d.change_pct >= 9.8
                AND d.ts_code NOT LIKE '300%'
                AND d.ts_code NOT LIKE '688%'
                ORDER BY d.change_pct DESC
                """
                
                today_limit_up = pd.read_sql(today_limit_up_query, db.connection)
                
                if today_limit_up.empty:
                    return {}
                
                # 分析每只股票的连板天数
                continuous_stocks = []
                
                for _, stock in today_limit_up.iterrows():
                    ts_code = stock['ts_code']
                    stock_name = stock['name'] if stock['name'] else ts_code
                    industry = stock['industry'] if stock['industry'] else '未知'
                    
                    # 查询该股票近期历史数据
                    history_query = f"""
                    SELECT trade_date, change_pct, close
                    FROM daily_data
                    WHERE ts_code = '{ts_code}'
                    AND trade_date <= '{target_date}'
                    ORDER BY trade_date DESC
                    LIMIT 10
                    """
                    
                    history_data = pd.read_sql(history_query, db.connection)
                    
                    if history_data.empty:
                        continue
                    
                    # 计算连续涨停天数
                    continuous_days = 0
                    for _, day_data in history_data.iterrows():
                        if day_data['change_pct'] >= 9.8:
                            continuous_days += 1
                        else:
                            break
                    
                    # 只记录2连板以上的股票
                    if continuous_days >= 2:
                        continuous_stocks.append({
                            'ts_code': ts_code,
                            'name': stock_name,
                            'industry': industry,
                            'continuous_days': continuous_days,
                            'close': stock['close'],
                            'change_pct': stock['change_pct'],
                            'amount_yi': stock['amount'] / 10000 if stock['amount'] else 0
                        })
                
                # 按连板天数排序
                continuous_stocks.sort(key=lambda x: x['continuous_days'], reverse=True)
                
                # 统计行业分布
                industry_counts = {}
                if today_limit_up['industry'].notna().any():
                    industry_counts = today_limit_up['industry'].value_counts().to_dict()
                
                return {
                    'continuous_stocks': continuous_stocks,
                    'industry_distribution': industry_counts,
                    'total_limit_up': len(today_limit_up)
                }
                
        except Exception as e:
            logger.warning(f"分析连板梯队时出错: {e}")
            return {}
    
    def analyze_market_sentiment_stocks(self) -> dict:
        """分析市场情绪票（高换手率+成交量放大的股票）"""
        if not self.market_stats:
            return {}
        
        try:
            target_date = self.market_stats['date']
            
            with StockDatabase() as db:
                # 获取今日所有股票数据，计算换手率和成交量倍数
                today_sentiment_query = f"""
                SELECT d.ts_code, d.trade_date, d.close, d.change_pct, d.vol, d.amount,
                       s.name as stock_name, s.industry, s.area
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
                WHERE d.trade_date = '{target_date}'
                AND d.close > 3.0
                AND d.amount > 50000
                ORDER BY d.amount DESC
                LIMIT 100
                """
                
                today_data = pd.read_sql(today_sentiment_query, db.connection)
                
                if today_data.empty:
                    return {}
                
                # 获取这些股票的历史数据（用于计算成交量倍数）
                emotion_stocks = []
                
                for _, stock in today_data.iterrows():
                    ts_code = stock['ts_code']
                    
                    # 获取该股票近期历史数据
                    history_query = f"""
                    SELECT trade_date, vol, amount, close, change_pct
                    FROM daily_data
                    WHERE ts_code = '{ts_code}'
                    AND trade_date <= '{target_date}'
                    ORDER BY trade_date DESC
                    LIMIT 10
                    """
                    
                    history_data = pd.read_sql(history_query, db.connection)
                    
                    if len(history_data) < 5:
                        continue
                    
                    # 计算成交量倍数（今日vs前5日均量）
                    today_vol = history_data.iloc[0]['vol']
                    recent_avg_vol = history_data.iloc[1:6]['vol'].mean()
                    
                    if recent_avg_vol > 0:
                        vol_ratio = today_vol / recent_avg_vol
                    else:
                        vol_ratio = 1
                    
                    # 计算换手率（简化计算：成交额/流通市值的近似）
                    today_amount = history_data.iloc[0]['amount']  # 万元
                    today_close = history_data.iloc[0]['close']
                    
                    # 简化换手率计算：成交额/(股价*假设总股本1亿)
                    # 这里做个估算，实际需要总股本数据
                    estimated_turnover = (today_amount * 10000) / (today_close * 100000000) * 100  # 百分比
                    
                    # 筛选条件：换手率>5% 且 成交量放大>2倍
                    if estimated_turnover > 5.0 and vol_ratio > 2.0:
                        
                        # 分析最近走势
                        recent_trend = self._analyze_stock_recent_trend(history_data)
                        
                        emotion_stocks.append({
                            'ts_code': ts_code,
                            'stock_name': stock.get('stock_name', '未知'),
                            'industry': stock.get('industry', '未知'),
                            'close': stock['close'],
                            'change_pct': stock['change_pct'],
                            'vol_ratio': vol_ratio,
                            'estimated_turnover': estimated_turnover,
                            'amount_yi': today_amount / 10000,
                            'recent_trend': recent_trend
                        })
                
                # 按成交量倍数排序
                emotion_stocks.sort(key=lambda x: x['vol_ratio'], reverse=True)
                
                return {
                    'emotion_stocks': emotion_stocks[:20],  # 返回前20只情绪票
                    'total_count': len(emotion_stocks)
                }
                
        except Exception as e:
            logger.warning(f"分析情绪票时出错: {e}")
            return {}
    
    def _analyze_stock_recent_trend(self, history_data: pd.DataFrame) -> dict:
        """分析单只股票的最近走势"""
        if history_data.empty or len(history_data) < 5:
            return {'trend': '数据不足', 'strength': 0}
        
        # 计算最近5日涨跌幅
        recent_5d_pct = []
        for i in range(min(5, len(history_data))):
            recent_5d_pct.append(history_data.iloc[i]['change_pct'])
        
        # 判断走势
        positive_days = sum(1 for pct in recent_5d_pct if pct > 0)
        avg_pct = sum(recent_5d_pct) / len(recent_5d_pct)
        
        if positive_days >= 4:
            trend = "连续上涨"
            strength = 90
        elif positive_days >= 3:
            trend = "多数上涨"
            strength = 70
        elif positive_days == 2:
            trend = "震荡走势"
            strength = 50
        elif positive_days == 1:
            trend = "多数下跌"
            strength = 30
        else:
            trend = "连续下跌"
            strength = 10
        
        return {
            'trend': trend,
            'strength': strength,
            'avg_pct': avg_pct,
            'positive_days': positive_days
        }
    
    
    def analyze_continuous_promotion_rate(self) -> dict:
        """分析连板晋级率"""
        if not self.market_stats:
            return {}
        
        try:
            target_date = self.market_stats['date']
            
            with StockDatabase() as db:
                # 获取前一交易日的连板股票
                prev_date_query = f"""
                SELECT DISTINCT trade_date
                FROM daily_data
                WHERE trade_date < '{target_date}'
                ORDER BY trade_date DESC
                LIMIT 1
                """
                
                prev_date_result = pd.read_sql(prev_date_query, db.connection)
                
                if prev_date_result.empty:
                    return {}
                
                prev_date = prev_date_result.iloc[0]['trade_date'].strftime('%Y-%m-%d')
                
                # 分析连板晋级情况
                promotion_analysis = {}
                
                for board_level in [2, 3, 4, 5]:  # 分析2板到5板的晋级情况
                    # 获取前一交易日该连板级别的股票
                    prev_board_query = f"""
                    SELECT d1.ts_code, d1.change_pct, s.name
                    FROM daily_data d1
                    LEFT JOIN stock_basic s ON d1.ts_code = s.ts_code
                    WHERE d1.trade_date = '{prev_date}'
                    AND d1.change_pct >= 9.8
                    AND d1.ts_code NOT LIKE '300%'
                    AND d1.ts_code NOT LIKE '688%'
                    """
                    
                    prev_board_stocks = pd.read_sql(prev_board_query, db.connection)
                    
                    if prev_board_stocks.empty:
                        continue
                    
                    # 计算每只股票的连板天数
                    qualified_stocks = []
                    
                    for _, stock in prev_board_stocks.iterrows():
                        ts_code = stock['ts_code']
                        
                        # 查询该股票的历史连板数据
                        history_query = f"""
                        SELECT trade_date, change_pct
                        FROM daily_data
                        WHERE ts_code = '{ts_code}'
                        AND trade_date <= '{prev_date}'
                        ORDER BY trade_date DESC
                        LIMIT 10
                        """
                        
                        history_data = pd.read_sql(history_query, db.connection)
                        
                        if history_data.empty:
                            continue
                        
                        # 计算连续涨停天数
                        continuous_days = 0
                        for _, day_data in history_data.iterrows():
                            if day_data['change_pct'] >= 9.8:
                                continuous_days += 1
                            else:
                                break
                        
                        if continuous_days == board_level:
                            qualified_stocks.append(ts_code)
                    
                    if not qualified_stocks:
                        continue
                    
                    # 检查这些股票今日是否继续涨停（晋级成功）
                    today_codes = "','".join(qualified_stocks)
                    today_check_query = f"""
                    SELECT ts_code, change_pct
                    FROM daily_data
                    WHERE trade_date = '{target_date}'
                    AND ts_code IN ('{today_codes}')
                    AND change_pct >= 9.8
                    """
                    
                    today_promoted = pd.read_sql(today_check_query, db.connection)
                    
                    # 计算晋级率
                    total_count = len(qualified_stocks)
                    promoted_count = len(today_promoted)
                    promotion_rate = (promoted_count / total_count * 100) if total_count > 0 else 0
                    
                    promotion_analysis[f"{board_level}板"] = {
                        'total': total_count,
                        'promoted': promoted_count,
                        'rate': promotion_rate
                    }
                
                return promotion_analysis
                
        except Exception as e:
            logger.warning(f"分析连板晋级率时出错: {e}")
            return {}
    
    
    def generate_market_review_markdown(self) -> str:
        """生成市场复盘报告"""
        # 获取数据
        market_data = self.get_latest_market_data()
        
        if market_data.empty:
            return "❌ 无法获取市场数据"
        
        # 分析数据
        market_stats = self.analyze_market_stats()
        sector_performance = self.analyze_sector_performance()
        continuous_analysis = self.get_continuous_limit_up_analysis()
        sentiment_stocks = self.analyze_market_sentiment_stocks()
        promotion_rate = self.analyze_continuous_promotion_rate()
        
        # 生成报告
        report = []
        
        # 标题
        report.append(f"# 📊 每日市场复盘 ({market_stats['date']})")
        report.append("")
        
        # 市场行情概述
        report.append("## 🎯 市场行情")
        
        up_ratio = market_stats['up_ratio']
        down_ratio = market_stats['down_ratio']
        
        if up_ratio > 70:
            market_mood = "普涨行情，市场情绪高涨"
        elif up_ratio > 50:
            market_mood = "涨跌均衡，市场情绪稳定"
        elif up_ratio > 30:
            market_mood = "涨跌分化，市场轮动明显"
        else:
            market_mood = "跌多涨少，市场情绪低迷"
        
        report.append(f"{market_mood}，主板股票上涨{market_stats['up_stocks']}只({up_ratio:.1f}%)，"
                     f"下跌{market_stats['down_stocks']}只({down_ratio:.1f}%)，"
                     f"平盘{market_stats['flat_stocks']}只。")
        
        report.append(f"全市场成交额{market_stats['total_amount']:.2f}万亿，"
                     f"主板涨停家数{market_stats['limit_up_count']}只。")
        
        # 连板梯队分析
        if market_stats['limit_up_count'] > 0 and continuous_analysis:
            report.append("")
            report.append("## 🔥 连板梯队")
            
            continuous_stocks = continuous_analysis.get('continuous_stocks', [])
            industry_dist = continuous_analysis.get('industry_distribution', {})
            total_limit_up = continuous_analysis.get('total_limit_up', 0)
            
            # 涨停板块分布
            if industry_dist:
                report.append("")
                report.append("**涨停板块分布：**")
                sorted_industries = sorted(industry_dist.items(), key=lambda x: x[1], reverse=True)
                for industry, count in sorted_industries[:8]:
                    if industry and industry != '未知':
                        report.append(f"- **{industry}**({count}只)")
            
            # 连板梯队
            if continuous_stocks:
                report.append("")
                report.append("**连板梯队：**")
                
                # 按连板天数分组
                board_groups = {}
                for stock in continuous_stocks:
                    days = stock['continuous_days']
                    if days not in board_groups:
                        board_groups[days] = []
                    board_groups[days].append(stock)
                
                # 按连板天数倒序显示
                for days in sorted(board_groups.keys(), reverse=True):
                    stocks_in_group = board_groups[days]
                    
                    if days >= 5:  # 5板以上单独显示
                        report.append(f"**{days}板：**")
                        for stock in stocks_in_group:
                            industry_info = f"（{stock['industry']}）" if stock['industry'] != '未知' else ""
                            report.append(f"- {stock['name']}{industry_info}")
                    else:  # 2-4板合并显示
                        stock_infos = []
                        for stock in stocks_in_group:
                            industry_info = f"（{stock['industry']}）" if stock['industry'] != '未知' else ""
                            stock_infos.append(f"{stock['name']}{industry_info}")
                        
                        if stock_infos:
                            report.append(f"**{days}板：**{', '.join(stock_infos[:10])}")  # 最多显示10只
                            if len(stock_infos) > 10:
                                report.append(f"等{len(stock_infos)}只")
                
                # 连板梯队总结
                max_boards = max([s['continuous_days'] for s in continuous_stocks])
                total_continuous = len(continuous_stocks)
                report.append("")
                report.append(f"连板梯队来看：最高{max_boards}板，连板股{total_continuous}只，"
                             f"涨停总数{total_limit_up}只。")
                
                # 连板晋级率分析
                if promotion_rate:
                    report.append("")
                    report.append("**连板晋级率：**")
                    for level, data in promotion_rate.items():
                        if data['total'] > 0:
                            report.append(f"- {level}晋级：{data['promoted']}/{data['total']}只 "
                                         f"({data['rate']:.0f}%)")
            else:
                report.append("今日无连板梯队。")
        
        # 情绪票分析
        if sentiment_stocks and sentiment_stocks.get('emotion_stocks'):
            report.append("")
            report.append("## 📊 情绪票分析")
            
            emotion_stocks_list = sentiment_stocks['emotion_stocks']
            total_emotion_count = sentiment_stocks['total_count']
            
            report.append("")
            report.append(f"**筛选出 {total_emotion_count} 只情绪票（高换手+放量）**")
            
            # 按走势分类统计
            trend_stats = {}
            for stock in emotion_stocks_list:
                trend = stock['recent_trend']['trend']
                if trend not in trend_stats:
                    trend_stats[trend] = []
                trend_stats[trend].append(stock)
            
            # 显示走势分布
            report.append("")
            report.append("**走势分布：**")
            for trend, stocks in trend_stats.items():
                report.append(f"- {trend}：{len(stocks)}只")
            
            # 重点关注TOP10情绪票
            report.append("")
            report.append("**重点情绪票TOP10：**")
            
            for i, stock in enumerate(emotion_stocks_list[:10], 1):
                code = stock['ts_code'].split('.')[0]
                trend_info = stock['recent_trend']
                
                # 走势标识
                if trend_info['strength'] >= 80:
                    trend_emoji = "🔥"
                elif trend_info['strength'] >= 60:
                    trend_emoji = "📈"
                elif trend_info['strength'] >= 40:
                    trend_emoji = "➡️"
                else:
                    trend_emoji = "📉"
                
                report.append(f"{i:>2}. **{stock['stock_name']}**({code}) - {stock['industry']}")
                report.append(f"     {trend_emoji} {trend_info['trend']}：5日均{trend_info['avg_pct']:+.1f}%")
                report.append(f"     📊 今日：{stock['change_pct']:+.1f}%，放量{stock['vol_ratio']:.1f}倍，"
                             f"换手{stock['estimated_turnover']:.1f}%，成交{stock['amount_yi']:.1f}亿")
            
            # 情绪票板块分析
            if emotion_stocks_list:
                industry_stats = {}
                for stock in emotion_stocks_list:
                    industry = stock['industry']
                    if industry != '未知':
                        if industry not in industry_stats:
                            industry_stats[industry] = {'count': 0, 'avg_vol_ratio': 0, 'stocks': []}
                        industry_stats[industry]['count'] += 1
                        industry_stats[industry]['stocks'].append(stock)
                
                # 计算平均放量倍数
                for industry, data in industry_stats.items():
                    avg_vol = sum(s['vol_ratio'] for s in data['stocks']) / len(data['stocks'])
                    industry_stats[industry]['avg_vol_ratio'] = avg_vol
                
                # 按情绪票数量排序
                sorted_industries = sorted(industry_stats.items(), key=lambda x: x[1]['count'], reverse=True)
                
                if sorted_industries:
                    report.append("")
                    report.append("**情绪票板块分布：**")
                    for industry, data in sorted_industries[:6]:
                        report.append(f"- **{industry}**：{data['count']}只，平均放量{data['avg_vol_ratio']:.1f}倍")
            
            # 情绪强度总结
            if emotion_stocks_list:
                avg_vol_ratio = sum(s['vol_ratio'] for s in emotion_stocks_list) / len(emotion_stocks_list)
                avg_turnover = sum(s['estimated_turnover'] for s in emotion_stocks_list) / len(emotion_stocks_list)
                
                report.append("")
                if total_emotion_count >= 30:
                    emotion_level = "情绪亢奋"
                elif total_emotion_count >= 20:
                    emotion_level = "情绪活跃"
                elif total_emotion_count >= 10:
                    emotion_level = "情绪一般"
                else:
                    emotion_level = "情绪低迷"
                
                report.append(f"**情绪强度**: {emotion_level}（筛选出{total_emotion_count}只情绪票）")
                report.append(f"平均放量{avg_vol_ratio:.1f}倍，平均换手{avg_turnover:.1f}%")
                
                # 给出操作建议
                if total_emotion_count >= 25:
                    report.append("市场情绪较为亢奋，注意追高风险，建议轻仓试探。")
                elif total_emotion_count >= 15:
                    report.append("市场情绪适中，可关注强势品种，控制仓位。")
                else:
                    report.append("市场情绪一般，建议观望为主，等待情绪回暖。")
        
        # 板块表现
        if sector_performance:
            report.append("")
            report.append("## 🏢 板块表现")
            
            report.append("")
            report.append("### 📈 强势板块TOP8")
            for i, sector in enumerate(sector_performance[:8], 1):
                report.append(f"{i}. **{sector['industry']}**：领涨{sector['top_stock_name']}({sector['top_stock_pct']:+.1f}%)，"
                             f"板块平均{sector['avg_pct']:+.1f}%，上涨比例{sector['up_ratio']:.0f}%")
            
            if len(sector_performance) > 8:
                report.append("")
                report.append("### 📉 弱势板块")
                for sector in sector_performance[-3:]:
                    report.append(f"- **{sector['industry']}**：平均{sector['avg_pct']:+.1f}%，"
                                 f"上涨比例{sector['up_ratio']:.0f}%")
        
        
        
        # 市场总结
        report.append("")
        report.append("## 📝 市场总结")
        
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
        
        limit_up_count = market_stats['limit_up_count']
        if limit_up_count > 30:
            report.append("涨停家数较多，赚钱效应良好，可关注次日连板机会。")
        elif limit_up_count > 10:
            report.append("涨停家数一般，市场活跃度适中。")
        else:
            report.append("涨停家数较少，市场缺乏热点。")
        
        report.append("")
        report.append("**操作策略**：")
        
        if sector_performance:
            top_sector = sector_performance[0]['industry']
            report.append(f"- 重点关注 **{top_sector}** 等强势板块")
        
        report.append("- 严格止损，控制单股仓位")
        report.append("- 根据市场情绪调整仓位大小")
        
        report.append("")
        report.append("---")
        report.append("*数据来源：A股实时数据（成交额为全市场，其他指标为主板股票）*")
        report.append("*分析工具：基于选手操作模式的量化策略*")
        
        return "\n".join(report)
    
    
    def send_review_notification(self, content: str) -> bool:
        """发送复盘通知"""
        try:
            # 尝试导入消息发送模块
            try:
                from send_msg import send_markdown_message
                
                # 截取前2000字符用于推送（避免消息过长）
                short_content = content[:2000] + "..." if len(content) > 2000 else content
                
                result = send_markdown_message(short_content)
                if result:
                    logger.info("✅ 每日复盘报告已推送")
                    return True
                else:
                    logger.warning("复盘报告推送失败")
                    return False
                    
            except ImportError:
                logger.info("未配置消息推送模块，跳过推送")
                return True
                
        except Exception as e:
            logger.error(f"推送复盘报告时出错: {e}")
            return False


def run_daily_market_review(notify: bool = False) -> str:
    """运行每日市场复盘"""
    try:
        logger.info("🚀 开始每日市场复盘分析...")
        
        reviewer = DailyMarketReviewer()
        
        # 生成复盘报告
        review_content = reviewer.generate_market_review_markdown()
        
        # 发送通知
        if notify:
            reviewer.send_review_notification(review_content)
        
        # 输出到控制台
        print(review_content)
        
        logger.info("✅ 每日市场复盘完成")
        
        return review_content
        
    except Exception as e:
        logger.error(f"每日市场复盘时出错: {e}")
        return "❌ 复盘分析失败"


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='每日市场复盘工具')
    parser.add_argument('--notify', action='store_true', help='发送推送通知')
    parser.add_argument('--output', help='输出文件路径')
    
    args = parser.parse_args()
    
    # 运行复盘分析
    review_content = run_daily_market_review(notify=args.notify)
    
    # 额外保存到指定路径
    if args.output:
        try:
            with open(args.output, 'w', encoding='utf-8') as f:
                f.write(review_content)
            logger.info(f"复盘报告已保存到 {args.output}")
        except Exception as e:
            logger.error(f"保存到指定路径时出错: {e}")


if __name__ == "__main__":
    main()
