#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
强势板块回调回踩10日线策略推送脚本
策略：强势板块 + 主板股票 + 回调后回踩10日线
筛选条件：
1. 主板股票：排除创业板、科创板等
2. 强势板块：该行业近5日平均涨幅排名前30%
3. 个股回调：从近10日高点回调5-20%
4. 回踩10日线：当前价格在10日均线上下3%范围内
5. 趋势完好：10日线 > 20日线，保持上升趋势
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import defaultdict

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('strong_sector_pullback_notify.log', encoding='utf-8'),
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


def calculate_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    计算技术指标
    
    Args:
        df: 股票历史数据（按日期倒序）
        
    Returns:
        pd.DataFrame: 带技术指标的数据
    """
    if df.empty or len(df) < 10:
        return df
    
    # 确保按日期正序排列用于计算
    df_calc = df.copy().sort_values('trade_date').reset_index(drop=True)
    
    # 计算移动平均线
    df_calc['ma5'] = df_calc['close'].rolling(window=5, min_periods=1).mean()
    df_calc['ma10'] = df_calc['close'].rolling(window=10, min_periods=1).mean()
    df_calc['ma20'] = df_calc['close'].rolling(window=20, min_periods=1).mean()
    
    # 计算涨幅
    df_calc['pct_1d'] = df_calc['close'].pct_change() * 100
    df_calc['pct_5d'] = df_calc['close'].pct_change(periods=5) * 100
    df_calc['pct_10d'] = df_calc['close'].pct_change(periods=10) * 100
    
    # 计算10日内最高价和最低价
    df_calc['high_10d'] = df_calc['high'].rolling(window=10, min_periods=1).max()
    df_calc['low_10d'] = df_calc['low'].rolling(window=10, min_periods=1).min()
    
    # 按原序列返回（最新日期在前）
    result_df = df_calc.sort_values('trade_date', ascending=False).reset_index(drop=True)
    
    return result_df


def calculate_sector_strength(all_stocks_df: pd.DataFrame, db_connection, days: int = 5) -> tuple:
    """
    计算各行业板块和概念板块的强势程度
    
    Args:
        all_stocks_df: 所有股票数据
        db_connection: 数据库连接
        days: 计算强势程度的天数
        
    Returns:
        tuple: (行业强势字典, 概念强势字典)
    """
    # 1. 计算行业板块强势
    industry_strength = {}
    
    # 只使用最近几天的数据计算板块强势
    recent_data = all_stocks_df.head(days * 4000)
    
    # 按行业分组计算平均涨幅
    industry_performance = defaultdict(list)
    
    for _, row in recent_data.iterrows():
        industry = row.get('industry')
        if industry and industry != '未知' and not pd.isna(row.get('pct_1d')):
            industry_performance[industry].append(row['pct_1d'])
    
    # 计算各行业的平均涨幅
    for industry, pct_list in industry_performance.items():
        if len(pct_list) >= 5:  # 至少要有5只股票
            avg_pct = np.mean(pct_list)
            stock_count = len(pct_list)
            strength_score = avg_pct + (stock_count / 100) * 2
            industry_strength[industry] = {
                'avg_pct': avg_pct,
                'stock_count': stock_count,
                'strength_score': strength_score,
                'type': '行业'
            }
    
    # 2. 计算概念板块强势
    concept_strength = {}
    
    try:
        # 获取概念指数的成分股和当日涨幅（修复is_new字段问题）
        concept_query = """
        SELECT tm.ts_code as concept_code, ti.name as concept_name, 
               tm.con_code as stock_code, d.change_pct as pct_1d
        FROM ths_member tm
        JOIN ths_index ti ON tm.ts_code = ti.ts_code  
        JOIN daily_data d ON tm.con_code = d.ts_code
        WHERE ti.type IN ('N', 'TH')  -- N-概念指数 TH-同花顺主题指数
        AND d.trade_date = (SELECT MAX(trade_date) FROM daily_data)
        AND tm.con_code NOT LIKE '300%%'  -- 只要主板股票
        AND tm.con_code NOT LIKE '688%%'
        AND tm.con_code NOT LIKE '830%%'
        AND tm.con_code NOT LIKE '430%%'
        """
        
        concept_df = pd.read_sql(concept_query, db_connection)
        
        if not concept_df.empty:
            # 按概念分组计算平均涨幅
            concept_performance = defaultdict(list)
            
            for _, row in concept_df.iterrows():
                concept_name = row['concept_name']
                if concept_name and not pd.isna(row['pct_1d']):
                    concept_performance[concept_name].append(row['pct_1d'])
            
            # 计算各概念的强势程度
            for concept, pct_list in concept_performance.items():
                if len(pct_list) >= 3:  # 概念股至少3只
                    avg_pct = np.mean(pct_list)
                    stock_count = len(pct_list)
                    strength_score = avg_pct + (stock_count / 50) * 2  # 概念股数量权重调整
                    concept_strength[concept] = {
                        'avg_pct': avg_pct,
                        'stock_count': stock_count,
                        'strength_score': strength_score,
                        'type': '概念'
                    }
            
            logger.info(f"计算了 {len(concept_strength)} 个概念板块的强势程度")
    
    except Exception as e:
        logger.warning(f"计算概念板块强势时出错: {e}")
    
    # 合并并排序所有板块
    all_sectors = {**industry_strength, **concept_strength}
    sorted_sectors = sorted(all_sectors.items(), 
                          key=lambda x: x[1]['strength_score'], 
                          reverse=True)
    
    logger.info(f"总计算了 {len(industry_strength)} 个行业 + {len(concept_strength)} 个概念 = {len(all_sectors)} 个板块")
    logger.info("前10强势板块:")
    for i, (sector, stats) in enumerate(sorted_sectors[:10], 1):
        logger.info(f"  {i}. {stats['type']}-{sector}: 平均涨幅{stats['avg_pct']:.1f}%, "
                   f"股票数{stats['stock_count']}, 强势评分{stats['strength_score']:.1f}")
    
    return dict(sorted_sectors), concept_strength


def get_all_stock_concepts(all_stock_codes: list, db_connection) -> dict:
    """
    批量获取所有股票的概念归属
    
    Args:
        all_stock_codes: 股票代码列表
        db_connection: 数据库连接
        
    Returns:
        dict: {股票代码: [概念列表]}
    """
    stock_concepts = defaultdict(list)
    
    try:
        # 批量查询所有股票的概念归属
        if all_stock_codes:
            codes_str = "','".join(all_stock_codes)
            concept_member_query = f"""
            SELECT tm.con_code as stock_code, ti.name as concept_name, tm.ts_code as concept_code
            FROM ths_member tm
            JOIN ths_index ti ON tm.ts_code = ti.ts_code
            WHERE tm.con_code IN ('{codes_str}')
            AND ti.type IN ('N', 'TH')
            """
            
            concept_df = pd.read_sql(concept_member_query, db_connection)
            
            for _, row in concept_df.iterrows():
                stock_code = row['stock_code']
                concept_name = row['concept_name']
                if concept_name:
                    stock_concepts[stock_code].append(concept_name)
            
            logger.info(f"批量获取了 {len(stock_concepts)} 只股票的概念归属信息")
    
    except Exception as e:
        logger.warning(f"批量获取概念信息时出错: {e}")
    
    return dict(stock_concepts)


def check_strong_sector_or_concept(ts_code: str, industry: str, strong_sectors: dict, 
                                  stock_concepts: dict, top_pct: float = 0.3) -> tuple:
    """
    检查是否属于强势板块（行业或概念）
    
    Args:
        ts_code: 股票代码
        industry: 行业名称
        strong_sectors: 强势板块字典（包含行业和概念）
        stock_concepts: 股票概念归属字典
        top_pct: 强势板块比例阈值（前30%）
        
    Returns:
        tuple: (是否强势, 所属强势板块列表)
    """
    strong_memberships = []
    
    # 1. 检查行业板块
    if industry and industry != '未知' and industry in strong_sectors:
        total_sectors = len(strong_sectors)
        sector_rank = list(strong_sectors.keys()).index(industry) + 1
        
        if sector_rank <= total_sectors * top_pct:
            strong_memberships.append({
                'name': industry,
                'type': '行业',
                'rank': sector_rank,
                'strength_score': strong_sectors[industry]['strength_score'],
                'avg_pct': strong_sectors[industry]['avg_pct']
            })
    
    # 2. 检查概念板块
    if ts_code in stock_concepts:
        for concept_name in stock_concepts[ts_code]:
            if concept_name in strong_sectors:
                total_sectors = len(strong_sectors)
                sector_rank = list(strong_sectors.keys()).index(concept_name) + 1
                
                if sector_rank <= total_sectors * top_pct:
                    strong_memberships.append({
                        'name': concept_name,
                        'type': '概念',
                        'rank': sector_rank,
                        'strength_score': strong_sectors[concept_name]['strength_score'],
                        'avg_pct': strong_sectors[concept_name]['avg_pct']
                    })
    
    # 返回是否有强势板块归属
    is_strong = len(strong_memberships) > 0
    return is_strong, strong_memberships


def check_pullback_to_ma10(row: pd.Series, ma10_tolerance: float = 0.03) -> bool:
    """
    检查是否回踩10日线
    
    Args:
        row: 股票数据行
        ma10_tolerance: 10日线容忍度（3%）
        
    Returns:
        bool: 是否满足回踩10日线条件
    """
    close = row['close']
    ma10 = row['ma10']
    
    if pd.isna(close) or pd.isna(ma10) or ma10 <= 0:
        return False
    
    # 价格在10日线上下3%范围内
    distance_pct = abs(close - ma10) / ma10
    is_near_ma10 = distance_pct <= ma10_tolerance
    
    # 价格在10日线之上（轻微突破也算）
    is_above_ma10 = close >= ma10 * (1 - ma10_tolerance)
    
    return is_near_ma10 and is_above_ma10


def check_recent_pullback(row: pd.Series, min_pullback: float = 0.05, max_pullback: float = 0.25) -> bool:
    """
    检查是否存在合理的回调
    
    Args:
        row: 股票数据行
        min_pullback: 最小回调幅度（5%）
        max_pullback: 最大回调幅度（25%）
        
    Returns:
        bool: 是否满足回调条件
    """
    close = row['close']
    high_10d = row['high_10d']
    
    if pd.isna(close) or pd.isna(high_10d) or high_10d <= 0:
        return False
    
    # 计算从10日高点的回调幅度
    pullback_pct = (high_10d - close) / high_10d
    
    return min_pullback <= pullback_pct <= max_pullback


def check_upward_trend(row: pd.Series) -> bool:
    """
    检查趋势是否依然向上
    
    Args:
        row: 股票数据行
        
    Returns:
        bool: 是否满足向上趋势条件
    """
    ma10 = row['ma10']
    ma20 = row['ma20']
    
    if pd.isna(ma10) or pd.isna(ma20):
        return False
    
    # 10日线在20日线之上，保持上升趋势
    return ma10 > ma20


def calculate_pullback_signal_strength(row: pd.Series, sector_strength: dict) -> float:
    """
    计算回踩信号强度评分（0-100）
    
    Args:
        row: 股票数据行
        sector_strength: 板块强势字典
        
    Returns:
        float: 信号强度评分
    """
    score = 0.0
    
    # 板块强势评分（0-30分）
    industry = row.get('industry', '未知')
    if industry in sector_strength:
        sector_rank = list(sector_strength.keys()).index(industry) + 1
        total_sectors = len(sector_strength)
        # 排名越靠前分数越高
        sector_score = max(0, 30 - (sector_rank / total_sectors) * 30)
        score += sector_score
    
    # 回调合理性评分（0-25分）
    close = row['close']
    high_10d = row['high_10d']
    if not pd.isna(close) and not pd.isna(high_10d) and high_10d > 0:
        pullback_pct = (high_10d - close) / high_10d
        # 5-15%回调得满分，过小或过大都减分
        if 0.05 <= pullback_pct <= 0.15:
            score += 25
        elif 0.03 <= pullback_pct <= 0.25:
            score += 20
        else:
            score += 10
    
    # 10日线位置评分（0-20分）
    ma10 = row['ma10']
    if not pd.isna(close) and not pd.isna(ma10) and ma10 > 0:
        distance_to_ma10 = abs(close - ma10) / ma10
        # 越接近10日线分数越高
        if distance_to_ma10 <= 0.01:  # 1%以内
            score += 20
        elif distance_to_ma10 <= 0.03:  # 3%以内
            score += 15
        elif distance_to_ma10 <= 0.05:  # 5%以内
            score += 10
        else:
            score += 5
    
    # 趋势保持评分（0-25分）
    ma10 = row['ma10']
    ma20 = row['ma20']
    if not pd.isna(ma10) and not pd.isna(ma20) and ma20 > 0:
        ma_ratio = ma10 / ma20
        if ma_ratio >= 1.05:  # 10日线比20日线高5%以上
            score += 25
        elif ma_ratio >= 1.02:  # 高2%以上
            score += 20
        elif ma_ratio >= 1.00:  # 略高
            score += 15
        else:
            score += 5
    
    return min(100.0, score)


def find_strong_sector_pullback_stocks(days_back: int = 15,
                                      min_pullback: float = 0.05,
                                      max_pullback: float = 0.25,
                                      ma10_tolerance: float = 0.03,
                                      min_signal_strength: float = 70.0,
                                      strong_sector_pct: float = 0.3) -> pd.DataFrame:
    """
    查找符合强势板块回调回踩10日线条件的股票
    
    Args:
        days_back: 查询历史数据天数
        min_pullback: 最小回调幅度
        max_pullback: 最大回调幅度
        ma10_tolerance: 10日线容忍度
        min_signal_strength: 最小信号强度
        strong_sector_pct: 强势板块比例（前30%）
        
    Returns:
        pd.DataFrame: 符合条件的股票
    """
    logger.info("开始筛选强势板块回调回踩10日线股票...")
    
    with StockDatabase() as db:
        # 获取数据库中的最新交易日期
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            logger.warning("数据库中没有数据")
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        if hasattr(latest_trade_date, 'strftime'):
            end_date = latest_trade_date.strftime('%Y-%m-%d')
        else:
            end_date = str(latest_trade_date)
        
        # 从最新交易日往前推25天（确保包含足够的工作日）
        from datetime import datetime, timedelta
        latest_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = latest_dt - timedelta(days=25)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        # 联表查询主板股票数据，包含股票名称和行业信息
        logger.info(f"查询日期范围: {start_date} 到 {end_date} (仅主板股票)")
        
        main_board_query = """
        SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close,
               d.change_pct, d.change_amount, d.vol, d.amount, d.created_at, d.updated_at,
               s.name as stock_name, s.industry, s.area
        FROM daily_data d
        LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
        WHERE d.trade_date >= %s AND d.trade_date <= %s
        AND d.ts_code NOT LIKE '300%%'
        AND d.ts_code NOT LIKE '688%%'
        AND d.ts_code NOT LIKE '830%%'
        AND d.ts_code NOT LIKE '430%%'
        AND d.ts_code NOT LIKE '200%%'
        AND d.ts_code NOT LIKE '900%%'
        ORDER BY d.ts_code, d.trade_date DESC
        """
        
        import pandas as pd
        all_stocks_df = pd.read_sql(main_board_query, db.connection, params=[start_date, end_date])
        
        if all_stocks_df is None or all_stocks_df.empty:
            logger.warning("没有获取到主板股票数据")
            return pd.DataFrame()
        
        logger.info(f"查询到 {len(all_stocks_df)} 条主板股票记录")
        
        # 计算板块强势程度（行业+概念）
        strong_sectors, concept_sectors = calculate_sector_strength(all_stocks_df, db.connection, days=5)
        
        # 批量获取所有股票的概念归属，避免逐一查询
        all_stock_codes = all_stocks_df['ts_code'].unique().tolist()
        stock_concepts = get_all_stock_concepts(all_stock_codes, db.connection)
        
        # 按股票分组处理
        qualified_stocks = []
        total_stocks = all_stocks_df['ts_code'].nunique()
        processed_count = 0
        
        debug_stats = {
            'total_stocks': 0,
            'insufficient_data': 0,
            'low_price_or_st': 0,
            'weak_sector': 0,
            'no_pullback': 0,
            'not_near_ma10': 0,
            'trend_broken': 0,
            'low_signal_strength': 0,
            'qualified': 0
        }
        
        logger.info(f"开始处理 {total_stocks} 只主板股票...")
        
        for ts_code, stock_df in all_stocks_df.groupby('ts_code'):
            try:
                debug_stats['total_stocks'] += 1
                processed_count += 1
                
                # 显示处理进度
                if processed_count % 1000 == 0:
                    logger.info(f"处理进度: {processed_count}/{total_stocks} ({processed_count/total_stocks*100:.1f}%)")
                
                # 确保有足够的数据
                if len(stock_df) < 10:
                    debug_stats['insufficient_data'] += 1
                    continue
                
                # 计算技术指标
                stock_df_with_indicators = calculate_technical_indicators(stock_df)
                
                # 获取最新数据
                latest_row = stock_df_with_indicators.iloc[0]
                
                # 过滤掉价格过低或ST股票
                if latest_row['close'] < 3.0 or 'ST' in ts_code:
                    debug_stats['low_price_or_st'] += 1
                    continue
                
                # 检查是否属于强势板块（行业或概念）
                industry = latest_row.get('industry', '未知')
                is_strong, strong_memberships = check_strong_sector_or_concept(
                    ts_code, industry, strong_sectors, stock_concepts, strong_sector_pct
                )
                
                if not is_strong:
                    debug_stats['weak_sector'] += 1
                    continue
                
                # 检查是否有合理回调
                if not check_recent_pullback(latest_row, min_pullback, max_pullback):
                    debug_stats['no_pullback'] += 1
                    continue
                
                # 检查是否回踩10日线
                if not check_pullback_to_ma10(latest_row, ma10_tolerance):
                    debug_stats['not_near_ma10'] += 1
                    continue
                
                # 检查趋势是否完好
                if not check_upward_trend(latest_row):
                    debug_stats['trend_broken'] += 1
                    continue
                
                # 计算信号强度
                signal_strength = calculate_pullback_signal_strength(latest_row, strong_sectors)
                
                if signal_strength < min_signal_strength:
                    debug_stats['low_signal_strength'] += 1
                    continue
                
                # 满足所有条件
                debug_stats['qualified'] += 1
                
                # 计算附加信息
                pullback_pct = ((latest_row['high_10d'] - latest_row['close']) / latest_row['high_10d'] 
                              if latest_row['high_10d'] > 0 else 0)
                distance_to_ma10 = abs(latest_row['close'] - latest_row['ma10']) / latest_row['ma10'] if latest_row['ma10'] > 0 else 0
                
                # 找出最强的板块归属（排名最高的）
                best_membership = min(strong_memberships, key=lambda x: x['rank']) if strong_memberships else None
                
                qualified_stocks.append({
                    'ts_code': ts_code,
                    'stock_name': latest_row.get('stock_name', '未知'),
                    'industry': industry,
                    'area': latest_row.get('area', '未知'),
                    'trade_date': latest_row['trade_date'],
                    'close': latest_row['close'],
                    'pct_1d': latest_row['pct_1d'],
                    'pct_5d': latest_row['pct_5d'],
                    'pct_10d': latest_row['pct_10d'],
                    'high_10d': latest_row['high_10d'],
                    'pullback_pct': pullback_pct * 100,  # 回调幅度
                    'distance_to_ma10_pct': distance_to_ma10 * 100,  # 距离10日线距离
                    'ma10': latest_row['ma10'],
                    'ma20': latest_row['ma20'],
                    'vol': latest_row['vol'],
                    'amount_yi': latest_row['amount'] / 10000,
                    'signal_strength': signal_strength,
                    'strong_sectors': strong_memberships,  # 所有强势板块归属
                    'best_sector_name': best_membership['name'] if best_membership else industry,
                    'best_sector_type': best_membership['type'] if best_membership else '行业',
                    'best_sector_rank': best_membership['rank'] if best_membership else 999,
                    'best_sector_pct': best_membership['avg_pct'] if best_membership else 0
                })
                
            except Exception as e:
                logger.debug(f"处理股票 {ts_code} 时出错: {e}")
                continue
        
        # 输出调试统计信息
        logger.info("📊 筛选统计:")
        logger.info(f"   总股票数: {debug_stats['total_stocks']}")
        logger.info(f"   数据不足: {debug_stats['insufficient_data']}")
        logger.info(f"   价格/ST过滤: {debug_stats['low_price_or_st']}")
        logger.info(f"   非强势板块: {debug_stats['weak_sector']}")
        logger.info(f"   无回调: {debug_stats['no_pullback']}")
        logger.info(f"   未回踩10日线: {debug_stats['not_near_ma10']}")
        logger.info(f"   趋势破坏: {debug_stats['trend_broken']}")
        logger.info(f"   信号强度不足: {debug_stats['low_signal_strength']}")
        logger.info(f"   ✅ 最终合格: {debug_stats['qualified']}")
        
        result_df = pd.DataFrame(qualified_stocks)
        
        if not result_df.empty:
            # 按信号强度排序
            result_df = result_df.sort_values('signal_strength', ascending=False)
            logger.info(f"找到 {len(result_df)} 只符合强势板块回调回踩10日线条件的股票")
        else:
            logger.info("没有找到符合条件的股票")
        
        return result_df


def create_strong_sector_pullback_markdown(df: pd.DataFrame, query_date: str, strong_sectors: dict) -> str:
    """
    创建强势板块回调回踩10日线的markdown格式消息
    
    Args:
        df: 符合条件的股票数据
        query_date: 查询日期
        strong_sectors: 强势板块信息
        
    Returns:
        str: markdown格式的消息内容
    """
    if df.empty:
        return f"""## 📈 强势板块回调回踩10日线提醒 ({query_date})

❌ **暂无符合条件的股票**

**筛选条件：**
- 🏢 强势板块：行业排名前30%
- 📉 合理回调：从10日高点回调5-25%
- 📊 回踩10日线：价格在10日线上下3%
- 📈 趋势完好：MA10 > MA20

建议关注强势板块的调整机会。
"""
    
    # 统计信息
    total_count = len(df)
    avg_signal_strength = df['signal_strength'].mean()
    avg_pullback = df['pullback_pct'].mean()
    avg_distance_ma10 = df['distance_to_ma10_pct'].mean()
    
    # 行业分布统计
    industry_stats = df['industry'].value_counts().head(5)
    hot_sectors = []
    for industry, count in industry_stats.items():
        if industry and industry != '未知':
            sector_rank = df[df['industry'] == industry]['sector_rank'].iloc[0]
            hot_sectors.append(f"{industry}(排名{sector_rank}, {count}只)")
    
    # 强势板块TOP5
    top_sectors = []
    for i, (industry, stats) in enumerate(list(strong_sectors.items())[:5], 1):
        top_sectors.append(f"{i}.{industry}({stats['avg_pct']:+.1f}%)")
    
    markdown = f"""## 📈 强势板块回调回踩10日线提醒 ({query_date})

🎯 **筛选结果：找到 {total_count} 只符合条件的主板股票**
- 📊 平均信号强度：{avg_signal_strength:.1f}分
- 📉 平均回调幅度：{avg_pullback:.1f}%
- 📏 距10日线距离：{avg_distance_ma10:.1f}%
- 🏢 涉及板块：{' | '.join(hot_sectors[:3])}

### 🔥 当前强势板块TOP5
{' | '.join(top_sectors)}

---

### 🏆 重点关注股票（按信号强度排序）

"""
    
    # 显示前10只股票
    for i, (_, row) in enumerate(df.head(10).iterrows(), 1):
        code = format_stock_code(row['ts_code'])
        
        markdown += f"""
**{i}. {row['stock_name']} ({code})**
- 🏢 强势归属：{row['best_sector_type']}-{row['best_sector_name']} (排名第{row['best_sector_rank']}, {row['best_sector_pct']:+.1f}%)
- 💰 价格：{row['close']:.2f}元 ({row['pct_1d']:+.1f}%)
- 📈 短期涨幅：5日{row['pct_5d']:+.1f}% | 10日{row['pct_10d']:+.1f}%
- 📉 回调幅度：从10日高点{row['high_10d']:.2f}元回调{row['pullback_pct']:.1f}%
- 📊 10日线位置：距MA10({row['ma10']:.2f}){row['distance_to_ma10_pct']:+.1f}%
- 🎯 信号强度：{row['signal_strength']:.0f}分
- 💸 成交额：{row['amount_yi']:.1f}亿元
- 📉 均线趋势：MA10({row['ma10']:.2f}) > MA20({row['ma20']:.2f})
"""
    
    if total_count > 10:
        markdown += f"\\n... 还有 {total_count - 10} 只股票符合条件"
    
    markdown += f"""

---

### 📋 策略说明
**强势板块回调回踩10日线策略四大要素：**
1. 🏢 **强势板块**：行业近5日平均涨幅排名前30%
2. 📉 **合理回调**：从10日高点回调5-25%
3. 📊 **回踩10日线**：价格在10日线上下3%范围
4. 📈 **趋势完好**：MA10 > MA20，保持上升趋势

**投资逻辑：**
- 强势板块中的回调为低吸机会
- 10日线是重要技术支撑位
- 回调后的反弹概率较高

**风险提示：**
- 需确认板块强势逻辑是否持续
- 注意整体市场环境变化
- 设置合理止损位

*数据来源：基于最新交易日数据计算*
"""
    
    return markdown


def run_strong_sector_pullback_strategy(notify: bool = True,
                                       min_pullback: float = 0.05,
                                       max_pullback: float = 0.25,
                                       ma10_tolerance: float = 0.03,
                                       min_signal_strength: float = 70.0,
                                       strong_sector_pct: float = 0.3) -> pd.DataFrame:
    """
    运行强势板块回调回踩10日线策略
    
    Args:
        notify: 是否发送通知
        min_pullback: 最小回调幅度
        max_pullback: 最大回调幅度
        ma10_tolerance: 10日线容忍度
        min_signal_strength: 最小信号强度
        strong_sector_pct: 强势板块比例
        
    Returns:
        pd.DataFrame: 符合条件的股票
    """
    try:
        logger.info("🚀 开始执行强势板块回调回踩10日线策略...")
        
        # 查找符合条件的股票
        result_df = find_strong_sector_pullback_stocks(
            min_pullback=min_pullback,
            max_pullback=max_pullback,
            ma10_tolerance=ma10_tolerance,
            min_signal_strength=min_signal_strength,
            strong_sector_pct=strong_sector_pct
        )
        
        # 获取查询日期
        if not result_df.empty:
            latest_date = result_df.iloc[0]['trade_date']
            if hasattr(latest_date, 'strftime'):
                query_date = latest_date.strftime('%Y-%m-%d')
            else:
                query_date = str(latest_date)
        else:
            query_date = datetime.now().strftime('%Y-%m-%d')
        
        # 重新计算强势板块用于显示（包含概念）
        with StockDatabase() as db:
            latest_data = db.query_data(limit=1)
            if latest_data is not None and not latest_data.empty:
                end_date = latest_data.iloc[0]['trade_date'].strftime('%Y-%m-%d')
                start_dt = datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=10)
                start_date = start_dt.strftime('%Y-%m-%d')
                
                recent_df = pd.read_sql(f"""
                SELECT d.ts_code, d.change_pct as pct_1d, s.industry, s.name as stock_name
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code  
                WHERE d.trade_date >= '{start_date}' AND d.trade_date <= '{end_date}'
                AND d.ts_code NOT LIKE '300%%'
                AND s.industry IS NOT NULL
                """, db.connection)
                
                strong_sectors, _ = calculate_sector_strength(recent_df, db.connection, days=5)
        
        logger.info(f"找到 {len(result_df)} 只符合强势板块回调回踩10日线条件的股票")
        
        # 创建并发送通知
        if notify:
            markdown_content = create_strong_sector_pullback_markdown(result_df, query_date, strong_sectors)
            
            try:
                send_result = send_markdown_message(markdown_content)
                if send_result:
                    logger.info("✅ 强势板块回调回踩10日线提醒已发送")
                else:
                    logger.error("❌ 发送强势板块回调回踩10日线提醒失败")
            except Exception as e:
                logger.error(f"发送消息时出错: {e}")
        
        # 打印强势板块分布统计（行业+概念）
        if not result_df.empty:
            print(f"\\n🏢 强势板块分布（行业+概念）:")
            
            # 统计最强板块归属
            best_sector_stats = result_df['best_sector_name'].value_counts()
            for sector, count in best_sector_stats.head(8).items():
                if sector and sector != '未知':
                    # 获取板块类型和排名
                    sample_row = result_df[result_df['best_sector_name'] == sector].iloc[0]
                    sector_type = sample_row['best_sector_type']
                    sector_rank = sample_row['best_sector_rank']
                    sector_pct = sample_row['best_sector_pct']
                    print(f"   {sector_type}-{sector} (排名第{sector_rank}, {sector_pct:+.1f}%): {count}只")
        
        # 打印结果摘要
        print(f"\\n📈 强势板块回调回踩10日线股票 ({query_date}):")
        print("=" * 120)
        print("排名  股票名称     代码      强势板块           类型  当前价  10日涨幅  回调幅度  距MA10   信号强度")
        print("-" * 120)
        
        for i, (_, row) in enumerate(result_df.head(10).iterrows(), 1):
            code = format_stock_code(row['ts_code'])
            name = row.get('stock_name', '未知')[:6]
            sector = row.get('best_sector_name', '未知')[:10]
            sector_type = row.get('best_sector_type', '行业')[:2]
            print(f"{i:>2}   {name:<8} {code:<8} {sector:<12} {sector_type:<4} "
                  f"{row['close']:>7.2f} {row['pct_10d']:>7.1f}% {row['pullback_pct']:>7.1f}% "
                  f"{row['distance_to_ma10_pct']:>6.1f}% {row['signal_strength']:>7.0f}分")
        
        if len(result_df) > 10:
            print(f"... 还有 {len(result_df) - 10} 只股票")
        
        return result_df
        
    except Exception as e:
        logger.error(f"执行强势板块回调回踩10日线策略时出错: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='强势板块回调回踩10日线策略')
    parser.add_argument('--min-pullback', type=float, default=0.05,
                       help='最小回调幅度（默认0.05，即5%）')
    parser.add_argument('--max-pullback', type=float, default=0.25,
                       help='最大回调幅度（默认0.25，即25%）')
    parser.add_argument('--ma10-tolerance', type=float, default=0.03,
                       help='10日线容忍度（默认0.03，即3%）')
    parser.add_argument('--min-signal-strength', type=float, default=70.0,
                       help='最小信号强度（默认70.0）')
    parser.add_argument('--strong-sector-pct', type=float, default=0.3,
                       help='强势板块比例（默认0.3，即前30%）')
    parser.add_argument('--no-notify', action='store_true',
                       help='不发送通知，仅显示结果')
    
    args = parser.parse_args()
    
    # 运行策略
    result_df = run_strong_sector_pullback_strategy(
        notify=not args.no_notify,
        min_pullback=args.min_pullback,
        max_pullback=args.max_pullback,
        ma10_tolerance=args.ma10_tolerance,
        min_signal_strength=args.min_signal_strength,
        strong_sector_pct=args.strong_sector_pct
    )
    
    if not result_df.empty:
        logger.info("✅ 强势板块回调回踩10日线策略执行完成")
    else:
        logger.info("📊 今日无符合条件的强势板块回调股票")


if __name__ == "__main__":
    main()
