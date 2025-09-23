#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
放量加速突破策略推送脚本
策略：放量 + 突然走加速 + 价格曲线陡增 + 趋势向上
筛选条件：
1. 放量：当日成交量 >= 5日均量的2倍
2. 加速：3日涨幅 > 前3日涨幅的1.5倍
3. 价格陡增：3日累计涨幅 >= 15%
4. 趋势向上：5日均线 > 10日均线 > 20日均线
"""

import logging
import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# 添加父目录到Python路径，以便导入database和fetcher模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from database import StockDatabase
from send_msg import send_markdown_message

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('volume_acceleration_notify.log', encoding='utf-8'),
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
    if df.empty or len(df) < 10:  # 调整最小数据要求
        return df
    
    # 确保按日期正序排列用于计算
    df_calc = df.copy().sort_values('trade_date').reset_index(drop=True)
    
    # 计算移动平均线
    df_calc['ma5'] = df_calc['close'].rolling(window=5, min_periods=1).mean()
    df_calc['ma10'] = df_calc['close'].rolling(window=10, min_periods=1).mean()
    df_calc['ma20'] = df_calc['close'].rolling(window=20, min_periods=1).mean()
    
    # 计算成交量均线
    df_calc['vol_ma5'] = df_calc['vol'].rolling(window=5, min_periods=1).mean()
    df_calc['vol_ma10'] = df_calc['vol'].rolling(window=10, min_periods=1).mean()
    
    # 计算涨幅
    df_calc['pct_1d'] = df_calc['close'].pct_change() * 100  # 1日涨幅
    df_calc['pct_3d'] = df_calc['close'].pct_change(periods=3) * 100  # 3日涨幅
    df_calc['pct_5d'] = df_calc['close'].pct_change(periods=5) * 100  # 5日涨幅
    
    # 计算加速度指标（价格变化的变化率）
    df_calc['price_acceleration'] = df_calc['pct_1d'].diff()  # 价格加速度
    df_calc['avg_pct_3d'] = df_calc['pct_1d'].rolling(window=3, min_periods=1).mean()  # 3日平均涨幅
    
    # 计算前期对比指标
    df_calc['prev_avg_pct_3d'] = df_calc['avg_pct_3d'].shift(3)  # 前3日的平均涨幅
    
    # 按原序列返回（最新日期在前）
    result_df = df_calc.sort_values('trade_date', ascending=False).reset_index(drop=True)
    
    return result_df


def check_volume_surge(row: pd.Series, vol_multiplier: float = 2.0) -> bool:
    """
    检查是否放量
    
    Args:
        row: 股票数据行
        vol_multiplier: 成交量倍数阈值
        
    Returns:
        bool: 是否满足放量条件
    """
    current_vol = row['vol']
    avg_vol_5d = row['vol_ma5']
    
    if pd.isna(current_vol) or pd.isna(avg_vol_5d) or avg_vol_5d == 0:
        return False
    
    volume_ratio = current_vol / avg_vol_5d
    return volume_ratio >= vol_multiplier


def check_price_acceleration(row: pd.Series, acceleration_threshold: float = 1.5) -> bool:
    """
    检查价格加速
    
    Args:
        row: 股票数据行
        acceleration_threshold: 加速倍数阈值
        
    Returns:
        bool: 是否满足加速条件
    """
    current_avg_pct = row['avg_pct_3d']  # 近3日平均涨幅
    prev_avg_pct = row['prev_avg_pct_3d']  # 前3日平均涨幅
    
    if pd.isna(current_avg_pct) or pd.isna(prev_avg_pct) or prev_avg_pct <= 0:
        return False
    
    acceleration_ratio = current_avg_pct / prev_avg_pct
    return acceleration_ratio >= acceleration_threshold and current_avg_pct > 2.0  # 至少要有2%的涨幅


def check_steep_price_rise(row: pd.Series, min_rise_pct: float = 15.0) -> bool:
    """
    检查价格陡增
    
    Args:
        row: 股票数据行
        min_rise_pct: 最小涨幅百分比
        
    Returns:
        bool: 是否满足陡增条件
    """
    pct_3d = row['pct_3d']
    
    if pd.isna(pct_3d):
        return False
    
    return pct_3d >= min_rise_pct


def check_upward_trend(row: pd.Series) -> bool:
    """
    检查趋势向上
    
    Args:
        row: 股票数据行
        
    Returns:
        bool: 是否满足向上趋势条件
    """
    ma5 = row['ma5']
    ma10 = row['ma10'] 
    ma20 = row['ma20']
    close = row['close']
    
    if pd.isna(ma5) or pd.isna(ma10) or pd.isna(ma20):
        return False
    
    # 均线多头排列：MA5 > MA10 > MA20
    # 股价在均线之上：close > MA5
    return close > ma5 > ma10 > ma20


def calculate_signal_strength(row: pd.Series) -> float:
    """
    计算信号强度评分（0-100）
    
    Args:
        row: 股票数据行
        
    Returns:
        float: 信号强度评分
    """
    score = 0.0
    
    # 放量评分（0-25分）
    current_vol = row['vol']
    avg_vol_5d = row['vol_ma5']
    if not pd.isna(current_vol) and not pd.isna(avg_vol_5d) and avg_vol_5d > 0:
        vol_ratio = current_vol / avg_vol_5d
        score += min(25.0, vol_ratio * 5)  # 最高25分
    
    # 加速评分（0-25分）
    current_avg_pct = row['avg_pct_3d']
    prev_avg_pct = row['prev_avg_pct_3d']
    if not pd.isna(current_avg_pct) and not pd.isna(prev_avg_pct) and prev_avg_pct > 0:
        accel_ratio = current_avg_pct / prev_avg_pct
        score += min(25.0, accel_ratio * 8)  # 最高25分
    
    # 涨幅评分（0-25分）
    pct_3d = row['pct_3d']
    if not pd.isna(pct_3d):
        score += min(25.0, pct_3d / 30.0 * 25)  # 30%涨幅得满分
    
    # 趋势评分（0-25分）
    ma5 = row['ma5']
    ma10 = row['ma10']
    ma20 = row['ma20']
    close = row['close']
    
    if not any(pd.isna(x) for x in [ma5, ma10, ma20, close]):
        # 检查均线排列和股价位置
        if close > ma5 > ma10 > ma20:
            score += 25.0  # 完美多头排列
        elif close > ma5 > ma10:
            score += 20.0  # 良好趋势
        elif close > ma5:
            score += 15.0  # 基本向上
        elif ma5 > ma10:
            score += 10.0  # 短期向上
        else:
            score += 5.0   # 微弱向上
    
    return min(100.0, score)


def find_volume_acceleration_stocks(days_back: int = 15, 
                                  vol_multiplier: float = 2.0,
                                  acceleration_threshold: float = 1.5,
                                  min_rise_pct: float = 15.0,
                                  min_signal_strength: float = 75.0) -> pd.DataFrame:
    """
    查找符合放量加速突破条件的股票
    
    Args:
        days_back: 查询历史数据天数
        vol_multiplier: 放量倍数阈值
        acceleration_threshold: 加速度阈值
        min_rise_pct: 最小涨幅要求
        min_signal_strength: 最小信号强度
        
    Returns:
        pd.DataFrame: 符合条件的股票
    """
    logger.info("开始筛选放量加速突破股票...")
    
    with StockDatabase() as db:
        # 优化查询：只获取最近10个工作日的数据，大大减少数据量
        logger.info(f"优化查询：获取最近{days_back}个交易日的数据...")
        
        # 获取数据库中的最新交易日期，从那里往回推
        latest_data = db.query_data(limit=1)
        if latest_data is None or latest_data.empty:
            logger.warning("数据库中没有数据")
            return pd.DataFrame()
        
        latest_trade_date = latest_data.iloc[0]['trade_date']
        if hasattr(latest_trade_date, 'strftime'):
            end_date = latest_trade_date.strftime('%Y-%m-%d')
        else:
            end_date = str(latest_trade_date)
        
        # 从最新交易日往前推20天（确保包含足够的工作日）
        from datetime import datetime, timedelta
        latest_dt = datetime.strptime(end_date, '%Y-%m-%d')
        start_dt = latest_dt - timedelta(days=20)
        start_date = start_dt.strftime('%Y-%m-%d')
        
        # 优化查询：只查询最近期间的主板股票数据
        logger.info(f"查询日期范围: {start_date} 到 {end_date} (仅主板股票)")
        
        # 联表查询主板股票，包含股票名称和行业信息
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
            logger.warning("没有获取到股票数据")
            return pd.DataFrame()
        
        logger.info(f"优化后查询到 {len(all_stocks_df)} 条记录")
        
        # 按股票分组处理
        qualified_stocks = []
        total_stocks = all_stocks_df['ts_code'].nunique()
        processed_count = 0
        debug_stats = {
            'total_stocks': 0,
            'insufficient_data': 0,
            'low_price_or_st': 0,
            'failed_volume': 0,
            'failed_acceleration': 0,
            'failed_steep_rise': 0,
            'failed_trend': 0,
            'low_signal_strength': 0,
            'qualified': 0
        }
        
        logger.info(f"开始处理 {total_stocks} 只股票的数据...")
        
        for ts_code, stock_df in all_stocks_df.groupby('ts_code'):
            try:
                debug_stats['total_stocks'] += 1
                processed_count += 1
                
                # 显示处理进度
                if processed_count % 1000 == 0:
                    logger.info(f"处理进度: {processed_count}/{total_stocks} ({processed_count/total_stocks*100:.1f}%)")
                
                # 确保有足够的数据（调整为更合理的阈值）
                if len(stock_df) < 10:  # 减少到10条，因为最近20天只有约15个交易日
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
                
                # 检查四个条件
                is_volume_surge = check_volume_surge(latest_row, vol_multiplier)
                is_accelerating = check_price_acceleration(latest_row, acceleration_threshold)
                is_steep_rise = check_steep_price_rise(latest_row, min_rise_pct)
                is_upward_trend = check_upward_trend(latest_row)
                
                # 统计每个条件的失败情况
                if not is_volume_surge:
                    debug_stats['failed_volume'] += 1
                if not is_accelerating:
                    debug_stats['failed_acceleration'] += 1
                if not is_steep_rise:
                    debug_stats['failed_steep_rise'] += 1
                if not is_upward_trend:
                    debug_stats['failed_trend'] += 1
                
                # 计算信号强度
                signal_strength = calculate_signal_strength(latest_row)
                
                # 检查信号强度
                if signal_strength < min_signal_strength:
                    debug_stats['low_signal_strength'] += 1
                
                # 所有条件都满足且信号强度够高
                if is_volume_surge and is_accelerating and is_steep_rise and is_upward_trend and signal_strength >= min_signal_strength:
                    debug_stats['qualified'] += 1
                    
                    # 计算附加信息
                    vol_ratio = latest_row['vol'] / latest_row['vol_ma5'] if latest_row['vol_ma5'] > 0 else 0
                    accel_ratio = (latest_row['avg_pct_3d'] / latest_row['prev_avg_pct_3d'] 
                                 if latest_row['prev_avg_pct_3d'] > 0 else 0)
                    
                    qualified_stocks.append({
                        'ts_code': ts_code,
                        'stock_name': latest_row.get('stock_name', '未知'),  # 股票名称
                        'industry': latest_row.get('industry', '未知'),    # 行业板块
                        'area': latest_row.get('area', '未知'),            # 地区
                        'trade_date': latest_row['trade_date'],
                        'close': latest_row['close'],
                        'pct_1d': latest_row['pct_1d'],  # 当日涨幅
                        'pct_3d': latest_row['pct_3d'],  # 3日涨幅
                        'pct_5d': latest_row['pct_5d'],  # 5日涨幅
                        'vol': latest_row['vol'],
                        'vol_ratio': vol_ratio,  # 放量倍数
                        'acceleration_ratio': accel_ratio,  # 加速倍数
                        'signal_strength': signal_strength,  # 信号强度
                        'ma5': latest_row['ma5'],
                        'ma10': latest_row['ma10'],
                        'ma20': latest_row['ma20'],
                        'amount_yi': latest_row['amount'] / 10000,  # 成交额（亿元）
                    })
                    
            except Exception as e:
                logger.debug(f"处理股票 {ts_code} 时出错: {e}")
                continue
        
        result_df = pd.DataFrame(qualified_stocks)
        
        # 输出调试统计信息
        logger.info("📊 筛选统计:")
        logger.info(f"   总股票数: {debug_stats['total_stocks']}")
        logger.info(f"   数据不足: {debug_stats['insufficient_data']}")
        logger.info(f"   价格/ST过滤: {debug_stats['low_price_or_st']}")
        logger.info(f"   放量条件失败: {debug_stats['failed_volume']}")
        logger.info(f"   加速条件失败: {debug_stats['failed_acceleration']}")
        logger.info(f"   陡增条件失败: {debug_stats['failed_steep_rise']}")
        logger.info(f"   趋势条件失败: {debug_stats['failed_trend']}")
        logger.info(f"   信号强度不足: {debug_stats['low_signal_strength']}")
        logger.info(f"   ✅ 最终合格: {debug_stats['qualified']}")
        
        if not result_df.empty:
            # 按信号强度排序
            result_df = result_df.sort_values('signal_strength', ascending=False)
            logger.info(f"找到 {len(result_df)} 只符合放量加速突破条件的股票")
        else:
            logger.info("没有找到符合条件的股票")
        
        return result_df


def create_volume_acceleration_markdown(df: pd.DataFrame, query_date: str) -> str:
    """
    创建放量加速突破的markdown格式消息
    
    Args:
        df: 符合条件的股票数据
        query_date: 查询日期
        
    Returns:
        str: markdown格式的消息内容
    """
    if df.empty:
        return f"""## 📈 放量加速突破提醒 ({query_date})

❌ **暂无符合条件的股票**

**筛选条件：**
- 🔊 放量：成交量 ≥ 5日均量的2倍
- ⚡ 加速：3日涨幅加速度 ≥ 1.5倍
- 📈 陡增：3日累计涨幅 ≥ 15%
- 📊 趋势：MA5 > MA10 > MA20

建议放宽筛选条件或关注市场整体情况。
"""
    
    total_count = len(df)
    
    markdown = f"""## 📈 放量加速突破机会 ({query_date})

🎯 **找到 {total_count} 只放量加速突破机会**

| 排名 | 股票名称 | 代码 | 行业板块 | 当前价 | 当日涨幅 | 3日涨幅 | 信号强度 |
|------|---------|------|----------|--------|----------|---------|----------|"""
    
    for i, (_, row) in enumerate(df.head(15).iterrows(), 1):
        code = format_stock_code(row['ts_code'])
        name = row['stock_name'][:6]  # 限制股票名称长度
        industry = row['industry'][:8] if row['industry'] else '未知'  # 限制行业名称长度
        
        markdown += f"""
| {i:>2} | {name} | {code} | {industry} | {row['close']:.2f} | {row['pct_1d']:+.1f}% | {row['pct_3d']:+.1f}% | {row['signal_strength']:.0f}分 |"""
    
    if total_count > 15:
        markdown += f"\n\n*还有 {total_count - 15} 只股票符合条件*"
    
    markdown += f"""

---

**策略说明：**
- 🔊 放量：成交量 ≥ 5日均量的2倍
- ⚡ 加速：3日涨幅加速度 ≥ 1.5倍
- 📈 陡增：3日累计涨幅 ≥ 15%
- 📊 趋势：MA5 > MA10 > MA20

*基于放量加速突破策略*
"""
    
    return markdown


def run_volume_acceleration_strategy(notify: bool = True, 
                                   vol_multiplier: float = 2.0,
                                   acceleration_threshold: float = 1.5,
                                   min_rise_pct: float = 15.0,
                                   min_signal_strength: float = 75.0) -> pd.DataFrame:
    """
    运行放量加速突破策略
    
    Args:
        notify: 是否发送通知
        vol_multiplier: 放量倍数
        acceleration_threshold: 加速度阈值
        min_rise_pct: 最小涨幅
        min_signal_strength: 最小信号强度
        
    Returns:
        pd.DataFrame: 符合条件的股票
    """
    try:
        logger.info("🚀 开始执行放量加速突破策略...")
        
        # 查找符合条件的股票
        result_df = find_volume_acceleration_stocks(
            vol_multiplier=vol_multiplier,
            acceleration_threshold=acceleration_threshold,
            min_rise_pct=min_rise_pct,
            min_signal_strength=min_signal_strength
        )
        
        if result_df.empty:
            logger.info("没有找到符合条件的股票")
            if notify:
                query_date = datetime.now().strftime('%Y-%m-%d')
                markdown_content = create_volume_acceleration_markdown(result_df, query_date)
                send_markdown_message(markdown_content)
            return result_df
        
        # 获取查询日期
        latest_date = result_df.iloc[0]['trade_date']
        if hasattr(latest_date, 'strftime'):
            query_date = latest_date.strftime('%Y-%m-%d')
        else:
            query_date = str(latest_date)
        
        logger.info(f"找到 {len(result_df)} 只符合放量加速突破条件的股票")
        
        # 创建并发送通知
        if notify:
            markdown_content = create_volume_acceleration_markdown(result_df, query_date)
            
            try:
                send_result = send_markdown_message(markdown_content)
                if send_result:
                    logger.info("✅ 放量加速突破提醒已发送")
                else:
                    logger.error("❌ 发送放量加速突破提醒失败")
            except Exception as e:
                logger.error(f"发送消息时出错: {e}")
        
        # 打印行业分布统计
        industry_stats = result_df['industry'].value_counts().head(5)
        print(f"\\n🏢 热点板块分布:")
        for industry, count in industry_stats.items():
            if industry and industry != '未知':
                print(f"   {industry}: {count}只")
        
        # 打印结果摘要
        print(f"\\n📈 放量加速突破股票 ({query_date}):")
        print("=" * 100)
        print("排名  股票名称       代码      行业板块         当前价  当日涨幅  3日涨幅   信号强度")
        print("-" * 100)
        
        for i, (_, row) in enumerate(result_df.head(10).iterrows(), 1):
            code = format_stock_code(row['ts_code'])
            name = row.get('stock_name', '未知')[:6]  # 限制名称长度
            industry = row.get('industry', '未知')[:8]  # 限制行业长度
            print(f"{i:>2}   {name:<8} {code:<8} {industry:<12} "
                  f"{row['close']:>7.2f} {row['pct_1d']:>7.1f}% {row['pct_3d']:>7.1f}% {row['signal_strength']:>7.0f}分")
        
        if len(result_df) > 10:
            print(f"... 还有 {len(result_df) - 10} 只股票")
        
        return result_df
        
    except Exception as e:
        logger.error(f"执行放量加速突破策略时出错: {e}")
        return pd.DataFrame()


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='放量加速突破策略')
    parser.add_argument('--vol-multiplier', type=float, default=2.0, 
                       help='放量倍数阈值（默认2.0）')
    parser.add_argument('--acceleration-threshold', type=float, default=1.5,
                       help='加速度阈值（默认1.5）')
    parser.add_argument('--min-rise-pct', type=float, default=15.0,
                       help='最小3日涨幅百分比（默认15.0）')
    parser.add_argument('--min-signal-strength', type=float, default=75.0,
                       help='最小信号强度（默认75.0）')
    parser.add_argument('--no-notify', action='store_true',
                       help='不发送通知，仅显示结果')
    
    args = parser.parse_args()
    
    # 运行策略
    result_df = run_volume_acceleration_strategy(
        notify=not args.no_notify,
        vol_multiplier=args.vol_multiplier,
        acceleration_threshold=args.acceleration_threshold,
        min_rise_pct=args.min_rise_pct,
        min_signal_strength=args.min_signal_strength
    )
    
    if not result_df.empty:
        logger.info("✅ 放量加速突破策略执行完成")
    else:
        logger.info("📊 今日无符合条件的股票")


if __name__ == "__main__":
    main()
