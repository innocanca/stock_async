# -*- coding: utf-8 -*-
"""
股票数据获取器模块
负责从Tushare API获取股票数据
"""

import tushare as ts
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import List, Optional

from config import TUSHARE_TOKEN

logger = logging.getLogger(__name__)


class StockDataFetcher:
    """股票数据获取器"""
    
    def __init__(self, token: str = None):
        """
        初始化数据获取器
        
        Args:
            token: Tushare API token
        """
        self.token = token or TUSHARE_TOKEN
        if not self.token or self.token == "your_tushare_token_here":
            raise ValueError("请在config.py中设置有效的Tushare token")
        
        # 设置tushare token
        ts.set_token(self.token)
        self.pro = ts.pro_api()
        
    def get_daily_data(self, ts_code: str, start_date: str = None, 
                      end_date: str = None) -> Optional[pd.DataFrame]:
        """
        获取单只股票的日线数据
        
        Args:
            ts_code: 股票代码（如：000001.SZ）
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            
        Returns:
            pd.DataFrame: 日线数据
        """
        try:
            logger.info(f"正在获取 {ts_code} 的日线数据...")
            
            # 获取日线数据
            df = self.pro.daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df.empty:
                logger.warning(f"股票 {ts_code} 在指定日期范围内没有数据")
                return None
            
            # 数据预处理
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            
            logger.info(f"成功获取 {ts_code} 的 {len(df)} 条日线数据")
            return df
            
        except Exception as e:
            logger.error(f"获取 {ts_code} 日线数据失败: {e}")
            return None
    
    def get_multiple_stocks_data(self, stock_codes: List[str], 
                                start_date: str = None, end_date: str = None,
                                batch_size: int = 50, delay: float = 0.5) -> pd.DataFrame:
        """
        批量获取多只股票的日线数据
        
        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            batch_size: 批次大小，防止API调用过于频繁
            delay: 每次调用的延迟时间（秒）
            
        Returns:
            pd.DataFrame: 合并后的日线数据
        """
        all_data = []
        total_stocks = len(stock_codes)
        
        logger.info(f"开始批量获取 {total_stocks} 只股票数据，批次大小: {batch_size}")
        
        for i, ts_code in enumerate(stock_codes, 1):
            try:
                # 获取单只股票数据
                df = self.get_daily_data(ts_code, start_date, end_date)
                if df is not None and not df.empty:
                    all_data.append(df)
                
                # 显示进度
                if i % 10 == 0 or i == total_stocks:
                    success_count = len(all_data)
                    logger.info(f"进度: {i}/{total_stocks} ({i/total_stocks*100:.1f}%), 成功获取: {success_count}只")
                
                # 避免频繁调用API，适当休眠
                import time
                time.sleep(delay)
                
                # 每批次后稍长休眠
                if i % batch_size == 0:
                    logger.info(f"完成第 {i//batch_size} 批次，休眠2秒...")
                    time.sleep(2.0)
                
            except Exception as e:
                logger.error(f"获取股票 {ts_code} 数据时发生错误: {e}")
                continue
        
        if not all_data:
            logger.warning("没有获取到任何股票数据")
            return pd.DataFrame()
        
        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        success_rate = len(all_data) / total_stocks * 100
        logger.info(f"批量获取完成！总共获取了 {len(combined_df)} 条股票数据记录")
        logger.info(f"成功率: {len(all_data)}/{total_stocks} ({success_rate:.1f}%)")
        
        return combined_df
    
    def get_daily_by_date(self, trade_date: str, ts_code: str = None) -> Optional[pd.DataFrame]:
        """
        根据交易日期获取当日所有股票或指定股票的行情数据
        
        这种方式适合：
        - 获取某个交易日的全市场数据
        - 对比某个交易日不同股票的表现
        
        Args:
            trade_date: 交易日期（YYYYMMDD格式）
            ts_code: 股票代码，如果指定则只获取该股票数据
            
        Returns:
            pd.DataFrame: 当日行情数据
        """
        try:
            logger.info(f"正在获取 {trade_date} 的行情数据...")
            
            # 使用trade_date参数获取数据
            df = self.pro.daily(
                trade_date=trade_date,
                ts_code=ts_code  # 可以为None获取全市场数据
            )
            
            if df.empty:
                logger.warning(f"交易日 {trade_date} 没有行情数据")
                return None
            
            # 数据预处理
            df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
            
            if ts_code:
                logger.info(f"成功获取 {ts_code} 在 {trade_date} 的数据")
            else:
                logger.info(f"成功获取 {trade_date} 全市场 {len(df)} 条数据")
            
            return df
            
        except Exception as e:
            logger.error(f"获取 {trade_date} 行情数据失败: {e}")
            return None
    
    def get_latest_trading_day_data(self, stock_codes: List[str] = None) -> Optional[pd.DataFrame]:
        """
        获取最新交易日的数据
        
        Args:
            stock_codes: 股票代码列表，如果为None则获取全市场数据
            
        Returns:
            pd.DataFrame: 最新交易日数据
        """
        # 获取最近几个交易日进行尝试
        today = datetime.now()
        for i in range(10):  # 尝试最近10天
            check_date = (today - timedelta(days=i)).strftime('%Y%m%d')
            
            try:
                if stock_codes:
                    # 获取指定股票的数据
                    all_data = []
                    for ts_code in stock_codes:
                        df = self.get_daily_by_date(check_date, ts_code)
                        if df is not None and not df.empty:
                            all_data.append(df)
                    
                    if all_data:
                        return pd.concat(all_data, ignore_index=True)
                else:
                    # 获取全市场数据
                    return self.get_daily_by_date(check_date)
                    
            except:
                continue
        
        logger.warning("无法获取最新交易日数据")
        return None

    def get_stock_basic(self, exchange: str = None, is_hs: str = None, 
                       list_status: str = 'L', market: str = None) -> Optional[pd.DataFrame]:
        """
        获取股票基础信息
        
        Args:
            exchange: 交易所（SSE上交所 SZSE深交所）
            is_hs: 是否沪深港通标的（N否 H沪股通 S深股通）
            list_status: 上市状态 L上市 D退市 P暂停上市（默认L）
            market: 市场类型 主板Main 创业板ChiNext 科创板STAR（默认None获取所有）
            
        Returns:
            pd.DataFrame: 股票基础信息
        """
        try:
            logger.info("正在获取股票基础信息...")
            df = self.pro.stock_basic(
                exchange=exchange, 
                is_hs=is_hs,
                list_status=list_status,
                market=market,
                fields='ts_code,symbol,name,area,industry,market,list_date,list_status'
            )
            logger.info(f"获取到 {len(df)} 只股票的基础信息")
            return df
        except Exception as e:
            logger.error(f"获取股票基础信息失败: {e}")
            return None
    
    def get_main_board_stocks(self, use_cache: bool = True) -> List[str]:
        """
        获取A股主板股票代码列表
        
        Args:
            use_cache: 是否使用缓存文件
            
        Returns:
            List[str]: 主板股票代码列表
        """
        cache_file = 'main_board_stocks_cache.txt'
        
        # 尝试从缓存文件读取
        if use_cache:
            try:
                import os
                
                if os.path.exists(cache_file):
                    # 检查文件修改时间
                    file_mtime = datetime.fromtimestamp(os.path.getmtime(cache_file))
                    if datetime.now() - file_mtime < timedelta(days=7):  # 7天内的缓存有效
                        with open(cache_file, 'r', encoding='utf-8') as f:
                            cached_stocks = [line.strip() for line in f.readlines() if line.strip()]
                        logger.info(f"从缓存文件读取到 {len(cached_stocks)} 只主板股票")
                        return cached_stocks
            except Exception as e:
                logger.warning(f"读取缓存文件失败: {e}")
        
        try:
            logger.info("正在从API获取A股主板股票列表...")
            
            # 获取主板股票（排除创业板和科创板）
            df = self.pro.stock_basic(
                list_status='L',  # 只要上市的股票
                fields='ts_code,symbol,name,market,list_date'
            )
            
            if df is not None and not df.empty:
                # 过滤主板股票（排除创业板300开头、科创板688开头、北交所830/430开头等）
                main_board_df = df[
                    (~df['ts_code'].str.startswith('300')) &  # 排除创业板
                    (~df['ts_code'].str.startswith('688')) &  # 排除科创板
                    (~df['ts_code'].str.startswith('830')) &  # 排除北交所
                    (~df['ts_code'].str.startswith('430')) &  # 排除北交所
                    (~df['ts_code'].str.startswith('200')) &  # 排除B股
                    (~df['ts_code'].str.startswith('900'))    # 排除B股
                ]
                
                stock_codes = main_board_df['ts_code'].tolist()
                logger.info(f"从API获取到 {len(stock_codes)} 只A股主板股票")
                
                # 保存到缓存文件
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        for code in stock_codes:
                            f.write(f"{code}\\n")
                    logger.info("股票列表已保存到缓存文件")
                except Exception as e:
                    logger.warning(f"保存缓存文件失败: {e}")
                
                # 显示一些统计信息
                sh_count = len([code for code in stock_codes if code.endswith('.SH')])
                sz_count = len([code for code in stock_codes if code.endswith('.SZ')])
                logger.info(f"其中上交所主板: {sh_count}只, 深交所主板: {sz_count}只")
                
                return stock_codes
            else:
                logger.warning("未获取到股票基础信息")
                return self._get_backup_main_board_stocks()
                
        except Exception as e:
            logger.error(f"获取主板股票列表失败: {e}")
            logger.info("使用备用主板股票列表...")
            return self._get_backup_main_board_stocks()
    
    def _get_backup_main_board_stocks(self) -> List[str]:
        """
        备用的主板股票列表（常见的大盘股）
        当API调用失败时使用
        
        Returns:
            List[str]: 备用股票代码列表
        """
        backup_stocks = [
            # 沪市主板大盘股
            '600000.SH', '600036.SH', '600519.SH', '600887.SH', '601318.SH',
            '601398.SH', '601857.SH', '601988.SH', '600028.SH', '600030.SH',
            '600050.SH', '600104.SH', '600276.SH', '600690.SH', '600703.SH',
            '600837.SH', '600900.SH', '601012.SH', '601066.SH', '601166.SH',
            '601169.SH', '601229.SH', '601288.SH', '601328.SH', '601336.SH',
            '601390.SH', '601601.SH', '601628.SH', '601668.SH', '601688.SH',
            '601766.SH', '601788.SH', '601818.SH', '601828.SH', '601888.SH',
            '601898.SH', '601919.SH', '601939.SH', '601985.SH', '601989.SH',
            
            # 深市主板大盘股
            '000001.SZ', '000002.SZ', '000063.SZ', '000100.SZ', '000157.SZ',
            '000166.SZ', '000333.SZ', '000338.SZ', '000858.SZ', '000895.SZ',
            '000938.SZ', '000961.SZ', '001979.SZ', '002001.SZ', '002007.SZ',
            '002024.SZ', '002027.SZ', '002032.SZ', '002142.SZ', '002202.SZ',
            '002230.SZ', '002236.SZ', '002241.SZ', '002304.SZ', '002352.SZ',
            '002415.SZ', '002456.SZ', '002475.SZ', '002493.SZ', '002508.SZ',
            '002594.SZ', '002601.SZ', '002602.SZ', '002714.SZ', '002736.SZ',
            '002791.SZ', '002812.SZ', '002841.SZ', '002867.SZ', '002916.SZ',
        ]
        
        logger.info(f"使用备用股票列表，包含 {len(backup_stocks)} 只主要的主板股票")
        return backup_stocks
    
    def get_trade_calendar(self, start_date: str, end_date: str, exchange: str = 'SSE') -> Optional[pd.DataFrame]:
        """
        获取交易日历
        
        Args:
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            exchange: 交易所（SSE上交所 SZSE深交所）
            
        Returns:
            pd.DataFrame: 交易日历数据
        """
        try:
            logger.info(f"正在获取 {start_date} 到 {end_date} 的交易日历...")
            
            df = self.pro.trade_cal(
                exchange=exchange,
                is_open='1',  # 只获取交易日
                start_date=start_date,
                end_date=end_date,
                fields='cal_date'
            )
            
            if df is not None and not df.empty:
                logger.info(f"获取到 {len(df)} 个交易日")
                return df
            else:
                logger.warning("未获取到交易日历数据")
                return None
                
        except Exception as e:
            logger.error(f"获取交易日历失败: {e}")
            return None
    
    def get_daily_with_retry(self, ts_code: str = '', trade_date: str = '', 
                           start_date: str = '', end_date: str = '', max_retries: int = 3) -> Optional[pd.DataFrame]:
        """
        带重试机制的日线数据获取
        
        Args:
            ts_code: 股票代码
            trade_date: 交易日期
            start_date: 开始日期
            end_date: 结束日期
            max_retries: 最大重试次数
            
        Returns:
            pd.DataFrame: 日线数据
        """
        import time
        
        for retry in range(max_retries):
            try:
                if trade_date:
                    df = self.pro.daily(ts_code=ts_code, trade_date=trade_date)
                else:
                    df = self.pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                
                if df is not None:
                    # 数据预处理
                    if not df.empty:
                        df['trade_date'] = pd.to_datetime(df['trade_date'], format='%Y%m%d')
                    return df
                    
            except Exception as e:
                logger.warning(f"第 {retry + 1} 次尝试失败: {e}")
                if retry < max_retries - 1:  # 不是最后一次重试
                    time.sleep(1 + retry)  # 递增延迟
                else:
                    logger.error(f"达到最大重试次数 {max_retries}，获取失败")
        
        return None
    
    def get_all_market_data_by_dates(self, start_date: str, end_date: str, 
                                   delay: float = 0.5, exchange: str = 'SSE') -> pd.DataFrame:
        """
        通过交易日循环获取全市场历史数据（推荐用于大批量数据获取）
        
        这种方式的优势：
        - 股票有5000+只，每年交易日只有220左右，循环次数少
        - 每次获取一天的全市场数据，效率更高
        - 更稳定，不容易触发API限制
        
        Args:
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            delay: 每次请求延迟（秒）
            exchange: 交易所
            
        Returns:
            pd.DataFrame: 全市场历史数据
        """
        import time
        
        # 获取交易日历
        trade_cal = self.get_trade_calendar(start_date, end_date, exchange)
        if trade_cal is None or trade_cal.empty:
            logger.error("无法获取交易日历，退出数据获取")
            return pd.DataFrame()
        
        trading_days = trade_cal['cal_date'].values
        total_days = len(trading_days)
        logger.info(f"开始通过交易日循环获取数据，共 {total_days} 个交易日")
        
        all_data = []
        successful_days = 0
        
        for i, trade_date in enumerate(trading_days, 1):
            try:
                logger.info(f"正在获取 {trade_date} 的全市场数据 ({i}/{total_days})")
                
                # 使用重试机制获取数据
                df = self.get_daily_with_retry(trade_date=trade_date)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                    successful_days += 1
                    logger.info(f"成功获取 {trade_date} 的 {len(df)} 只股票数据")
                else:
                    logger.warning(f"未获取到 {trade_date} 的数据")
                
                # 显示进度
                if i % 10 == 0 or i == total_days:
                    success_rate = successful_days / i * 100
                    logger.info(f"进度: {i}/{total_days} ({i/total_days*100:.1f}%), "
                              f"成功获取: {successful_days}天 ({success_rate:.1f}%)")
                
                # API调用延迟
                time.sleep(delay)
                
            except Exception as e:
                logger.error(f"获取 {trade_date} 数据时发生错误: {e}")
                continue
        
        if not all_data:
            logger.error("未获取到任何交易日数据")
            return pd.DataFrame()
        
        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        total_records = len(combined_df)
        unique_stocks = combined_df['ts_code'].nunique() if 'ts_code' in combined_df.columns else 0
        
        logger.info(f"🎉 全市场数据获取完成！")
        logger.info(f"   📊 总记录数: {total_records:,} 条")
        logger.info(f"   📈 涉及股票: {unique_stocks} 只") 
        logger.info(f"   📅 交易日数: {successful_days}/{total_days}")
        logger.info(f"   ✅ 成功率: {successful_days/total_days*100:.1f}%")
        
        return combined_df
    
    def estimate_market_data_time(self, start_date: str, end_date: str, delay: float = 0.5) -> str:
        """
        预估全市场数据获取时间
        
        Args:
            start_date: 开始日期
            end_date: 结束日期  
            delay: 延迟时间
            
        Returns:
            str: 预估时间描述
        """
        # 粗略估算交易日数量（每年约220个交易日）
        from datetime import datetime
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            days_diff = (end_dt - start_dt).days
            estimated_trading_days = int(days_diff * 220 / 365)  # 粗略估算
        except:
            estimated_trading_days = 220  # 默认一年
        
        total_seconds = estimated_trading_days * delay
        
        if total_seconds < 60:
            return f"{total_seconds:.0f}秒"
        elif total_seconds < 3600:
            return f"{total_seconds/60:.1f}分钟"
        else:
            return f"{total_seconds/3600:.1f}小时"
