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
    
    def get_all_market_data_by_dates_with_batch_insert(self, start_date: str, end_date: str, 
                                                      delay: float = 0.5, exchange: str = 'SSE',
                                                      db_instance=None, batch_days: int = 10) -> dict:
        """
        通过交易日循环获取全市场历史数据，并分批插入数据库（推荐用于大批量数据）
        
        优势：
        - 按交易日批量插入，避免内存溢出
        - 实时显示插入进度
        - 支持断点续传（避免重复插入）
        - 更好的性能和稳定性
        
        Args:
            start_date: 开始日期（YYYYMMDD格式）
            end_date: 结束日期（YYYYMMDD格式）
            delay: 每次请求延迟（秒）
            exchange: 交易所
            db_instance: 数据库实例
            batch_days: 每批处理的交易日数量
            
        Returns:
            dict: 包含统计信息的字典
        """
        import time
        
        if db_instance is None:
            logger.error("需要提供数据库实例进行分批插入")
            return {}
        
        # 获取交易日历
        trade_cal = self.get_trade_calendar(start_date, end_date, exchange)
        if trade_cal is None or trade_cal.empty:
            logger.error("无法获取交易日历，退出数据获取")
            return {}
        
        trading_days = trade_cal['cal_date'].values
        total_days = len(trading_days)
        logger.info(f"🚀 开始全市场数据获取和分批插入，共 {total_days} 个交易日")
        logger.info(f"📦 分批设置: 每 {batch_days} 个交易日插入一次数据库")
        
        # 统计信息
        stats = {
            'total_trading_days': total_days,
            'successful_days': 0,
            'total_records': 0,
            'total_batches': 0,
            'failed_days': [],
            'batch_insert_success': 0,
            'batch_insert_failed': 0
        }
        
        current_batch_data = []
        batch_trading_days = []
        
        for i, trade_date in enumerate(trading_days, 1):
            try:
                logger.info(f"📅 正在获取 {trade_date} 的全市场数据 ({i}/{total_days})")
                
                # 使用重试机制获取数据
                df = self.get_daily_with_retry(trade_date=trade_date)
                
                if df is not None and not df.empty:
                    current_batch_data.append(df)
                    batch_trading_days.append(trade_date)
                    stats['successful_days'] += 1
                    logger.info(f"✅ 成功获取 {trade_date} 的 {len(df)} 只股票数据")
                else:
                    logger.warning(f"⚠️ 未获取到 {trade_date} 的数据")
                    stats['failed_days'].append(trade_date)
                
                # API调用延迟
                time.sleep(delay)
                
                # 检查是否需要插入数据库
                should_insert = (
                    len(current_batch_data) >= batch_days or  # 达到批次大小
                    i == total_days or  # 是最后一个交易日
                    len(current_batch_data) >= 20  # 数据量较大时提前插入
                )
                
                if should_insert and current_batch_data:
                    # 合并当前批次数据
                    batch_df = pd.concat(current_batch_data, ignore_index=True)
                    batch_records = len(batch_df)
                    
                    logger.info(f"💾 开始插入第 {stats['total_batches'] + 1} 批数据...")
                    logger.info(f"   📊 本批数据: {batch_records:,} 条记录")
                    logger.info(f"   📅 交易日: {batch_trading_days[0]} 到 {batch_trading_days[-1]}")
                    
                    # 插入数据库
                    insert_success = db_instance.insert_daily_data(batch_df)
                    
                    if insert_success:
                        stats['total_batches'] += 1
                        stats['total_records'] += batch_records
                        stats['batch_insert_success'] += 1
                        logger.info(f"✅ 第 {stats['total_batches']} 批数据插入成功！")
                        logger.info(f"   📈 累计插入: {stats['total_records']:,} 条记录")
                    else:
                        stats['batch_insert_failed'] += 1
                        logger.error(f"❌ 第 {stats['total_batches'] + 1} 批数据插入失败")
                    
                    # 清空当前批次数据，释放内存
                    current_batch_data = []
                    batch_trading_days = []
                
                # 显示进度
                if i % 10 == 0 or i == total_days:
                    success_rate = stats['successful_days'] / i * 100
                    logger.info(f"📊 进度: {i}/{total_days} ({i/total_days*100:.1f}%), "
                              f"成功获取: {stats['successful_days']}天 ({success_rate:.1f}%)")
                    logger.info(f"   💾 已插入: {stats['total_records']:,} 条记录")
                
            except Exception as e:
                logger.error(f"❌ 获取 {trade_date} 数据时发生错误: {e}")
                stats['failed_days'].append(trade_date)
                continue
        
        # 最终统计
        logger.info(f"🎉 全市场数据获取和插入完成！")
        logger.info(f"   📅 总交易日: {stats['total_trading_days']} 天")
        logger.info(f"   ✅ 成功获取: {stats['successful_days']} 天")
        logger.info(f"   📊 总插入记录: {stats['total_records']:,} 条")
        logger.info(f"   📦 插入批次: {stats['total_batches']} 次")
        logger.info(f"   💾 插入成功率: {stats['batch_insert_success']}/{stats['total_batches']}")
        
        if stats['failed_days']:
            logger.warning(f"   ⚠️ 失败的交易日: {len(stats['failed_days'])} 天")
            logger.debug(f"   失败日期: {stats['failed_days']}")
        
        return stats
    
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
    
    def get_ths_index(self, ts_code: str = None, exchange: str = None, 
                     index_type: str = None) -> Optional[pd.DataFrame]:
        """
        获取同花顺概念和行业指数数据
        
        根据Tushare文档，需要5000积分权限，单次最大返回5000行数据
        
        Args:
            ts_code: 指数代码
            exchange: 市场类型 A-a股 HK-港股 US-美股
            index_type: 指数类型 N-概念指数 I-行业指数 R-地域指数 S-同花顺特色指数 
                       ST-同花顺风格指数 TH-同花顺主题指数 BB-同花顺宽基指数
            
        Returns:
            pd.DataFrame: 同花顺指数数据
        """
        try:
            logger.info("正在获取同花顺概念和行业指数数据...")
            
            # 构建参数字典
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if exchange:
                params['exchange'] = exchange
            if index_type:
                params['type'] = index_type
            
            # 调用Tushare API
            df = self.pro.ths_index(**params)
            
            if df is None or df.empty:
                logger.warning("未获取到同花顺指数数据")
                return None
            
            # 数据预处理
            if 'list_date' in df.columns:
                # 将list_date转换为日期格式
                df['list_date'] = pd.to_datetime(df['list_date'], format='%Y%m%d', errors='coerce')
            
            logger.info(f"成功获取 {len(df)} 条同花顺指数数据")
            
            # 显示统计信息
            if 'type' in df.columns:
                type_counts = df['type'].value_counts()
                logger.info("指数类型分布：")
                for idx_type, count in type_counts.items():
                    type_name = self._get_index_type_name(idx_type)
                    logger.info(f"  {type_name}({idx_type}): {count}个")
            
            # 显示前几个指数信息
            logger.info("部分指数示例：")
            for i, (_, row) in enumerate(df.head(3).iterrows()):
                type_name = self._get_index_type_name(row.get('type', ''))
                logger.info(f"  {row.get('name', 'N/A')}({row.get('ts_code', 'N/A')}) - {type_name} - 成分股:{row.get('count', 'N/A')}个")
                
            return df
            
        except Exception as e:
            logger.error(f"获取同花顺概念指数失败: {e}")
            
            # 检查是否是权限问题
            if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
                logger.error("可能是权限不足，需要5000积分才能调用ths_index接口")
                logger.info("请检查您的Tushare账户积分或升级账户权限")
            
            return None
    
    def _get_index_type_name(self, index_type: str) -> str:
        """
        获取指数类型中文名称
        
        Args:
            index_type: 指数类型代码
            
        Returns:
            str: 中文名称
        """
        type_mapping = {
            'N': '概念指数',
            'I': '行业指数', 
            'R': '地域指数',
            'S': '同花顺特色指数',
            'ST': '同花顺风格指数',
            'TH': '同花顺主题指数',
            'BB': '同花顺宽基指数'
        }
        return type_mapping.get(index_type, '未知类型')
    
    def get_all_ths_index_data(self) -> Optional[pd.DataFrame]:
        """
        获取所有同花顺概念和行业指数数据（分类型获取）
        
        由于API单次调用限制5000条，这里分类型获取以确保获取完整数据
        
        Returns:
            pd.DataFrame: 所有指数数据
        """
        logger.info("🚀 开始获取所有同花顺概念和行业指数数据...")
        
        # 定义要获取的指数类型
        index_types = ['N', 'I', 'R', 'S', 'ST', 'TH', 'BB']
        all_data = []
        
        for index_type in index_types:
            try:
                type_name = self._get_index_type_name(index_type)
                logger.info(f"正在获取{type_name}({index_type})...")
                
                df = self.get_ths_index(index_type=index_type)
                
                if df is not None and not df.empty:
                    all_data.append(df)
                    logger.info(f"✅ 成功获取{type_name} {len(df)} 个指数")
                else:
                    logger.warning(f"⚠️ 未获取到{type_name}数据")
                
                # API调用延迟
                import time
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ 获取{type_name}时发生错误: {e}")
                continue
        
        if not all_data:
            logger.error("未获取到任何同花顺指数数据")
            return None
        
        # 合并所有数据
        combined_df = pd.concat(all_data, ignore_index=True)
        
        logger.info(f"🎉 同花顺指数数据获取完成！")
        logger.info(f"   📊 总指数数量: {len(combined_df)} 个")
        
        # 统计各类型数量
        if 'type' in combined_df.columns:
            type_summary = combined_df['type'].value_counts()
            logger.info("📈 指数类型汇总：")
            for idx_type, count in type_summary.items():
                type_name = self._get_index_type_name(idx_type)
                logger.info(f"   {type_name}: {count} 个")
        
        return combined_df
    
    def get_ths_member(self, ts_code: str = None, con_code: str = None) -> Optional[pd.DataFrame]:
        """
        获取同花顺概念指数成分股数据
        
        根据Tushare文档，需要5000积分权限，每分钟可调取200次
        
        Args:
            ts_code: 板块指数代码
            con_code: 股票代码
            
        Returns:
            pd.DataFrame: 概念指数成分股数据
        """
        try:
            logger.info(f"正在获取同花顺概念指数成分股数据...")
            
            # 构建参数字典
            params = {}
            if ts_code:
                params['ts_code'] = ts_code
            if con_code:
                params['con_code'] = con_code
            
            # 调用Tushare API
            df = self.pro.ths_member(**params)
            
            if df is None or df.empty:
                logger.warning(f"未获取到指数 {ts_code} 的成分股数据")
                return None
            
            logger.info(f"成功获取 {len(df)} 条成分股数据")
            
            # 显示成分股信息
            if len(df) > 0:
                logger.info(f"成分股示例：")
                for i, (_, row) in enumerate(df.head(3).iterrows()):
                    logger.info(f"  {row.get('con_name', 'N/A')}({row.get('con_code', 'N/A')})")
                    
            return df
            
        except Exception as e:
            logger.error(f"获取同花顺概念指数成分股失败: {e}")
            
            # 检查是否是权限问题
            if "权限" in str(e) or "积分" in str(e) or "permission" in str(e).lower():
                logger.error("可能是权限不足，需要5000积分才能调用ths_member接口")
                logger.info("请检查您的Tushare账户积分或升级账户权限")
            
            return None
    
    def get_all_concept_members(self, concept_indexes: List[str] = None, 
                               batch_delay: float = 0.3) -> pd.DataFrame:
        """
        获取所有概念指数的成分股数据
        
        Args:
            concept_indexes: 概念指数代码列表，如果为None则从数据库中获取
            batch_delay: 每次API调用的延迟时间（秒），防止触发频率限制
            
        Returns:
            pd.DataFrame: 所有概念指数成分股数据
        """
        import time
        
        logger.info("🚀 开始获取所有概念指数成分股数据...")
        
        # 如果没有提供指数列表，从数据库获取概念指数
        if concept_indexes is None:
            try:
                from database import StockDatabase
                with StockDatabase() as db:
                    # 只获取概念指数(N)
                    concept_df = db.query_ths_index(index_type='N')
                    if concept_df is not None and not concept_df.empty:
                        concept_indexes = concept_df['ts_code'].tolist()
                        logger.info(f"从数据库获取到 {len(concept_indexes)} 个概念指数")
                    else:
                        logger.error("数据库中没有概念指数数据")
                        return pd.DataFrame()
            except Exception as e:
                logger.error(f"从数据库获取概念指数失败: {e}")
                return pd.DataFrame()
        
        if not concept_indexes:
            logger.error("没有可用的概念指数列表")
            return pd.DataFrame()
        
        all_members_data = []
        total_indexes = len(concept_indexes)
        successful_count = 0
        failed_count = 0
        
        logger.info(f"开始批量获取 {total_indexes} 个概念指数的成分股数据")
        
        for i, ts_code in enumerate(concept_indexes, 1):
            try:
                logger.info(f"正在获取指数 {ts_code} 的成分股 ({i}/{total_indexes})")
                
                # 获取单个指数的成分股
                df = self.get_ths_member(ts_code=ts_code)
                
                if df is not None and not df.empty:
                    all_members_data.append(df)
                    successful_count += 1
                    logger.info(f"✅ 成功获取 {ts_code} 的 {len(df)} 只成分股")
                else:
                    failed_count += 1
                    logger.warning(f"⚠️ 未获取到 {ts_code} 的成分股数据")
                
                # 显示进度
                if i % 10 == 0 or i == total_indexes:
                    success_rate = successful_count / i * 100
                    logger.info(f"📊 进度: {i}/{total_indexes} ({i/total_indexes*100:.1f}%), "
                              f"成功: {successful_count}, 失败: {failed_count} ({success_rate:.1f}%)")
                
                # API调用延迟，防止频率限制
                time.sleep(batch_delay)
                
            except Exception as e:
                failed_count += 1
                logger.error(f"❌ 获取 {ts_code} 成分股时发生错误: {e}")
                continue
        
        if not all_members_data:
            logger.error("未获取到任何概念指数成分股数据")
            return pd.DataFrame()
        
        # 合并所有数据
        combined_df = pd.concat(all_members_data, ignore_index=True)
        
        logger.info(f"🎉 概念指数成分股数据获取完成！")
        logger.info(f"   📊 总成分股记录: {len(combined_df):,} 条")
        logger.info(f"   📈 涉及指数: {successful_count} 个")
        logger.info(f"   📈 不重复股票: {combined_df['con_code'].nunique() if 'con_code' in combined_df.columns else 0} 只")
        logger.info(f"   ✅ 成功率: {successful_count}/{total_indexes} ({successful_count/total_indexes*100:.1f}%)")
        
        return combined_df
    
    def get_concept_members_batch_with_db_insert(self, db_instance=None, 
                                               concept_indexes: List[str] = None,
                                               batch_delay: float = 0.3,
                                               batch_size: int = 20) -> dict:
        """
        批量获取概念指数成分股并分批插入数据库
        
        Args:
            db_instance: 数据库实例
            concept_indexes: 概念指数代码列表
            batch_delay: API调用延迟
            batch_size: 分批插入的数量
            
        Returns:
            dict: 统计信息
        """
        import time
        
        if db_instance is None:
            logger.error("需要提供数据库实例进行分批插入")
            return {}
        
        # 如果没有提供指数列表，从数据库获取概念指数
        if concept_indexes is None:
            concept_df = db_instance.query_ths_index(index_type='N')
            if concept_df is not None and not concept_df.empty:
                concept_indexes = concept_df['ts_code'].tolist()
                logger.info(f"从数据库获取到 {len(concept_indexes)} 个概念指数")
            else:
                logger.error("数据库中没有概念指数数据")
                return {}
        
        if not concept_indexes:
            logger.error("没有可用的概念指数列表")
            return {}
        
        # 统计信息
        stats = {
            'total_indexes': len(concept_indexes),
            'successful_indexes': 0,
            'failed_indexes': 0,
            'total_members': 0,
            'batch_count': 0,
            'successful_batches': 0,
            'failed_batches': 0,
            'failed_index_codes': []
        }
        
        logger.info(f"🚀 开始批量获取并插入 {stats['total_indexes']} 个概念指数的成分股数据")
        logger.info(f"📦 分批设置: 每 {batch_size} 个指数插入一次数据库")
        
        current_batch_data = []
        
        for i, ts_code in enumerate(concept_indexes, 1):
            try:
                logger.info(f"📊 正在获取指数 {ts_code} 的成分股 ({i}/{stats['total_indexes']})")
                
                # 获取单个指数的成分股
                df = self.get_ths_member(ts_code=ts_code)
                
                if df is not None and not df.empty:
                    current_batch_data.append(df)
                    stats['successful_indexes'] += 1
                    stats['total_members'] += len(df)
                    logger.info(f"✅ 成功获取 {ts_code} 的 {len(df)} 只成分股")
                else:
                    stats['failed_indexes'] += 1
                    stats['failed_index_codes'].append(ts_code)
                    logger.warning(f"⚠️ 未获取到 {ts_code} 的成分股数据")
                
                # API调用延迟
                time.sleep(batch_delay)
                
                # 检查是否需要插入数据库
                should_insert = (
                    len(current_batch_data) >= batch_size or  # 达到批次大小
                    i == stats['total_indexes']  # 是最后一个指数
                )
                
                if should_insert and current_batch_data:
                    # 合并当前批次数据
                    batch_df = pd.concat(current_batch_data, ignore_index=True)
                    batch_records = len(batch_df)
                    
                    logger.info(f"💾 开始插入第 {stats['batch_count'] + 1} 批数据...")
                    logger.info(f"   📊 本批数据: {batch_records:,} 条成分股记录")
                    
                    # 插入数据库
                    insert_success = db_instance.insert_ths_member(batch_df)
                    
                    if insert_success:
                        stats['batch_count'] += 1
                        stats['successful_batches'] += 1
                        logger.info(f"✅ 第 {stats['batch_count']} 批数据插入成功！")
                    else:
                        stats['failed_batches'] += 1
                        logger.error(f"❌ 第 {stats['batch_count'] + 1} 批数据插入失败")
                    
                    # 清空当前批次数据，释放内存
                    current_batch_data = []
                
                # 显示进度
                if i % 10 == 0 or i == stats['total_indexes']:
                    success_rate = stats['successful_indexes'] / i * 100
                    logger.info(f"📊 进度: {i}/{stats['total_indexes']} ({i/stats['total_indexes']*100:.1f}%), "
                              f"成功: {stats['successful_indexes']}, 失败: {stats['failed_indexes']} ({success_rate:.1f}%)")
                    logger.info(f"   💾 已插入: {stats['total_members']:,} 条成分股记录")
                
            except Exception as e:
                stats['failed_indexes'] += 1
                stats['failed_index_codes'].append(ts_code)
                logger.error(f"❌ 获取 {ts_code} 成分股时发生错误: {e}")
                continue
        
        # 最终统计
        logger.info(f"🎉 概念指数成分股数据获取和插入完成！")
        logger.info(f"   📊 处理指数: {stats['total_indexes']} 个")
        logger.info(f"   ✅ 成功指数: {stats['successful_indexes']} 个")
        logger.info(f"   📊 总成分股记录: {stats['total_members']:,} 条")
        logger.info(f"   📦 插入批次: {stats['batch_count']} 次")
        logger.info(f"   💾 插入成功率: {stats['successful_batches']}/{stats['batch_count']}")
        
        if stats['failed_index_codes']:
            logger.warning(f"   ⚠️ 失败的指数: {len(stats['failed_index_codes'])} 个")
            logger.debug(f"   失败指数代码: {stats['failed_index_codes']}")
        
        return stats