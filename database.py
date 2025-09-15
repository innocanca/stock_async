# -*- coding: utf-8 -*-
"""
数据库操作类
负责MySQL数据库的连接、创建表、数据插入等操作
"""

import pymysql
import pandas as pd
from typing import Optional, List
import logging
from config import MYSQL_CONFIG

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class StockDatabase:
    """股票数据库操作类"""
    
    def __init__(self, config: dict = None):
        """
        初始化数据库连接
        
        Args:
            config: 数据库配置字典，默认使用config.py中的配置
        """
        self.config = config or MYSQL_CONFIG
        self.connection = None
        
    def connect(self):
        """连接到MySQL数据库"""
        try:
            self.connection = pymysql.connect(**self.config)
            logger.info("数据库连接成功")
            return True
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            return False
    
    def disconnect(self):
        """断开数据库连接"""
        if self.connection:
            self.connection.close()
            logger.info("数据库连接已断开")
    
    def create_database(self):
        """创建股票数据库（如果不存在）"""
        try:
            # 先连接到MySQL服务器（不指定数据库）
            config_without_db = self.config.copy()
            database_name = config_without_db.pop('database')
            
            connection = pymysql.connect(**config_without_db)
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
                connection.commit()
                logger.info(f"数据库 {database_name} 创建成功或已存在")
            connection.close()
            return True
        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            return False
    
    def create_daily_table(self):
        """创建日线数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS daily_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    open DECIMAL(10,2) COMMENT '开盘价',
                    high DECIMAL(10,2) COMMENT '最高价',
                    low DECIMAL(10,2) COMMENT '最低价',
                    close DECIMAL(10,2) COMMENT '收盘价',
                    pre_close DECIMAL(10,2) COMMENT '昨收价',
                    change_pct DECIMAL(8,4) COMMENT '涨跌幅(%)',
                    change_amount DECIMAL(10,2) COMMENT '涨跌额',
                    vol DECIMAL(15,2) COMMENT '成交量(手)',
                    amount DECIMAL(20,2) COMMENT '成交额(千元)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_stock_date (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票日线数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("日线数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建日线数据表失败: {e}")
            return False
    
    def create_stock_basic_table(self):
        """创建股票基础信息表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS stock_basic (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL UNIQUE COMMENT '股票代码',
                    symbol VARCHAR(10) COMMENT '股票简称代码',
                    name VARCHAR(20) NOT NULL COMMENT '股票名称',
                    area VARCHAR(20) COMMENT '地区',
                    industry VARCHAR(50) COMMENT '行业',
                    market VARCHAR(20) COMMENT '市场类型（主板/创业板等）',
                    list_date DATE COMMENT '上市日期',
                    list_status VARCHAR(5) COMMENT '上市状态',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_name (name)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票基础信息表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("股票基础信息表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建股票基础信息表失败: {e}")
            return False
    
    def create_ths_index_table(self):
        """创建同花顺概念和行业指数表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS ths_index (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL UNIQUE COMMENT '指数代码',
                    name VARCHAR(100) NOT NULL COMMENT '指数名称',
                    count INT COMMENT '成分个数',
                    exchange VARCHAR(10) COMMENT '交易所',
                    list_date DATE COMMENT '上市日期',
                    type VARCHAR(10) COMMENT '指数类型(N-概念指数 I-行业指数 R-地域指数 S-同花顺特色指数 ST-同花顺风格指数 TH-同花顺主题指数 BB-同花顺宽基指数)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_name (name),
                    INDEX idx_type (type),
                    INDEX idx_exchange (exchange)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同花顺概念和行业指数表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("同花顺概念和行业指数表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建同花顺概念和行业指数表失败: {e}")
            return False
    
    def create_ths_member_table(self):
        """创建同花顺概念指数成分股表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS ths_member (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '指数代码',
                    con_code VARCHAR(20) NOT NULL COMMENT '成分股代码',
                    con_name VARCHAR(50) COMMENT '成分股名称',
                    weight DECIMAL(8,4) COMMENT '权重(暂无)',
                    in_date DATE COMMENT '纳入日期(暂无)',
                    out_date DATE COMMENT '剔除日期(暂无)',
                    is_new VARCHAR(1) COMMENT '是否最新(Y是N否)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_index_stock (ts_code, con_code),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_con_code (con_code),
                    INDEX idx_con_name (con_name),
                    INDEX idx_is_new (is_new),
                    FOREIGN KEY fk_ths_index (ts_code) REFERENCES ths_index(ts_code) ON DELETE CASCADE
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同花顺概念指数成分股表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("同花顺概念指数成分股表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建同花顺概念指数成分股表失败: {e}")
            return False
    
    def insert_daily_data(self, df: pd.DataFrame):
        """
        批量插入日线数据
        
        Args:
            df: 包含日线数据的DataFrame
        
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO daily_data 
                (ts_code, trade_date, open, high, low, close, pre_close, 
                 change_pct, change_amount, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), pre_close=VALUES(pre_close),
                change_pct=VALUES(change_pct), change_amount=VALUES(change_amount),
                vol=VALUES(vol), amount=VALUES(amount), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['ts_code'],
                        row['trade_date'],
                        row['open'] if pd.notna(row['open']) else None,
                        row['high'] if pd.notna(row['high']) else None,
                        row['low'] if pd.notna(row['low']) else None,
                        row['close'] if pd.notna(row['close']) else None,
                        row['pre_close'] if pd.notna(row['pre_close']) else None,
                        row['pct_chg'] if pd.notna(row['pct_chg']) else None,
                        row['change'] if pd.notna(row['change']) else None,
                        row['vol'] if pd.notna(row['vol']) else None,
                        row['amount'] if pd.notna(row['amount']) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条日线数据记录")
                return True
                
        except Exception as e:
            logger.error(f"插入日线数据失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_stock_basic(self, df: pd.DataFrame):
        """
        批量插入股票基础信息
        
        Args:
            df: 包含股票基础信息的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("股票基础信息为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO stock_basic 
                (ts_code, symbol, name, area, industry, market, list_date, list_status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name=VALUES(name), area=VALUES(area), industry=VALUES(industry),
                market=VALUES(market), list_date=VALUES(list_date), 
                list_status=VALUES(list_status), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['ts_code'],
                        row['symbol'] if pd.notna(row['symbol']) else None,
                        row['name'] if pd.notna(row['name']) else None,
                        row['area'] if pd.notna(row['area']) else None,
                        row['industry'] if pd.notna(row['industry']) else None,
                        row['market'] if pd.notna(row['market']) else None,
                        row['list_date'] if pd.notna(row['list_date']) else None,
                        row['list_status'] if pd.notna(row['list_status']) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条股票基础信息记录")
                return True
                
        except Exception as e:
            logger.error(f"插入股票基础信息失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_ths_index(self, df: pd.DataFrame):
        """
        批量插入同花顺概念和行业指数数据
        
        Args:
            df: 包含同花顺概念指数的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("同花顺概念指数数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO ths_index 
                (ts_code, name, count, exchange, list_date, type)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name=VALUES(name), count=VALUES(count), exchange=VALUES(exchange),
                list_date=VALUES(list_date), type=VALUES(type), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['ts_code'],
                        row['name'] if pd.notna(row['name']) else None,
                        row['count'] if pd.notna(row['count']) else None,
                        row['exchange'] if pd.notna(row['exchange']) else None,
                        row['list_date'] if pd.notna(row['list_date']) else None,
                        row['type'] if pd.notna(row['type']) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条同花顺概念指数记录")
                return True
                
        except Exception as e:
            logger.error(f"插入同花顺概念指数失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_ths_member(self, df: pd.DataFrame):
        """
        批量插入同花顺概念指数成分股数据
        
        Args:
            df: 包含同花顺概念指数成分股的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("同花顺概念指数成分股数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO ths_member 
                (ts_code, con_code, con_name, weight, in_date, out_date, is_new)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                con_name=VALUES(con_name), weight=VALUES(weight), in_date=VALUES(in_date),
                out_date=VALUES(out_date), is_new=VALUES(is_new), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row['ts_code'],
                        row['con_code'] if pd.notna(row['con_code']) else None,
                        row['con_name'] if pd.notna(row['con_name']) else None,
                        row.get('weight') if pd.notna(row.get('weight')) else None,
                        row.get('in_date') if pd.notna(row.get('in_date')) else None,
                        row.get('out_date') if pd.notna(row.get('out_date')) else None,
                        row.get('is_new') if pd.notna(row.get('is_new')) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条同花顺概念指数成分股记录")
                return True
                
        except Exception as e:
            logger.error(f"插入同花顺概念指数成分股失败: {e}")
            self.connection.rollback()
            return False
    
    def query_data(self, ts_code: str = None, start_date: str = None, 
                   end_date: str = None, limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询股票数据
        
        Args:
            ts_code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            limit: 限制返回条数
            
        Returns:
            pd.DataFrame: 查询结果
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            conditions = []
            params = []
            
            if ts_code:
                conditions.append("ts_code = %s")
                params.append(ts_code)
            
            if start_date:
                conditions.append("trade_date >= %s")
                params.append(start_date)
                
            if end_date:
                conditions.append("trade_date <= %s")
                params.append(end_date)
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_sql = f"""
            SELECT ts_code, trade_date, open, high, low, close, pre_close,
                   change_pct, change_amount, vol, amount, created_at, updated_at
            FROM daily_data
            {where_clause}
            ORDER BY trade_date DESC, ts_code
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条记录")
            return df
            
        except Exception as e:
            logger.error(f"查询数据失败: {e}")
            return None
    
    def get_limit_up_stocks(self, trade_date: str = None, min_pct: float = None) -> Optional[pd.DataFrame]:
        """
        查询涨停股票
        
        Args:
            trade_date: 指定交易日期，如果为None则查询最近交易日
            min_pct: 最小涨跌幅阈值，如果为None则自动判断涨停
            
        Returns:
            pd.DataFrame: 涨停股票数据（包含股票名称）
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            with self.connection.cursor() as cursor:
                # 如果未指定日期，则获取最近交易日
                if not trade_date:
                    cursor.execute("SELECT MAX(trade_date) FROM daily_data")
                    result = cursor.fetchone()
                    if not result or not result[0]:
                        logger.warning("数据库中没有数据")
                        return None
                    trade_date = result[0].strftime('%Y-%m-%d')
                
                # 构建查询条件
                if min_pct is None:
                    # 自动判断涨停：涨幅 >= 9.8% 且 收盘价 = 最高价（或接近最高价）
                    where_condition = """
                    WHERE d.trade_date = %s 
                    AND d.change_pct >= 9.8 
                    AND ABS(d.close - d.high) / d.high <= 0.003
                    AND d.vol > 0
                    """
                    params = [trade_date]
                    logger.info(f"使用自动涨停判断条件：涨幅>=9.8%且收盘价接近最高价")
                else:
                    where_condition = """
                    WHERE d.trade_date = %s 
                    AND d.change_pct >= %s
                    """
                    params = [trade_date, min_pct]
                    logger.info(f"使用自定义涨幅阈值：>={min_pct}%")
                
                # 联表查询股票基础信息，获取股票名称
                query_sql = f"""
                SELECT d.ts_code, d.trade_date, d.open, d.high, d.low, d.close, d.pre_close,
                       d.change_pct, d.change_amount, d.vol, d.amount,
                       COALESCE(s.name, '未知') as name,
                       COALESCE(s.industry, '未知') as industry,
                       COALESCE(s.market, '未知') as market
                FROM daily_data d
                LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
                {where_condition}
                ORDER BY d.amount DESC, d.change_pct DESC
                """
                
                df = pd.read_sql(query_sql, self.connection, params=params)
                
                if not df.empty:
                    # 进一步筛选真正的涨停股票（去除ST股票等特殊情况）
                    # ST股票涨停幅度通常是5%
                    st_stocks = df[df['name'].str.contains('ST|退', na=False)]
                    normal_stocks = df[~df['name'].str.contains('ST|退', na=False)]
                    
                    # 对于ST股票，使用较低的涨停阈值
                    if not st_stocks.empty and min_pct is None:
                        st_limit_up = st_stocks[st_stocks['change_pct'] >= 4.8]
                        normal_limit_up = normal_stocks[normal_stocks['change_pct'] >= 9.8]
                        df = pd.concat([normal_limit_up, st_limit_up], ignore_index=True)
                        df = df.sort_values(['amount', 'change_pct'], ascending=[False, False])
                    
                    logger.info(f"查询到 {len(df)} 只涨停股票 (日期: {trade_date})")
                    
                    # 打印前几只股票用于调试
                    if len(df) > 0:
                        logger.info("涨停股票示例：")
                        for idx, row in df.head(3).iterrows():
                            logger.info(f"  {row['name']}({row['ts_code']}) 涨幅:{row['change_pct']:.2f}%")
                else:
                    logger.info(f"未查询到涨停股票 (日期: {trade_date})")
                
                return df
                
        except Exception as e:
            logger.error(f"查询涨停股票失败: {e}")
            return None
    
    def query_ths_index(self, ts_code: str = None, index_type: str = None, 
                       exchange: str = None, limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询同花顺概念和行业指数数据
        
        Args:
            ts_code: 指数代码
            index_type: 指数类型 (N-概念指数 I-行业指数等)
            exchange: 交易所
            limit: 限制返回条数
            
        Returns:
            pd.DataFrame: 查询结果
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            conditions = []
            params = []
            
            if ts_code:
                conditions.append("ts_code = %s")
                params.append(ts_code)
            
            if index_type:
                conditions.append("type = %s")
                params.append(index_type)
                
            if exchange:
                conditions.append("exchange = %s")
                params.append(exchange)
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_sql = f"""
            SELECT ts_code, name, count, exchange, list_date, type, 
                   created_at, updated_at
            FROM ths_index
            {where_clause}
            ORDER BY type, name
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条同花顺指数记录")
            return df
            
        except Exception as e:
            logger.error(f"查询同花顺指数数据失败: {e}")
            return None
    
    def query_ths_member(self, ts_code: str = None, con_code: str = None, 
                        con_name: str = None, is_new: str = None, 
                        limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询同花顺概念指数成分股数据
        
        Args:
            ts_code: 指数代码
            con_code: 成分股代码
            con_name: 成分股名称关键字
            is_new: 是否最新 (Y-是 N-否)
            limit: 限制返回条数
            
        Returns:
            pd.DataFrame: 查询结果
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            conditions = []
            params = []
            
            if ts_code:
                conditions.append("m.ts_code = %s")
                params.append(ts_code)
            
            if con_code:
                conditions.append("m.con_code = %s")
                params.append(con_code)
                
            if con_name:
                conditions.append("m.con_name LIKE %s")
                params.append(f"%{con_name}%")
            
            if is_new:
                conditions.append("m.is_new = %s")
                params.append(is_new)
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            # 联表查询，获取指数信息
            query_sql = f"""
            SELECT m.ts_code, i.name as index_name, i.type as index_type,
                   m.con_code, m.con_name, m.weight, m.in_date, m.out_date, m.is_new,
                   m.created_at, m.updated_at
            FROM ths_member m
            LEFT JOIN ths_index i ON m.ts_code = i.ts_code
            {where_clause}
            ORDER BY m.ts_code, m.con_code
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条同花顺概念指数成分股记录")
            return df
            
        except Exception as e:
            logger.error(f"查询同花顺概念指数成分股数据失败: {e}")
            return None

    def get_latest_trading_date(self) -> Optional[str]:
        """获取最近交易日期"""
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT MAX(trade_date) FROM daily_data")
                result = cursor.fetchone()
                if result and result[0]:
                    return result[0].strftime('%Y-%m-%d')
                return None
        except Exception as e:
            logger.error(f"获取最近交易日期失败: {e}")
            return None

    def get_stats(self) -> dict:
        """获取数据库统计信息"""
        if not self.connection:
            logger.error("请先连接数据库")
            return {}
            
        try:
            with self.connection.cursor() as cursor:
                stats = {}
                
                # 总记录数
                cursor.execute("SELECT COUNT(*) as total_records FROM daily_data")
                stats['total_records'] = cursor.fetchone()[0]
                
                # 股票数量
                cursor.execute("SELECT COUNT(DISTINCT ts_code) as stock_count FROM daily_data")
                stats['stock_count'] = cursor.fetchone()[0]
                
                # 日期范围
                cursor.execute("SELECT MIN(trade_date) as min_date, MAX(trade_date) as max_date FROM daily_data")
                result = cursor.fetchone()
                stats['date_range'] = {
                    'min_date': result[0],
                    'max_date': result[1]
                }
                
                # 最新更新时间
                cursor.execute("SELECT MAX(updated_at) as last_update FROM daily_data")
                stats['last_update'] = cursor.fetchone()[0]
                
                logger.info("统计信息获取成功")
                return stats
                
        except Exception as e:
            logger.error(f"获取统计信息失败: {e}")
            return {}
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
