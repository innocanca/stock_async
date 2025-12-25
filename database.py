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

# 使用统一日志配置
from log_config import get_logger
logger = get_logger(__name__)


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
    
    def create_weekly_table(self):
        """创建周线数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS weekly_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    trade_date DATE NOT NULL COMMENT '交易日期(周)',
                    open DECIMAL(10,2) COMMENT '周开盘价',
                    high DECIMAL(10,2) COMMENT '周最高价',
                    low DECIMAL(10,2) COMMENT '周最低价',
                    close DECIMAL(10,2) COMMENT '周收盘价',
                    pre_close DECIMAL(10,2) COMMENT '上一周收盘价',
                    change_amount DECIMAL(10,2) COMMENT '周涨跌额',
                    pct_chg DECIMAL(8,4) COMMENT '周涨跌幅(%)',
                    vol DECIMAL(15,2) COMMENT '周成交量',
                    amount DECIMAL(20,2) COMMENT '周成交额',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_stock_week (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票周线数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("周线数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建周线数据表失败: {e}")
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
    
    def create_index_basic_table(self):
        """创建指数基本信息表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS index_basic (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL UNIQUE COMMENT 'TS指数代码',
                    name VARCHAR(100) NOT NULL COMMENT '简称',
                    fullname VARCHAR(200) COMMENT '指数全称',
                    market VARCHAR(10) COMMENT '市场',
                    publisher VARCHAR(50) COMMENT '发布方',
                    index_type VARCHAR(50) COMMENT '指数风格',
                    category VARCHAR(50) COMMENT '指数类别',
                    base_date DATE COMMENT '基期',
                    base_point DECIMAL(15,4) COMMENT '基点',
                    list_date DATE COMMENT '发布日期',
                    weight_rule VARCHAR(200) COMMENT '加权方式',
                    description TEXT COMMENT '描述',
                    exp_date DATE COMMENT '终止日期',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_name (name),
                    INDEX idx_market (market),
                    INDEX idx_publisher (publisher),
                    INDEX idx_category (category),
                    INDEX idx_list_date (list_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数基本信息表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("指数基本信息表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建指数基本信息表失败: {e}")
            return False
    
    def create_index_daily_table(self):
        """创建指数日线行情表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS index_daily (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '指数代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    close DECIMAL(15,4) COMMENT '收盘点位',
                    open DECIMAL(15,4) COMMENT '开盘点位',
                    high DECIMAL(15,4) COMMENT '最高点位',
                    low DECIMAL(15,4) COMMENT '最低点位',
                    pre_close DECIMAL(15,4) COMMENT '昨收盘点位',
                    change_pct DECIMAL(8,4) COMMENT '涨跌幅(%)',
                    vol DECIMAL(20,2) COMMENT '成交量(手)',
                    amount DECIMAL(20,2) COMMENT '成交额(千元)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_index_date (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_change_pct (change_pct)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数日线行情表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("指数日线行情表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建指数日线行情表失败: {e}")
            return False

    def create_index_weekly_table(self):
        """创建指数周线行情表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS index_weekly (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '指数代码',
                    trade_date DATE NOT NULL COMMENT '周线交易日期',
                    close DECIMAL(15,4) COMMENT '收盘点位',
                    open DECIMAL(15,4) COMMENT '开盘点位',
                    high DECIMAL(15,4) COMMENT '最高点位',
                    low DECIMAL(15,4) COMMENT '最低点位',
                    pre_close DECIMAL(15,4) COMMENT '昨日收盘点',
                    change_amount DECIMAL(15,4) COMMENT '涨跌点位',
                    change_pct DECIMAL(8,4) COMMENT '涨跌幅(%)',
                    vol DECIMAL(20,2) COMMENT '成交量(手)',
                    amount DECIMAL(20,2) COMMENT '成交额(千元)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_index_week (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_change_pct (change_pct)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数周线行情表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("指数周线行情表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建指数周线行情表失败: {e}")
            return False

    def create_index_weight_table(self):
        """创建指数成分和权重表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS index_weight (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    index_code VARCHAR(20) NOT NULL COMMENT '指数代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    con_code VARCHAR(20) NOT NULL COMMENT '成分股代码',
                    con_name VARCHAR(100) COMMENT '成分股名称',
                    weight DECIMAL(10,4) COMMENT '权重(%)',
                    i_weight DECIMAL(10,4) COMMENT '权重(指数内)',
                    is_new VARCHAR(2) COMMENT '是否最新(Y/N)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_index_con_date (index_code, con_code, trade_date),
                    INDEX idx_index_code (index_code),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_con_code (con_code)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='指数成分和权重表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("指数成分和权重表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建指数成分和权重表失败: {e}")
            return False

    def create_etf_daily_table(self):
        """创建ETF日线行情表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS etf_daily (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT 'ETF代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    open DECIMAL(15,4) COMMENT '开盘价(元)',
                    high DECIMAL(15,4) COMMENT '最高价(元)',
                    low DECIMAL(15,4) COMMENT '最低价(元)',
                    close DECIMAL(15,4) COMMENT '收盘价(元)',
                    pre_close DECIMAL(15,4) COMMENT '昨收盘价(元)',
                    change_amount DECIMAL(15,4) COMMENT '涨跌额(元)',
                    change_pct DECIMAL(8,4) COMMENT '涨跌幅(%)',
                    vol DECIMAL(20,2) COMMENT '成交量(手)',
                    amount DECIMAL(20,2) COMMENT '成交额(千元)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_etf_date (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date),
                    INDEX idx_change_pct (change_pct)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF日线行情表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("ETF日线行情表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建ETF日线行情表失败: {e}")
            return False

    def create_etf_basic_table(self):
        """创建ETF基础信息表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS etf_basic (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL UNIQUE COMMENT 'ETF代码',
                    extname VARCHAR(100) COMMENT 'ETF简称',
                    index_code VARCHAR(20) COMMENT '跟踪指数代码',
                    index_name VARCHAR(200) COMMENT '跟踪指数名称',
                    exchange VARCHAR(10) COMMENT '交易所',
                    etf_type VARCHAR(20) COMMENT 'ETF类型',
                    list_date DATE COMMENT '上市日期',
                    list_status VARCHAR(5) COMMENT '上市状态',
                    delist_date DATE COMMENT '退市日期',
                    mgr_name VARCHAR(100) COMMENT '基金管理人',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_index_code (index_code),
                    INDEX idx_exchange (exchange),
                    INDEX idx_list_status (list_status)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ETF基础信息表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("ETF基础信息表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建ETF基础信息表失败: {e}")
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
    
    def insert_weekly_data(self, df: pd.DataFrame):
        """
        批量插入周线数据
        
        Args:
            df: 包含周线数据的DataFrame
        
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
                INSERT INTO weekly_data 
                (ts_code, trade_date, open, high, low, close, pre_close, 
                 change_amount, pct_chg, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low), 
                close=VALUES(close), pre_close=VALUES(pre_close),
                change_amount=VALUES(change_amount), pct_chg=VALUES(pct_chg),
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
                        row['change'] if pd.notna(row['change']) else None,
                        row['pct_chg'] if pd.notna(row['pct_chg']) else None,
                        row['vol'] if pd.notna(row['vol']) else None,
                        row['amount'] if pd.notna(row['amount']) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条周线数据记录")
                return True
                
        except Exception as e:
            logger.error(f"插入周线数据失败: {e}")
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
    
    def query_weekly_data(self, ts_code: str = None, start_date: str = None, 
                         end_date: str = None, limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询股票周线数据
        
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
                   change_amount, pct_chg, vol, amount, created_at, updated_at
            FROM weekly_data
            {where_clause}
            ORDER BY trade_date DESC, ts_code
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条周线记录")
            return df
            
        except Exception as e:
            logger.error(f"查询周线数据失败: {e}")
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

    def get_stocks_concept_sectors(self, stock_codes: List[str]) -> dict:
        """
        批量获取股票所属的概念板块
        
        Args:
            stock_codes: 股票代码列表
            
        Returns:
            dict: {股票代码: [(概念板块名称, 指数代码), ...]}
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return {}
            
        if not stock_codes:
            return {}
            
        try:
            # 构建IN子句的占位符
            placeholders = ','.join(['%s'] * len(stock_codes))
            
            query_sql = f"""
            SELECT m.con_code, i.name as concept_name, m.ts_code as index_code
            FROM ths_member m
            LEFT JOIN ths_index i ON m.ts_code = i.ts_code
            WHERE m.con_code IN ({placeholders})
            AND i.type = 'N'
            ORDER BY m.con_code, i.name
            """
            
            df = pd.read_sql(query_sql, self.connection, params=stock_codes)
            
            # 组织返回数据
            result = {}
            for stock_code in stock_codes:
                result[stock_code] = []
            
            if not df.empty:
                for _, row in df.iterrows():
                    stock_code = row['con_code']
                    concept_name = row['concept_name'] or '未知概念'
                    index_code = row['index_code']
                    
                    if stock_code in result:
                        result[stock_code].append((concept_name, index_code))
            
            # 统计有概念板块的股票数量
            stocks_with_concepts = sum(1 for concepts in result.values() if concepts)
            logger.info(f"在 {len(stock_codes)} 只股票中，{stocks_with_concepts} 只股票有概念板块数据")
            
            return result
            
        except Exception as e:
            logger.error(f"批量获取股票概念板块失败: {e}")
            return {}

    def get_pullback_to_ma10_stocks(self, strong_rise_days: int = 3, min_rise_pct: float = 25.0,
                                    pullback_days_min: int = 3, pullback_days_max: int = 5,
                                    ma10_tolerance: float = 3.0) -> Optional[pd.DataFrame]:
        """
        查询强势回踩10日线的股票
        
        Args:
            strong_rise_days: 强势上涨天数（默认3天）
            min_rise_pct: 最小上涨幅度（默认25%）
            pullback_days_min: 最小回调天数（默认3天）
            pullback_days_max: 最大回调天数（默认5天）
            ma10_tolerance: 10日线容忍度百分比（默认3%）
            
        Returns:
            pd.DataFrame: 符合条件的股票数据
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return None
            
        try:
            # 简化查询：分步骤查找符合条件的股票
            query_sql = """
            SELECT 
                d.ts_code,
                s.name,
                s.industry,
                d.trade_date,
                d.close,
                d.change_pct,
                d.amount,
                -- 计算10日移动平均线
                (SELECT AVG(d2.close) 
                 FROM daily_data d2 
                 WHERE d2.ts_code = d.ts_code 
                 AND d2.trade_date <= d.trade_date 
                 ORDER BY d2.trade_date DESC 
                 LIMIT 10) as ma10
            FROM daily_data d
            LEFT JOIN stock_basic s ON d.ts_code = s.ts_code
            WHERE d.trade_date >= (
                SELECT trade_date 
                FROM (
                    SELECT DISTINCT trade_date 
                    FROM daily_data 
                    ORDER BY trade_date DESC 
                    LIMIT 10
                ) recent_dates 
                ORDER BY trade_date ASC 
                LIMIT 1
            )
            AND d.vol > 0
            AND (
                -- 上交所主板
                (d.ts_code LIKE '600%.SH' OR d.ts_code LIKE '601%.SH' OR 
                 d.ts_code LIKE '603%.SH' OR d.ts_code LIKE '605%.SH')
                OR
                -- 深交所主板（包括原中小板）
                (d.ts_code LIKE '000%.SZ' OR d.ts_code LIKE '001%.SZ' OR d.ts_code LIKE '002%.SZ')
            )
            ORDER BY d.ts_code, d.trade_date DESC
            """
            
            df = pd.read_sql(query_sql, self.connection)
            
            if df.empty:
                logger.info("未查询到任何股票数据")
                return pd.DataFrame()
            
            # 使用pandas进行进一步筛选
            result_stocks = []
            
            for ts_code in df['ts_code'].unique():
                stock_data = df[df['ts_code'] == ts_code].sort_values('trade_date').reset_index(drop=True)
                
                if len(stock_data) < 15:  # 确保有足够数据
                    continue
                
                # 查找强势上涨期（前3天涨幅≥25%）
                for i in range(len(stock_data) - 8):  # 留出回调空间
                    if i + strong_rise_days >= len(stock_data):
                        break
                    
                    rise_start_price = stock_data.iloc[i]['close']
                    rise_end_price = stock_data.iloc[i + strong_rise_days - 1]['close']
                    rise_pct = (rise_end_price - rise_start_price) / rise_start_price * 100
                    
                    if rise_pct >= min_rise_pct:
                        # 检查后续3-5天的回调情况
                        for j in range(pullback_days_min, min(pullback_days_max + 1, len(stock_data) - i - strong_rise_days + 1)):
                            if i + strong_rise_days - 1 + j >= len(stock_data):
                                break
                                
                            current_row = stock_data.iloc[i + strong_rise_days - 1 + j]
                            current_price = current_row['close']
                            ma10 = current_row['ma10']
                            
                            if ma10 is None or ma10 == 0:
                                continue
                            
                            # 计算回调幅度和距MA10距离
                            pullback_pct = (current_price - rise_end_price) / rise_end_price * 100
                            distance_from_ma10 = (current_price - ma10) / ma10 * 100
                            
                            # 检查是否符合回踩条件
                            if (pullback_pct < -2 and  # 有明显回调
                                abs(distance_from_ma10) <= ma10_tolerance):  # 接近10日线
                                
                                result_stocks.append({
                                    'ts_code': ts_code,
                                    'name': current_row['name'],
                                    'industry': current_row['industry'],
                                    'rise_start_date': stock_data.iloc[i]['trade_date'],
                                    'rise_end_date': stock_data.iloc[i + strong_rise_days - 1]['trade_date'],
                                    'current_date': current_row['trade_date'],
                                    'rise_start_price': rise_start_price,
                                    'rise_end_price': rise_end_price,
                                    'current_price': current_price,
                                    'ma10': ma10,
                                    'rise_pct': rise_pct,
                                    'pullback_days': j,
                                    'pullback_pct': pullback_pct,
                                    'distance_from_ma10': distance_from_ma10,
                                    'amount': current_row['amount'],
                                    'current_change_pct': current_row['change_pct']
                                })
                                break  # 找到一个符合条件的就跳出
                        break  # 找到强势期就跳出
            
            result_df = pd.DataFrame(result_stocks)
            
            if not result_df.empty:
                # 按前期涨幅和距离MA10排序
                result_df = result_df.sort_values(['rise_pct', 'distance_from_ma10'], 
                                                ascending=[False, True]).reset_index(drop=True)
                result_df['is_above_ma10'] = result_df['distance_from_ma10'] > 0
                logger.info(f"查询到 {len(result_df)} 只强势回踩10日线股票")
            else:
                logger.info("未查询到符合条件的强势回踩股票")
                
            return result_df
                
        except Exception as e:
            logger.error(f"查询强势回踩10日线股票失败: {e}")
            import traceback
            traceback.print_exc()
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
    
    def insert_index_basic(self, df: pd.DataFrame):
        """
        批量插入指数基本信息数据
        
        Args:
            df: 包含指数基本信息的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("指数基本信息数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO index_basic 
                (ts_code, name, fullname, market, publisher, index_type, category, 
                 base_date, base_point, list_date, weight_rule, description, exp_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                name=VALUES(name), fullname=VALUES(fullname), market=VALUES(market),
                publisher=VALUES(publisher), index_type=VALUES(index_type), 
                category=VALUES(category), base_date=VALUES(base_date),
                base_point=VALUES(base_point), list_date=VALUES(list_date),
                weight_rule=VALUES(weight_rule), description=VALUES(description),
                exp_date=VALUES(exp_date), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('name') if pd.notna(row.get('name')) else None,
                        row.get('fullname') if pd.notna(row.get('fullname')) else None,
                        row.get('market') if pd.notna(row.get('market')) else None,
                        row.get('publisher') if pd.notna(row.get('publisher')) else None,
                        row.get('index_type') if pd.notna(row.get('index_type')) else None,
                        row.get('category') if pd.notna(row.get('category')) else None,
                        row.get('base_date') if pd.notna(row.get('base_date')) else None,
                        row.get('base_point') if pd.notna(row.get('base_point')) else None,
                        row.get('list_date') if pd.notna(row.get('list_date')) else None,
                        row.get('weight_rule') if pd.notna(row.get('weight_rule')) else None,
                        row.get('desc') if pd.notna(row.get('desc')) else None,
                        row.get('exp_date') if pd.notna(row.get('exp_date')) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条指数基本信息记录")
                return True
                
        except Exception as e:
            logger.error(f"插入指数基本信息失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_index_daily(self, df: pd.DataFrame):
        """
        批量插入指数日线行情数据
        
        Args:
            df: 包含指数日线行情的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("指数日线行情数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO index_daily 
                (ts_code, trade_date, close, open, high, low, pre_close, 
                 change_pct, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                close=VALUES(close), open=VALUES(open), high=VALUES(high),
                low=VALUES(low), pre_close=VALUES(pre_close),
                change_pct=VALUES(change_pct), vol=VALUES(vol),
                amount=VALUES(amount), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('trade_date'),
                        row.get('close') if pd.notna(row.get('close')) else None,
                        row.get('open') if pd.notna(row.get('open')) else None,
                        row.get('high') if pd.notna(row.get('high')) else None,
                        row.get('low') if pd.notna(row.get('low')) else None,
                        row.get('pre_close') if pd.notna(row.get('pre_close')) else None,
                        row.get('pct_chg') if pd.notna(row.get('pct_chg')) else None,
                        row.get('vol') if pd.notna(row.get('vol')) else None,
                        row.get('amount') if pd.notna(row.get('amount')) else None,
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条指数日线行情记录")
                return True
                
        except Exception as e:
            logger.error(f"插入指数日线行情失败: {e}")
            self.connection.rollback()
            return False

    def insert_index_weekly(self, df: pd.DataFrame):
        """
        批量插入指数周线行情数据

        Args:
            df: 包含指数周线行情的DataFrame（对应 index_weekly 接口）

        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        if df.empty:
            logger.warning("指数周线行情数据为空，跳过插入")
            return True

        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO index_weekly
                (ts_code, trade_date, close, open, high, low, pre_close,
                 change_amount, change_pct, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                close=VALUES(close),
                open=VALUES(open),
                high=VALUES(high),
                low=VALUES(low),
                pre_close=VALUES(pre_close),
                change_amount=VALUES(change_amount),
                change_pct=VALUES(change_pct),
                vol=VALUES(vol),
                amount=VALUES(amount),
                updated_at=CURRENT_TIMESTAMP
                """

                data_list = []
                for _, row in df.iterrows():
                    data_list.append(
                        (
                            row.get("ts_code"),
                            row.get("trade_date"),
                            row.get("close") if pd.notna(row.get("close")) else None,
                            row.get("open") if pd.notna(row.get("open")) else None,
                            row.get("high") if pd.notna(row.get("high")) else None,
                            row.get("low") if pd.notna(row.get("low")) else None,
                            row.get("pre_close") if pd.notna(row.get("pre_close")) else None,
                            row.get("change") if pd.notna(row.get("change")) else None,
                            row.get("pct_chg") if pd.notna(row.get("pct_chg")) else None,
                            row.get("vol") if pd.notna(row.get("vol")) else None,
                            row.get("amount") if pd.notna(row.get("amount")) else None,
                        )
                    )

                cursor.executemany(insert_sql, data_list)
                self.connection.commit()

                logger.info(f"成功插入/更新 {len(data_list)} 条指数周线行情记录")
                return True

        except Exception as e:
            logger.error(f"插入指数周线行情失败: {e}")
            self.connection.rollback()
            return False

    def insert_index_weight(self, df: pd.DataFrame):
        """
        批量插入指数成分和权重数据

        Args:
            df: 包含指数成分权重数据的DataFrame，对应Tushare index_weight接口输出

        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        if df.empty:
            logger.warning("指数成分和权重数据为空，跳过插入")
            return True

        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO index_weight
                (index_code, trade_date, con_code, con_name, weight, i_weight, is_new)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                con_name=VALUES(con_name),
                weight=VALUES(weight),
                i_weight=VALUES(i_weight),
                is_new=VALUES(is_new),
                updated_at=CURRENT_TIMESTAMP
                """

                data_list = []

                def safe_date(val):
                    if pd.isna(val) or val == "NaT":
                        return None
                    return val

                for _, row in df.iterrows():
                    data_list.append(
                        (
                            row.get("index_code") or row.get("ts_code"),
                            safe_date(row.get("trade_date")),
                            row.get("con_code"),
                            row.get("con_name"),
                            row.get("weight") if pd.notna(row.get("weight")) else None,
                            row.get("i_weight") if pd.notna(row.get("i_weight")) else None,
                            row.get("is_new") if pd.notna(row.get("is_new")) else None,
                        )
                    )

                cursor.executemany(insert_sql, data_list)
                self.connection.commit()

                logger.info(f"成功插入/更新 {len(data_list)} 条指数成分和权重记录")
                return True

        except Exception as e:
            logger.error(f"插入指数成分和权重失败: {e}")
            self.connection.rollback()
            return False

    def insert_etf_daily(self, df: pd.DataFrame):
        """
        批量插入ETF日线行情数据

        Args:
            df: 包含ETF日线行情的DataFrame，对应Tushare fund_daily输出字段

        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        if df.empty:
            logger.warning("ETF日线行情数据为空，跳过插入")
            return True

        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO etf_daily
                (ts_code, trade_date, open, high, low, close, pre_close,
                 change_amount, change_pct, vol, amount)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open),
                high=VALUES(high),
                low=VALUES(low),
                close=VALUES(close),
                pre_close=VALUES(pre_close),
                change_amount=VALUES(change_amount),
                change_pct=VALUES(change_pct),
                vol=VALUES(vol),
                amount=VALUES(amount),
                updated_at=CURRENT_TIMESTAMP
                """

                data_list = []
                for _, row in df.iterrows():
                    data_list.append(
                        (
                            row.get("ts_code"),
                            row.get("trade_date"),
                            row.get("open") if pd.notna(row.get("open")) else None,
                            row.get("high") if pd.notna(row.get("high")) else None,
                            row.get("low") if pd.notna(row.get("low")) else None,
                            row.get("close") if pd.notna(row.get("close")) else None,
                            row.get("pre_close") if pd.notna(row.get("pre_close")) else None,
                            row.get("change") if pd.notna(row.get("change")) else None,
                            row.get("pct_chg") if pd.notna(row.get("pct_chg")) else None,
                            row.get("vol") if pd.notna(row.get("vol")) else None,
                            row.get("amount") if pd.notna(row.get("amount")) else None,
                        )
                    )

                cursor.executemany(insert_sql, data_list)
                self.connection.commit()

                logger.info(f"成功插入/更新 {len(data_list)} 条ETF日线行情记录")
                return True

        except Exception as e:
            logger.error(f"插入ETF日线行情失败: {e}")
            self.connection.rollback()
            return False

    def insert_etf_basic(self, df: pd.DataFrame):
        """
        批量插入ETF基础信息

        Args:
            df: 包含ETF基础信息的DataFrame

        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False

        if df.empty:
            logger.warning("ETF基础信息数据为空，跳过插入")
            return True

        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO etf_basic
                (ts_code, extname, index_code, index_name, exchange, etf_type,
                 list_date, list_status, delist_date, mgr_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                extname=VALUES(extname),
                index_code=VALUES(index_code),
                index_name=VALUES(index_name),
                exchange=VALUES(exchange),
                etf_type=VALUES(etf_type),
                list_date=VALUES(list_date),
                list_status=VALUES(list_status),
                delist_date=VALUES(delist_date),
                mgr_name=VALUES(mgr_name),
                updated_at=CURRENT_TIMESTAMP
                """

                data_list = []

                def safe_date(val):
                    if pd.isna(val) or val == "NaT":
                        return None
                    return val

                for _, row in df.iterrows():
                    data_list.append(
                        (
                            row.get("ts_code"),
                            row.get("extname") if pd.notna(row.get("extname")) else None,
                            row.get("index_code") if pd.notna(row.get("index_code")) else None,
                            row.get("index_name") if pd.notna(row.get("index_name")) else None,
                            row.get("exchange") if pd.notna(row.get("exchange")) else None,
                            row.get("etf_type") if pd.notna(row.get("etf_type")) else None,
                            safe_date(row.get("list_date")),
                            row.get("list_status") if pd.notna(row.get("list_status")) else None,
                            safe_date(row.get("delist_date")),
                            row.get("mgr_name") if pd.notna(row.get("mgr_name")) else None,
                        )
                    )

                cursor.executemany(insert_sql, data_list)
                self.connection.commit()

                logger.info(f"成功插入/更新 {len(data_list)} 条ETF基础信息记录")
                return True

        except Exception as e:
            logger.error(f"插入ETF基础信息失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_income_data(self, df: pd.DataFrame):
        """
        批量插入利润表数据
        
        Args:
            df: 包含利润表数据的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("利润表数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句（包含主要字段）
                insert_sql = """
                INSERT INTO income_data 
                (ts_code, ann_date, f_ann_date, end_date, report_type, comp_type,
                 basic_eps, diluted_eps, total_revenue, revenue, n_income, n_income_attr_p,
                 total_profit, operate_profit, oper_cost, sell_exp, admin_exp, fin_exp,
                 assets_impair_loss, rd_exp, ebit, ebitda, update_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                basic_eps=VALUES(basic_eps), diluted_eps=VALUES(diluted_eps),
                total_revenue=VALUES(total_revenue), revenue=VALUES(revenue),
                n_income=VALUES(n_income), n_income_attr_p=VALUES(n_income_attr_p),
                total_profit=VALUES(total_profit), operate_profit=VALUES(operate_profit),
                oper_cost=VALUES(oper_cost), sell_exp=VALUES(sell_exp),
                admin_exp=VALUES(admin_exp), fin_exp=VALUES(fin_exp),
                assets_impair_loss=VALUES(assets_impair_loss), rd_exp=VALUES(rd_exp),
                ebit=VALUES(ebit), ebitda=VALUES(ebitda),
                update_flag=VALUES(update_flag), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据，处理日期格式
                data_list = []
                for _, row in df.iterrows():
                    # 处理日期字段，将NaT转换为None
                    def safe_date(date_val):
                        if pd.isna(date_val) or date_val == 'NaT':
                            return None
                        return date_val
                    
                    data_list.append((
                        row.get('ts_code'),
                        safe_date(row.get('ann_date')),
                        safe_date(row.get('f_ann_date')),
                        safe_date(row.get('end_date')),
                        row.get('report_type'),
                        row.get('comp_type'),
                        row.get('basic_eps') if pd.notna(row.get('basic_eps')) else None,
                        row.get('diluted_eps') if pd.notna(row.get('diluted_eps')) else None,
                        row.get('total_revenue') if pd.notna(row.get('total_revenue')) else None,
                        row.get('revenue') if pd.notna(row.get('revenue')) else None,
                        row.get('n_income') if pd.notna(row.get('n_income')) else None,
                        row.get('n_income_attr_p') if pd.notna(row.get('n_income_attr_p')) else None,
                        row.get('total_profit') if pd.notna(row.get('total_profit')) else None,
                        row.get('operate_profit') if pd.notna(row.get('operate_profit')) else None,
                        row.get('oper_cost') if pd.notna(row.get('oper_cost')) else None,
                        row.get('sell_exp') if pd.notna(row.get('sell_exp')) else None,
                        row.get('admin_exp') if pd.notna(row.get('admin_exp')) else None,
                        row.get('fin_exp') if pd.notna(row.get('fin_exp')) else None,
                        row.get('assets_impair_loss') if pd.notna(row.get('assets_impair_loss')) else None,
                        row.get('rd_exp') if pd.notna(row.get('rd_exp')) else None,
                        row.get('ebit') if pd.notna(row.get('ebit')) else None,
                        row.get('ebitda') if pd.notna(row.get('ebitda')) else None,
                        row.get('update_flag')
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条利润表记录")
                return True
                
        except Exception as e:
            logger.error(f"插入利润表数据失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_dividend_data(self, df: pd.DataFrame):
        """
        批量插入分红送股数据
        
        Args:
            df: 包含分红送股数据的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("分红送股数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句
                insert_sql = """
                INSERT INTO dividend_data 
                (ts_code, end_date, ann_date, div_proc, stk_div, stk_bo_rate, stk_co_rate,
                 cash_div, cash_div_tax, record_date, ex_date, pay_date, div_listdate,
                 imp_ann_date, base_date, base_share)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                div_proc=VALUES(div_proc), stk_div=VALUES(stk_div), stk_bo_rate=VALUES(stk_bo_rate),
                stk_co_rate=VALUES(stk_co_rate), cash_div=VALUES(cash_div), cash_div_tax=VALUES(cash_div_tax),
                record_date=VALUES(record_date), ex_date=VALUES(ex_date), pay_date=VALUES(pay_date),
                div_listdate=VALUES(div_listdate), imp_ann_date=VALUES(imp_ann_date),
                base_date=VALUES(base_date), base_share=VALUES(base_share), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据，处理日期格式
                data_list = []
                for _, row in df.iterrows():
                    # 处理日期字段，将NaT转换为None
                    def safe_date(date_val):
                        if pd.isna(date_val) or date_val == 'NaT':
                            return None
                        return date_val
                    
                    data_list.append((
                        row.get('ts_code'),
                        safe_date(row.get('end_date')),
                        safe_date(row.get('ann_date')),
                        row.get('div_proc'),
                        row.get('stk_div') if pd.notna(row.get('stk_div')) else None,
                        row.get('stk_bo_rate') if pd.notna(row.get('stk_bo_rate')) else None,
                        row.get('stk_co_rate') if pd.notna(row.get('stk_co_rate')) else None,
                        row.get('cash_div') if pd.notna(row.get('cash_div')) else None,
                        row.get('cash_div_tax') if pd.notna(row.get('cash_div_tax')) else None,
                        safe_date(row.get('record_date')),
                        safe_date(row.get('ex_date')),
                        safe_date(row.get('pay_date')),
                        safe_date(row.get('div_listdate')),
                        safe_date(row.get('imp_ann_date')),
                        safe_date(row.get('base_date')),
                        row.get('base_share') if pd.notna(row.get('base_share')) else None
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条分红送股记录")
                return True
                
        except Exception as e:
            logger.error(f"插入分红送股数据失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_cashflow_data(self, df: pd.DataFrame):
        """
        批量插入现金流量表数据
        
        Args:
            df: 包含现金流量表数据的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("现金流量表数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 准备插入SQL语句（包含主要字段）
                insert_sql = """
                INSERT INTO cashflow_data 
                (ts_code, ann_date, f_ann_date, end_date, report_type, comp_type,
                 net_profit, finan_exp, c_fr_sale_sg, n_cashflow_act, n_cashflow_inv_act,
                 n_cash_flows_fnc_act, n_incr_cash_cash_equ, c_cash_equ_beg_period,
                 c_cash_equ_end_period, c_paid_goods_s, c_paid_to_for_empl, c_paid_for_taxes,
                 update_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                net_profit=VALUES(net_profit), finan_exp=VALUES(finan_exp),
                c_fr_sale_sg=VALUES(c_fr_sale_sg), n_cashflow_act=VALUES(n_cashflow_act),
                n_cashflow_inv_act=VALUES(n_cashflow_inv_act), n_cash_flows_fnc_act=VALUES(n_cash_flows_fnc_act),
                n_incr_cash_cash_equ=VALUES(n_incr_cash_cash_equ), c_cash_equ_beg_period=VALUES(c_cash_equ_beg_period),
                c_cash_equ_end_period=VALUES(c_cash_equ_end_period), c_paid_goods_s=VALUES(c_paid_goods_s),
                c_paid_to_for_empl=VALUES(c_paid_to_for_empl), c_paid_for_taxes=VALUES(c_paid_for_taxes),
                update_flag=VALUES(update_flag), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('ann_date'),
                        row.get('f_ann_date'),
                        row.get('end_date'),
                        row.get('report_type'),
                        row.get('comp_type'),
                        row.get('net_profit') if pd.notna(row.get('net_profit')) else None,
                        row.get('finan_exp') if pd.notna(row.get('finan_exp')) else None,
                        row.get('c_fr_sale_sg') if pd.notna(row.get('c_fr_sale_sg')) else None,
                        row.get('n_cashflow_act') if pd.notna(row.get('n_cashflow_act')) else None,
                        row.get('n_cashflow_inv_act') if pd.notna(row.get('n_cashflow_inv_act')) else None,
                        row.get('n_cash_flows_fnc_act') if pd.notna(row.get('n_cash_flows_fnc_act')) else None,
                        row.get('n_incr_cash_cash_equ') if pd.notna(row.get('n_incr_cash_cash_equ')) else None,
                        row.get('c_cash_equ_beg_period') if pd.notna(row.get('c_cash_equ_beg_period')) else None,
                        row.get('c_cash_equ_end_period') if pd.notna(row.get('c_cash_equ_end_period')) else None,
                        row.get('c_paid_goods_s') if pd.notna(row.get('c_paid_goods_s')) else None,
                        row.get('c_paid_to_for_empl') if pd.notna(row.get('c_paid_to_for_empl')) else None,
                        row.get('c_paid_for_taxes') if pd.notna(row.get('c_paid_for_taxes')) else None,
                        row.get('update_flag')
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条现金流量表记录")
                return True
                
        except Exception as e:
            logger.error(f"插入现金流量表数据失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_balancesheet_data(self, df: pd.DataFrame):
        """
        批量插入资产负债表数据
        
        Args:
            df: 包含资产负债表数据的DataFrame
            
        Returns:
            bool: 插入是否成功
        """
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        
        if df.empty:
            logger.warning("资产负债表数据为空，跳过插入")
            return True
            
        try:
            with self.connection.cursor() as cursor:
                # 资产负债表字段较多，这里选取核心常用字段
                insert_sql = """
                INSERT INTO balancesheet_data 
                (ts_code, ann_date, f_ann_date, end_date, report_type, comp_type,
                 total_assets, total_liab, total_hld_eqy_exc_min_int, total_hld_eqy_inc_min_int,
                 cap_rese, undist_profit, money_cap, trad_asset, notes_receiv, accounts_receiv,
                 oth_receiv, inventories, lt_eqt_invest, fix_assets, cip, intan_assets,
                 deferred_tax_assets, notes_payable, accounts_payable, adv_receipts,
                 st_borr, lt_borr, update_flag)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                ann_date=VALUES(ann_date), f_ann_date=VALUES(f_ann_date),
                total_assets=VALUES(total_assets), total_liab=VALUES(total_liab),
                total_hld_eqy_exc_min_int=VALUES(total_hld_eqy_exc_min_int),
                total_hld_eqy_inc_min_int=VALUES(total_hld_eqy_inc_min_int),
                undist_profit=VALUES(undist_profit), money_cap=VALUES(money_cap),
                update_flag=VALUES(update_flag), updated_at=CURRENT_TIMESTAMP
                """
                
                # 准备数据
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('ann_date'),
                        row.get('f_ann_date'),
                        row.get('end_date'),
                        row.get('report_type'),
                        row.get('comp_type'),
                        row.get('total_assets') if pd.notna(row.get('total_assets')) else None,
                        row.get('total_liab') if pd.notna(row.get('total_liab')) else None,
                        row.get('total_hld_eqy_exc_min_int') if pd.notna(row.get('total_hld_eqy_exc_min_int')) else None,
                        row.get('total_hld_eqy_inc_min_int') if pd.notna(row.get('total_hld_eqy_inc_min_int')) else None,
                        row.get('cap_rese') if pd.notna(row.get('cap_rese')) else None,
                        row.get('undist_profit') if pd.notna(row.get('undist_profit')) else None,
                        row.get('money_cap') if pd.notna(row.get('money_cap')) else None,
                        row.get('trad_asset') if pd.notna(row.get('trad_asset')) else None,
                        row.get('notes_receiv') if pd.notna(row.get('notes_receiv')) else None,
                        row.get('accounts_receiv') if pd.notna(row.get('accounts_receiv')) else None,
                        row.get('oth_receiv') if pd.notna(row.get('oth_receiv')) else None,
                        row.get('inventories') if pd.notna(row.get('inventories')) else None,
                        row.get('lt_eqt_invest') if pd.notna(row.get('lt_eqt_invest')) else None,
                        row.get('fix_assets') if pd.notna(row.get('fix_assets')) else None,
                        row.get('cip') if pd.notna(row.get('cip')) else None,
                        row.get('intan_assets') if pd.notna(row.get('intan_assets')) else None,
                        row.get('deferred_tax_assets') if pd.notna(row.get('deferred_tax_assets')) else None,
                        row.get('notes_payable') if pd.notna(row.get('notes_payable')) else None,
                        row.get('accounts_payable') if pd.notna(row.get('accounts_payable')) else None,
                        row.get('adv_receipts') if pd.notna(row.get('adv_receipts')) else None,
                        row.get('st_borr') if pd.notna(row.get('st_borr')) else None,
                        row.get('lt_borr') if pd.notna(row.get('lt_borr')) else None,
                        row.get('update_flag')
                    ))
                
                # 批量执行插入
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                
                logger.info(f"成功插入/更新 {len(data_list)} 条资产负债表记录")
                return True
                
        except Exception as e:
            logger.error(f"插入资产负债表数据失败: {e}")
            self.connection.rollback()
            return False
    
    def insert_index_dailybasic(self, df: pd.DataFrame):
        """批量插入指数每日指标数据"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        if df.empty:
            return True
        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO index_dailybasic 
                (ts_code, trade_date, total_mv, float_mv, total_share, float_share, 
                 free_share, turnover_rate, turnover_rate_f, pe, pe_ttm, pb)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                total_mv=VALUES(total_mv), float_mv=VALUES(float_mv),
                total_share=VALUES(total_share), float_share=VALUES(float_share),
                free_share=VALUES(free_share), turnover_rate=VALUES(turnover_rate),
                turnover_rate_f=VALUES(turnover_rate_f), pe=VALUES(pe),
                pe_ttm=VALUES(pe_ttm), pb=VALUES(pb), updated_at=CURRENT_TIMESTAMP
                """
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('trade_date'),
                        row.get('total_mv'),
                        row.get('float_mv'),
                        row.get('total_share'),
                        row.get('float_share'),
                        row.get('free_share'),
                        row.get('turnover_rate'),
                        row.get('turnover_rate_f'),
                        row.get('pe'),
                        row.get('pe_ttm'),
                        row.get('pb')
                    ))
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                logger.info(f"成功插入/更新 {len(data_list)} 条指数每日指标记录")
                return True
        except Exception as e:
            logger.error(f"插入指数每日指标数据失败: {e}")
            self.connection.rollback()
            return False

    def insert_ths_daily(self, df: pd.DataFrame):
        """批量插入同花顺指数行情数据"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
        if df.empty:
            return True
        try:
            with self.connection.cursor() as cursor:
                insert_sql = """
                INSERT INTO ths_daily 
                (ts_code, trade_date, open, high, low, close, pre_close, avg_price, 
                 `change`, pct_change, vol, turnover_rate)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                open=VALUES(open), high=VALUES(high), low=VALUES(low),
                close=VALUES(close), pre_close=VALUES(pre_close),
                avg_price=VALUES(avg_price), `change`=VALUES(`change`),
                pct_change=VALUES(pct_change), vol=VALUES(vol),
                turnover_rate=VALUES(turnover_rate), updated_at=CURRENT_TIMESTAMP
                """
                data_list = []
                for _, row in df.iterrows():
                    data_list.append((
                        row.get('ts_code'),
                        row.get('trade_date'),
                        row.get('open'),
                        row.get('high'),
                        row.get('low'),
                        row.get('close'),
                        row.get('pre_close'),
                        row.get('avg_price'),
                        row.get('change'),
                        row.get('pct_change'),
                        row.get('vol'),
                        row.get('turnover_rate')
                    ))
                cursor.executemany(insert_sql, data_list)
                self.connection.commit()
                logger.info(f"成功插入/更新 {len(data_list)} 条同花顺指数行情记录")
                return True
        except Exception as e:
            logger.error(f"插入同花顺指数行情数据失败: {e}")
            self.connection.rollback()
            return False

    def query_index_basic(self, ts_code: str = None, market: str = None, 
                         publisher: str = None, category: str = None,
                         limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询指数基本信息数据
        
        Args:
            ts_code: 指数代码
            market: 市场类型
            publisher: 发布商
            category: 指数类别
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
            
            if market:
                conditions.append("market = %s")
                params.append(market)
                
            if publisher:
                conditions.append("publisher = %s")
                params.append(publisher)
            
            if category:
                conditions.append("category = %s")
                params.append(category)
            
            where_clause = "WHERE " + " AND ".join(conditions) if conditions else ""
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            query_sql = f"""
            SELECT ts_code, name, fullname, market, publisher, index_type, 
                   category, base_date, base_point, list_date, weight_rule, 
                   description, exp_date, created_at, updated_at
            FROM index_basic
            {where_clause}
            ORDER BY market, list_date DESC
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条指数基本信息记录")
            return df
            
        except Exception as e:
            logger.error(f"查询指数基本信息数据失败: {e}")
            return None
    
    def query_index_daily(self, ts_code: str = None, start_date: str = None, 
                         end_date: str = None, limit: int = None) -> Optional[pd.DataFrame]:
        """
        查询指数日线行情数据
        
        Args:
            ts_code: 指数代码
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
            SELECT ts_code, trade_date, close, open, high, low, pre_close,
                   change_pct, vol, amount, created_at, updated_at
            FROM index_daily
            {where_clause}
            ORDER BY trade_date DESC, ts_code
            {limit_clause}
            """
            
            df = pd.read_sql(query_sql, self.connection, params=params)
            logger.info(f"查询到 {len(df)} 条指数日线行情记录")
            return df
            
        except Exception as e:
            logger.error(f"查询指数日线行情数据失败: {e}")
            return None
    
    def create_income_table(self):
        """创建利润表数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS income_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    ann_date DATE COMMENT '公告日期',
                    f_ann_date DATE COMMENT '实际公告日期',
                    end_date DATE NOT NULL COMMENT '报告期',
                    report_type TINYINT COMMENT '报表类型',
                    comp_type TINYINT COMMENT '公司类型',
                    basic_eps DECIMAL(10,4) COMMENT '基本每股收益',
                    diluted_eps DECIMAL(10,4) COMMENT '稀释每股收益',
                    total_revenue DECIMAL(20,2) COMMENT '营业总收入',
                    revenue DECIMAL(20,2) COMMENT '营业收入',
                    int_income DECIMAL(20,2) COMMENT '利息收入',
                    prem_earned DECIMAL(20,2) COMMENT '已赚保费',
                    comm_income DECIMAL(20,2) COMMENT '手续费及佣金收入',
                    n_commis_income DECIMAL(20,2) COMMENT '手续费及佣金净收入',
                    n_oth_income DECIMAL(20,2) COMMENT '其他经营净收益',
                    n_oth_b_income DECIMAL(20,2) COMMENT '加:其他业务净收益',
                    prem_income DECIMAL(20,2) COMMENT '保险业务收入',
                    out_prem DECIMAL(20,2) COMMENT '其中:分出保费',
                    une_prem_reser DECIMAL(20,2) COMMENT '提取未到期责任准备金',
                    reins_income DECIMAL(20,2) COMMENT '其中:分保费收入',
                    n_sec_tb_income DECIMAL(20,2) COMMENT '代理买卖证券业务净收入',
                    n_sec_uw_income DECIMAL(20,2) COMMENT '证券承销业务净收入',
                    n_asset_mg_income DECIMAL(20,2) COMMENT '受托客户资产管理业务净收入',
                    oth_b_income DECIMAL(20,2) COMMENT '其他业务收入',
                    fv_value_chg_gain DECIMAL(20,2) COMMENT '公允价值变动收益',
                    invest_income DECIMAL(20,2) COMMENT '投资净收益',
                    ass_invest_income DECIMAL(20,2) COMMENT '其中:联营企业和合营企业的投资收益',
                    forex_gain DECIMAL(20,2) COMMENT '汇兑收益',
                    total_cogs DECIMAL(20,2) COMMENT '营业总成本',
                    oper_cost DECIMAL(20,2) COMMENT '减:营业成本',
                    int_exp DECIMAL(20,2) COMMENT '利息支出',
                    comm_exp DECIMAL(20,2) COMMENT '手续费及佣金支出',
                    biz_tax_surchg DECIMAL(20,2) COMMENT '营业税金及附加',
                    sell_exp DECIMAL(20,2) COMMENT '销售费用',
                    admin_exp DECIMAL(20,2) COMMENT '管理费用',
                    fin_exp DECIMAL(20,2) COMMENT '财务费用',
                    assets_impair_loss DECIMAL(20,2) COMMENT '资产减值损失',
                    prem_refund DECIMAL(20,2) COMMENT '退保金',
                    compens_payout DECIMAL(20,2) COMMENT '赔付支出净额',
                    reser_insur_liab DECIMAL(20,2) COMMENT '提取保险责任准备金',
                    div_payt DECIMAL(20,2) COMMENT '保户红利支出',
                    reins_exp DECIMAL(20,2) COMMENT '分保费用',
                    oper_exp DECIMAL(20,2) COMMENT '营业支出',
                    compens_payout_refu DECIMAL(20,2) COMMENT '摊回赔付支出',
                    insur_reser_refu DECIMAL(20,2) COMMENT '摊回保险责任准备金',
                    reins_cost_refund DECIMAL(20,2) COMMENT '摊回分保费用',
                    other_bus_cost DECIMAL(20,2) COMMENT '其他业务成本',
                    operate_profit DECIMAL(20,2) COMMENT '营业利润',
                    non_oper_income DECIMAL(20,2) COMMENT '加:营业外收入',
                    non_oper_exp DECIMAL(20,2) COMMENT '减:营业外支出',
                    nca_disploss DECIMAL(20,2) COMMENT '其中:减:非流动资产处置净损失',
                    total_profit DECIMAL(20,2) COMMENT '利润总额',
                    income_tax DECIMAL(20,2) COMMENT '所得税费用',
                    n_income DECIMAL(20,2) COMMENT '净利润(含少数股东损益)',
                    n_income_attr_p DECIMAL(20,2) COMMENT '净利润(不含少数股东损益)',
                    minority_gain DECIMAL(20,2) COMMENT '少数股东损益',
                    oth_compr_income DECIMAL(20,2) COMMENT '其他综合收益',
                    t_compr_income DECIMAL(20,2) COMMENT '综合收益总额',
                    compr_inc_attr_p DECIMAL(20,2) COMMENT '归属于母公司(或股东)的综合收益总额',
                    compr_inc_attr_m_s DECIMAL(20,2) COMMENT '归属于少数股东的综合收益总额',
                    ebit DECIMAL(20,2) COMMENT '息税前利润',
                    ebitda DECIMAL(20,2) COMMENT '息税折旧摊销前利润',
                    insurance_exp DECIMAL(20,2) COMMENT '保险业务支出',
                    undist_profit DECIMAL(20,2) COMMENT '年初未分配利润',
                    distable_profit DECIMAL(20,2) COMMENT '可分配利润',
                    rd_exp DECIMAL(20,2) COMMENT '研发费用',
                    fin_exp_int_exp DECIMAL(20,2) COMMENT '财务费用:利息费用',
                    fin_exp_int_inc DECIMAL(20,2) COMMENT '财务费用:利息收入',
                    transfer_surplus_rese DECIMAL(20,2) COMMENT '转入盈余公积',
                    transfer_housing_imprest DECIMAL(20,2) COMMENT '转入住房周转金',
                    transfer_oth DECIMAL(20,2) COMMENT '其他转入',
                    adj_lossgain DECIMAL(20,2) COMMENT '调整以前年度损益',
                    withdra_legal_surplus DECIMAL(20,2) COMMENT '提取法定盈余公积',
                    withdra_legal_pubfund DECIMAL(20,2) COMMENT '提取法定公益金',
                    withdra_biz_devfund DECIMAL(20,2) COMMENT '提取企业发展基金',
                    workers_welfare DECIMAL(20,2) COMMENT '职工奖金福利',
                    distr_profit_shrhder DECIMAL(20,2) COMMENT '可供股东分配的利润',
                    prfshare_payable_dvd DECIMAL(20,2) COMMENT '应付优先股股利',
                    comshare_payable_dvd DECIMAL(20,2) COMMENT '应付普通股股利',
                    capit_comstock_div DECIMAL(20,2) COMMENT '转作股本的普通股股利',
                    net_after_nr_lp_correct DECIMAL(20,2) COMMENT '扣除非经常性损益后的净利润',
                    credit_impa_loss DECIMAL(20,2) COMMENT '信用减值损失',
                    net_expo_hedging_benefits DECIMAL(20,2) COMMENT '净敞口套期收益',
                    oth_impair_loss_assets DECIMAL(20,2) COMMENT '其他资产减值损失',
                    total_opcost DECIMAL(20,2) COMMENT '营业总成本',
                    amodcost_fin_assets DECIMAL(20,2) COMMENT '以摊余成本计量的金融资产终止确认收益',
                    oth_income DECIMAL(20,2) COMMENT '其他收益',
                    asset_disp_income DECIMAL(20,2) COMMENT '资产处置收益',
                    continued_net_profit DECIMAL(20,2) COMMENT '持续经营净利润',
                    end_net_profit DECIMAL(20,2) COMMENT '终止经营净利润',
                    update_flag VARCHAR(10) COMMENT '更新标识',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_stock_period (ts_code, end_date, report_type),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_end_date (end_date),
                    INDEX idx_ann_date (ann_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票利润表数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("利润表数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建利润表数据表失败: {e}")
            return False
    
    def create_cashflow_table(self):
        """创建现金流量表数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS cashflow_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    ann_date DATE COMMENT '公告日期',
                    f_ann_date DATE COMMENT '实际公告日期',
                    end_date DATE NOT NULL COMMENT '报告期',
                    report_type TINYINT COMMENT '报表类型',
                    comp_type TINYINT COMMENT '公司类型',
                    net_profit DECIMAL(20,2) COMMENT '净利润',
                    finan_exp DECIMAL(20,2) COMMENT '财务费用',
                    c_fr_sale_sg DECIMAL(20,2) COMMENT '销售商品、提供劳务收到的现金',
                    recp_tax_rends DECIMAL(20,2) COMMENT '收到的税费返还',
                    n_depos_incr_fi DECIMAL(20,2) COMMENT '客户存款和同业存放款项净增加额',
                    n_incr_loans_cb DECIMAL(20,2) COMMENT '向中央银行借款净增加额',
                    n_inc_borr_oth_fi DECIMAL(20,2) COMMENT '向其他金融机构拆入资金净增加额',
                    prem_fr_orig_contr DECIMAL(20,2) COMMENT '收到原保险合同保费取得的现金',
                    n_incr_insured_dep DECIMAL(20,2) COMMENT '保户储金净增加额',
                    n_reinsur_prem DECIMAL(20,2) COMMENT '收到再保业务现金净额',
                    n_incr_disp_tfa DECIMAL(20,2) COMMENT '处置交易性金融资产净增加额',
                    ifc_cash_incr DECIMAL(20,2) COMMENT '收取利息和手续费的现金',
                    n_incr_disp_faas DECIMAL(20,2) COMMENT '处置可供出售金融资产净增加额',
                    n_incr_disc_rec DECIMAL(20,2) COMMENT '拆入资金净增加额',
                    c_fr_oth_operate_a DECIMAL(20,2) COMMENT '收到其他与经营活动有关的现金',
                    c_inf_fr_operate_a DECIMAL(20,2) COMMENT '经营活动现金流入小计',
                    c_paid_goods_s DECIMAL(20,2) COMMENT '购买商品、接受劳务支付的现金',
                    c_paid_to_for_empl DECIMAL(20,2) COMMENT '支付给职工以及为职工支付的现金',
                    c_paid_for_taxes DECIMAL(20,2) COMMENT '支付的各项税费',
                    n_incr_clt_loan_adv DECIMAL(20,2) COMMENT '客户贷款及垫款净增加额',
                    n_incr_dep_cbob DECIMAL(20,2) COMMENT '存放央行和同业款项净增加额',
                    c_pay_claims_orig_inco DECIMAL(20,2) COMMENT '支付原保险合同赔付款项的现金',
                    pay_handling_chrg DECIMAL(20,2) COMMENT '支付利息和手续费的现金',
                    pay_comm_insur_plcy DECIMAL(20,2) COMMENT '支付保单红利的现金',
                    c_paid_oth_operate_a DECIMAL(20,2) COMMENT '支付其他与经营活动有关的现金',
                    c_outf_fr_operate_a DECIMAL(20,2) COMMENT '经营活动现金流出小计',
                    n_cashflow_act DECIMAL(20,2) COMMENT '经营活动产生的现金流量净额',
                    c_recp_disp_withdrwl_invest DECIMAL(20,2) COMMENT '收回投资收到的现金',
                    c_recp_return_invest DECIMAL(20,2) COMMENT '取得投资收益收到的现金',
                    n_recp_disp_fiolta DECIMAL(20,2) COMMENT '处置固定资产、无形资产和其他长期资产收回的现金净额',
                    n_recp_disp_sobu DECIMAL(20,2) COMMENT '处置子公司及其他营业单位收到的现金净额',
                    c_recp_oth_invest DECIMAL(20,2) COMMENT '收到其他与投资活动有关的现金',
                    c_inf_fr_inv_act DECIMAL(20,2) COMMENT '投资活动现金流入小计',
                    c_paid_acq_const_fiolta DECIMAL(20,2) COMMENT '购建固定资产、无形资产和其他长期资产支付的现金',
                    c_paid_invest DECIMAL(20,2) COMMENT '投资支付的现金',
                    n_disp_subs_oth_biz DECIMAL(20,2) COMMENT '质押贷款净增加额',
                    c_pay_oth_inv_act DECIMAL(20,2) COMMENT '支付其他与投资活动有关的现金',
                    c_outf_fr_inv_act DECIMAL(20,2) COMMENT '投资活动现金流出小计',
                    n_cashflow_inv_act DECIMAL(20,2) COMMENT '投资活动产生的现金流量净额',
                    c_recp_borrow DECIMAL(20,2) COMMENT '取得借款收到的现金',
                    proc_issue_bonds DECIMAL(20,2) COMMENT '发行债券收到的现金',
                    c_recp_oth_fin_act DECIMAL(20,2) COMMENT '收到其他与筹资活动有关的现金',
                    c_inf_fr_fin_act DECIMAL(20,2) COMMENT '筹资活动现金流入小计',
                    c_prepay_amt_borr DECIMAL(20,2) COMMENT '偿还债务支付的现金',
                    c_pay_dist_dpcp_int_exp DECIMAL(20,2) COMMENT '分配股利、利润或偿付利息支付的现金',
                    incl_dvd_profit_paid_sc_ms DECIMAL(20,2) COMMENT '其中:子公司支付给少数股东的股利、利润',
                    c_pay_oth_fin_act DECIMAL(20,2) COMMENT '支付其他与筹资活动有关的现金',
                    c_outf_fr_fin_act DECIMAL(20,2) COMMENT '筹资活动现金流出小计',
                    n_cash_flows_fnc_act DECIMAL(20,2) COMMENT '筹资活动产生的现金流量净额',
                    eff_fx_flu_cash DECIMAL(20,2) COMMENT '汇率变动对现金的影响',
                    n_incr_cash_cash_equ DECIMAL(20,2) COMMENT '现金及现金等价物净增加额',
                    c_cash_equ_beg_period DECIMAL(20,2) COMMENT '期初现金及现金等价物余额',
                    c_cash_equ_end_period DECIMAL(20,2) COMMENT '期末现金及现金等价物余额',
                    c_recp_cap_contrib DECIMAL(20,2) COMMENT '吸收投资收到的现金',
                    incl_cash_rec_saims DECIMAL(20,2) COMMENT '其中:子公司吸收少数股东投资收到的现金',
                    uncon_invest_loss DECIMAL(20,2) COMMENT '未确认投资损失',
                    prov_depr_assets DECIMAL(20,2) COMMENT '加:资产减值准备',
                    depr_fa_coga_dpba DECIMAL(20,2) COMMENT '固定资产折旧、油气资产折耗、生产性生物资产折旧',
                    amort_intang_assets DECIMAL(20,2) COMMENT '无形资产摊销',
                    lt_amort_deferred_exp DECIMAL(20,2) COMMENT '长期待摊费用摊销',
                    decr_deferred_exp DECIMAL(20,2) COMMENT '待摊费用减少',
                    incr_acc_exp DECIMAL(20,2) COMMENT '预提费用增加',
                    loss_disp_fiolta DECIMAL(20,2) COMMENT '处置固定、无形资产和其他长期资产的损失',
                    loss_scr_fa DECIMAL(20,2) COMMENT '固定资产报废损失',
                    loss_fv_chg DECIMAL(20,2) COMMENT '公允价值变动损失',
                    invest_loss DECIMAL(20,2) COMMENT '投资损失',
                    decr_def_inc_tax_assets DECIMAL(20,2) COMMENT '递延所得税资产减少',
                    incr_def_inc_tax_liab DECIMAL(20,2) COMMENT '递延所得税负债增加',
                    decr_inventories DECIMAL(20,2) COMMENT '存货的减少',
                    decr_oper_payable DECIMAL(20,2) COMMENT '经营性应收项目的减少',
                    incr_oper_payable DECIMAL(20,2) COMMENT '经营性应付项目的增加',
                    others DECIMAL(20,2) COMMENT '其他',
                    im_net_cashflow_oper_act DECIMAL(20,2) COMMENT '经营活动产生的现金流量净额(间接法)',
                    conv_debt_into_cap DECIMAL(20,2) COMMENT '债务转为资本',
                    conv_copbonds_due_within_1y DECIMAL(20,2) COMMENT '一年内到期的可转换公司债券',
                    fa_fnc_leases DECIMAL(20,2) COMMENT '融资租入固定资产',
                    update_flag VARCHAR(10) COMMENT '更新标识',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_stock_period (ts_code, end_date, report_type),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_end_date (end_date),
                    INDEX idx_ann_date (ann_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票现金流量表数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("现金流量表数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建现金流量表数据表失败: {e}")
            return False
    
    def create_balancesheet_table(self):
        """创建资产负债表数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS balancesheet_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    ann_date DATE COMMENT '公告日期',
                    f_ann_date DATE COMMENT '实际公告日期',
                    end_date DATE NOT NULL COMMENT '报告期',
                    report_type TINYINT COMMENT '报表类型',
                    comp_type TINYINT COMMENT '公司类型',
                    total_assets DECIMAL(20,2) COMMENT '资产总计',
                    total_liab DECIMAL(20,2) COMMENT '负债合计',
                    total_hld_eqy_exc_min_int DECIMAL(20,2) COMMENT '股东权益合计(不含少数股东权益)',
                    total_hld_eqy_inc_min_int DECIMAL(20,2) COMMENT '股东权益合计(含少数股东权益)',
                    cap_rese DECIMAL(20,2) COMMENT '资本公积',
                    undist_profit DECIMAL(20,2) COMMENT '未分配利润',
                    money_cap DECIMAL(20,2) COMMENT '货币资金',
                    trad_asset DECIMAL(20,2) COMMENT '交易性金融资产',
                    notes_receiv DECIMAL(20,2) COMMENT '应收票据',
                    accounts_receiv DECIMAL(20,2) COMMENT '应收账款',
                    oth_receiv DECIMAL(20,2) COMMENT '其他应收款',
                    inventories DECIMAL(20,2) COMMENT '存货',
                    lt_eqt_invest DECIMAL(20,2) COMMENT '长期股权投资',
                    fix_assets DECIMAL(20,2) COMMENT '固定资产',
                    cip DECIMAL(20,2) COMMENT '在建工程',
                    intan_assets DECIMAL(20,2) COMMENT '无形资产',
                    deferred_tax_assets DECIMAL(20,2) COMMENT '递延所得税资产',
                    notes_payable DECIMAL(20,2) COMMENT '应付票据',
                    accounts_payable DECIMAL(20,2) COMMENT '应付账款',
                    adv_receipts DECIMAL(20,2) COMMENT '预收款项',
                    st_borr DECIMAL(20,2) COMMENT '短期借款',
                    lt_borr DECIMAL(20,2) COMMENT '长期借款',
                    update_flag VARCHAR(10) COMMENT '更新标识',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_stock_period (ts_code, end_date, report_type),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_end_date (end_date),
                    INDEX idx_ann_date (ann_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票资产负债表数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("资产负债表数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建资产负债表数据表失败: {e}")
            return False
    
    def create_index_dailybasic_table(self):
        """创建指数每日指标数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS index_dailybasic (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT 'TS代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    total_mv DECIMAL(20,4) COMMENT '当日总市值（元）',
                    float_mv DECIMAL(20,4) COMMENT '当日流通市值（元）',
                    total_share DECIMAL(20,4) COMMENT '当日总股本（股）',
                    float_share DECIMAL(20,4) COMMENT '当日流通股本（股）',
                    free_share DECIMAL(20,4) COMMENT '当日自由流通股本（股）',
                    turnover_rate DECIMAL(10,4) COMMENT '换手率',
                    turnover_rate_f DECIMAL(10,4) COMMENT '换手率(基于自由流通股本)',
                    pe DECIMAL(10,4) COMMENT '市盈率',
                    pe_ttm DECIMAL(10,4) COMMENT '市盈率TTM',
                    pb DECIMAL(10,4) COMMENT '市净率',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_index_date (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='大盘指数每日指标表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("指数每日指标数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建指数每日指标数据表失败: {e}")
            return False

    def create_ths_daily_table(self):
        """创建同花顺指数行情数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS ths_daily (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT 'TS代码',
                    trade_date DATE NOT NULL COMMENT '交易日期',
                    open DECIMAL(10,4) COMMENT '开盘价',
                    high DECIMAL(10,4) COMMENT '最高价',
                    low DECIMAL(10,4) COMMENT '最低价',
                    close DECIMAL(10,4) COMMENT '收盘价',
                    pre_close DECIMAL(10,4) COMMENT '昨收价',
                    avg_price DECIMAL(10,4) COMMENT '平均价',
                    `change` DECIMAL(10,4) COMMENT '涨跌额',
                    pct_change DECIMAL(10,4) COMMENT '涨跌幅',
                    vol DECIMAL(20,4) COMMENT '成交量',
                    turnover_rate DECIMAL(10,4) COMMENT '换手率',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    UNIQUE KEY unique_index_date (ts_code, trade_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_trade_date (trade_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='同花顺概念和行业指数行情表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("同花顺指数行情数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建同花顺指数行情数据表失败: {e}")
            return False

    def create_dividend_table(self):
        """创建分红送股数据表"""
        if not self.connection:
            logger.error("请先连接数据库")
            return False
            
        try:
            with self.connection.cursor() as cursor:
                create_table_sql = """
                CREATE TABLE IF NOT EXISTS dividend_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    ts_code VARCHAR(20) NOT NULL COMMENT '股票代码',
                    end_date DATE NOT NULL COMMENT '分红年度',
                    ann_date DATE COMMENT '预案公告日',
                    div_proc VARCHAR(50) COMMENT '实施进度',
                    stk_div DECIMAL(10,4) COMMENT '每股送红股数',
                    stk_bo_rate DECIMAL(10,4) COMMENT '每股转增',
                    stk_co_rate DECIMAL(10,4) COMMENT '每股配股',
                    cash_div DECIMAL(10,4) COMMENT '每股分红(税前)',
                    cash_div_tax DECIMAL(10,4) COMMENT '每股分红(税后)',
                    record_date DATE COMMENT '股权登记日',
                    ex_date DATE COMMENT '除权除息日',
                    pay_date DATE COMMENT '派息日',
                    div_listdate DATE COMMENT '红股上市日',
                    imp_ann_date DATE COMMENT '实施公告日',
                    base_date DATE COMMENT '基准日',
                    base_share DECIMAL(20,2) COMMENT '基准股本(万股)',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
                    UNIQUE KEY unique_dividend (ts_code, end_date, ann_date),
                    INDEX idx_ts_code (ts_code),
                    INDEX idx_end_date (end_date),
                    INDEX idx_ann_date (ann_date),
                    INDEX idx_ex_date (ex_date)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='股票分红送股数据表';
                """
                cursor.execute(create_table_sql)
                self.connection.commit()
                logger.info("分红送股数据表创建成功")
                return True
        except Exception as e:
            logger.error(f"创建分红送股数据表失败: {e}")
            return False
    
    def __enter__(self):
        """上下文管理器入口"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.disconnect()
