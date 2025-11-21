# 财务数据初始化使用说明

## 概述

本项目新增了完整的财务数据获取、存储和分析功能，基于 [Tushare财务数据接口](https://tushare.pro/document/2?doc_id=33) 实现，支持：

- 📈 **利润表数据** - 盈利能力分析
- 💰 **现金流量表数据** - 现金流状况分析  
- 🎁 **分红送股数据** - 股息率和分红策略分析

## 快速开始

### 🚀 一键初始化所有财务数据

```bash
# 推荐：一次性初始化所有财务数据
python init_data/init_all_financial.py
```

### 📊 分别初始化各类财务数据

```bash
# 初始化利润表数据（最近3年）
python init_data/init_income.py

# 初始化现金流量表数据（最近3年）  
python init_data/init_cashflow.py

# 初始化分红送股数据（最近5年）
python init_data/init_dividend.py
```

## 数据表结构

### 1. 利润表 (`income_data`)

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR(20) | 股票代码 |
| end_date | DATE | 报告期 |
| ann_date | DATE | 公告日期 |
| basic_eps | DECIMAL(10,4) | 基本每股收益 |
| diluted_eps | DECIMAL(10,4) | 稀释每股收益 |
| total_revenue | DECIMAL(20,2) | 营业总收入 |
| revenue | DECIMAL(20,2) | 营业收入 |
| n_income | DECIMAL(20,2) | 净利润(含少数股东) |
| n_income_attr_p | DECIMAL(20,2) | 净利润(归母) |
| operate_profit | DECIMAL(20,2) | 营业利润 |
| oper_cost | DECIMAL(20,2) | 营业成本 |
| sell_exp | DECIMAL(20,2) | 销售费用 |
| admin_exp | DECIMAL(20,2) | 管理费用 |
| fin_exp | DECIMAL(20,2) | 财务费用 |
| rd_exp | DECIMAL(20,2) | 研发费用 |
| ebit | DECIMAL(20,2) | 息税前利润 |
| ebitda | DECIMAL(20,2) | 息税折旧摊销前利润 |

### 2. 现金流量表 (`cashflow_data`)

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR(20) | 股票代码 |
| end_date | DATE | 报告期 |
| net_profit | DECIMAL(20,2) | 净利润 |
| n_cashflow_act | DECIMAL(20,2) | 经营活动现金流净额 |
| n_cashflow_inv_act | DECIMAL(20,2) | 投资活动现金流净额 |
| n_cash_flows_fnc_act | DECIMAL(20,2) | 筹资活动现金流净额 |
| n_incr_cash_cash_equ | DECIMAL(20,2) | 现金及现金等价物净增加额 |
| c_cash_equ_end_period | DECIMAL(20,2) | 期末现金及现金等价物余额 |
| c_fr_sale_sg | DECIMAL(20,2) | 销售商品收到的现金 |
| c_paid_goods_s | DECIMAL(20,2) | 购买商品支付的现金 |
| c_paid_to_for_empl | DECIMAL(20,2) | 支付给职工的现金 |
| c_paid_for_taxes | DECIMAL(20,2) | 支付的各项税费 |

### 3. 分红送股数据 (`dividend_data`)

| 字段 | 类型 | 说明 |
|------|------|------|
| ts_code | VARCHAR(20) | 股票代码 |
| end_date | DATE | 分红年度 |
| ann_date | DATE | 预案公告日 |
| div_proc | VARCHAR(50) | 实施进度 |
| stk_div | DECIMAL(10,4) | 每股送红股数 |
| stk_bo_rate | DECIMAL(10,4) | 每股转增 |
| cash_div | DECIMAL(10,4) | 每股分红(税前) |
| cash_div_tax | DECIMAL(10,4) | 每股分红(税后) |
| record_date | DATE | 股权登记日 |
| ex_date | DATE | 除权除息日 |
| pay_date | DATE | 派息日 |
| base_share | DECIMAL(20,2) | 基准股本(万股) |

## API 使用方法

### 数据获取 (fetcher.py)

```python
from fetcher import StockDataFetcher

fetcher = StockDataFetcher()

# 获取单只股票利润表数据
income_df = fetcher.get_income_data(
    ts_code="000001.SZ",
    start_date="20220101", 
    end_date="20241231"
)

# 获取现金流量表数据
cashflow_df = fetcher.get_cashflow_data(
    ts_code="000001.SZ",
    start_date="20220101",
    end_date="20241231"
)

# 获取分红送股数据
dividend_df = fetcher.get_dividend_data(
    ts_code="000001.SZ",
    start_date="20200101",
    end_date="20241231"
)

# 批量获取多只股票的财务数据
stocks = ["000001.SZ", "000002.SZ", "600000.SH"]
financial_df = fetcher.get_multiple_stocks_financial_data(
    stock_codes=stocks,
    data_type='income',  # 'income', 'cashflow', 'dividend'
    years_back=3,
    batch_size=20,
    delay=0.5
)
```

### 数据库操作 (database.py)

```python
from database import StockDatabase

with StockDatabase() as db:
    # 创建财务数据表
    db.create_income_table()
    db.create_cashflow_table()
    db.create_dividend_table()
    
    # 插入财务数据
    db.insert_income_data(income_df)
    db.insert_cashflow_data(cashflow_df)
    db.insert_dividend_data(dividend_df)
```

## 数据分析示例

### 1. 盈利能力分析

```sql
-- 查询近3年营业收入增长情况
SELECT 
    ts_code, 
    YEAR(end_date) as year,
    total_revenue/100000000 as revenue_yi,
    n_income_attr_p/100000000 as net_profit_yi,
    basic_eps,
    (n_income_attr_p/total_revenue)*100 as net_margin
FROM income_data 
WHERE ts_code = '000001.SZ' 
    AND end_date >= '2022-01-01'
ORDER BY end_date DESC;
```

### 2. 现金流分析

```sql
-- 查询经营现金流与净利润比较
SELECT 
    ts_code,
    YEAR(end_date) as year,
    net_profit/100000000 as net_profit_yi,
    n_cashflow_act/100000000 as operating_cf_yi,
    (n_cashflow_act/net_profit) as cf_quality_ratio
FROM cashflow_data 
WHERE ts_code = '000001.SZ'
    AND end_date >= '2022-01-01'
ORDER BY end_date DESC;
```

### 3. 分红策略分析

```sql
-- 查询分红历史和股息率
SELECT 
    ts_code,
    end_date,
    cash_div,
    stk_div,
    stk_bo_rate,
    record_date,
    ex_date,
    div_proc
FROM dividend_data 
WHERE ts_code = '000001.SZ'
    AND end_date >= '2020-01-01'
ORDER BY end_date DESC;
```

## 数据更新策略

### 定期更新建议

```bash
# 每季度更新一次财务数据（季报发布后）
# 建议在4月、8月、10月、次年1月运行

# 利润表更新
python init_data/init_income.py

# 现金流量表更新
python init_data/init_cashflow.py

# 分红数据更新（分红预案公布期间）
python init_data/init_dividend.py
```

### 增量更新脚本

可以修改初始化脚本的日期范围，只获取最新的数据：

```python
# 只获取最近1年的数据
start_date, end_date = calculate_date_range(years_back=1)
```

## 权限要求

### Tushare积分要求

根据 [Tushare文档](https://tushare.pro/document/2?doc_id=33)：

- **利润表数据**: 需要一定积分权限
- **现金流量表数据**: 需要一定积分权限
- **分红送股数据**: 基础权限即可

### 建议

1. 注册Tushare账户并获取足够积分
2. 设置合理的API调用延迟
3. 监控API调用频率限制
4. 及时更新财务数据

## 性能优化

### 批量处理优化

```python
# 推荐的批量参数设置
batch_size = 20      # 每批处理20只股票
delay = 0.5          # 每次API调用间隔0.5秒
years_back = 3       # 获取最近3年数据
```

### 数据筛选

```python
# 只获取重点关注的股票
important_stocks = ["000001.SZ", "000002.SZ", "600000.SH"]  # 根据需要调整
```

## 常见问题

### Q: 初始化时间很长怎么办？
A: 可以减少股票数量或年数，分批次进行初始化。

### Q: API调用失败怎么办？
A: 检查Tushare token配置和积分权限，适当增加延迟时间。

### Q: 数据不完整怎么办？
A: 部分股票可能没有完整的财务数据，这是正常现象。

### Q: 如何查看详细日志？
A: 查看 `stock_analysis.log` 文件获取详细的执行日志。

## 投资分析应用

### 基本面筛选

利用财务数据可以进行：

1. **盈利筛选**: ROE、净利润增长率
2. **成长筛选**: 营收增长率、利润增长率  
3. **价值筛选**: PE、PB、PEG
4. **质量筛选**: 现金流质量、资产负债率
5. **分红筛选**: 股息率、分红稳定性

### 示例分析脚本

```python
# 筛选高ROE且现金流良好的股票
SELECT 
    i.ts_code,
    i.n_income_attr_p/i.total_revenue as net_margin,
    c.n_cashflow_act/i.n_income_attr_p as cf_ratio
FROM income_data i
JOIN cashflow_data c ON i.ts_code = c.ts_code AND i.end_date = c.end_date
WHERE i.end_date = '2023-12-31'
    AND i.n_income_attr_p > 0
    AND c.n_cashflow_act > 0
    AND (c.n_cashflow_act/i.n_income_attr_p) > 0.8
ORDER BY net_margin DESC;
```

## 注意事项

⚠️ **重要提醒**：
- 财务数据通常滞后1-3个月
- 数据质量依赖于上市公司披露
- 建议结合多个维度综合分析
- 注意会计准则变更的影响

🎯 **投资建议**：
- 财务数据是基本面分析的基础
- 建议结合技术面和消息面分析
- 关注数据的连续性和一致性
- 重视现金流量表的分析价值
