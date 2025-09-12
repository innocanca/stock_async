# 股票数据获取和存储系统

基于Tushare API的股票数据获取和MySQL存储系统，支持批量获取股票日线数据并进行持久化存储。

## 功能特点

- 🔥 **自动获取A股主板数据** - 不指定股票代码时自动获取所有主板股票
- ✅ 使用Tushare Pro API获取股票日线数据
- ✅ 支持批量获取多只股票数据（带进度显示）
- ✅ 自动创建MySQL数据库和数据表
- ✅ 数据去重和更新机制
- ✅ 完整的日志记录
- ✅ 命令行参数支持
- ✅ 数据查询和统计功能
- ✅ 错误处理和重试机制
- ✅ API调用频率控制和批次处理
- ✅ 缓存机制和备用股票列表
- ✅ 两种数据获取模式（ts_code/trade_date）
- 🔥 **配置文件支持** - 预设8种配置模式，简化命令行操作

## 系统要求

- Python 3.8+
- MySQL 5.7+ 或 MySQL 8.0+
- 有效的Tushare Pro账户和Token

## 安装说明

### 1. 克隆项目
```bash
git clone <repository-url>
cd stock-data-system
```

### 2. 安装依赖
```bash
pip install -r requirements.txt
```

### 3. 配置设置

编辑 `config.py` 文件，设置以下配置：

```python
# Tushare API Token（必须）
TUSHARE_TOKEN = "your_tushare_token_here"

# MySQL数据库配置
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'your_password',
    'database': 'stock_data',
    'charset': 'utf8mb4'
}
```

### 4. 获取Tushare Token

1. 访问 [Tushare官网](https://tushare.pro/) 注册账户
2. 完成邮箱验证和手机号绑定
3. 在个人中心获取API Token
4. 将Token填入 `config.py` 中的 `TUSHARE_TOKEN` 字段

### 5. 准备MySQL数据库

确保MySQL服务正在运行，并且配置文件中的数据库连接信息正确。程序会自动创建数据库和数据表。

## 使用方法

### 基本用法

```bash
# 🔥 获取所有A股主板数据（新功能！）
python main.py

# 获取指定股票的数据
python main.py --codes 000001.SZ 000002.SZ 600000.SH

# 指定日期范围
python main.py --start-date 2024-01-01 --end-date 2024-03-31

# 创建数据库和数据表
python main.py --create-db
```

### 🚀 A股主板数据获取（新功能）

```bash
# 获取所有A股主板股票数据（不指定--codes时自动开启）
python main.py --start-date 2024-01-01

# 限制获取数量用于测试（推荐先测试）
python main.py --limit 10 --start-date 2024-09-01

# 调整批次大小和延迟，适应API限制
python main.py --limit 50 --batch-size 20 --delay 0.2

# 获取最新交易日的所有主板股票数据
python main.py --latest --limit 20
```

### 📊 两种数据获取模式

#### 按股票获取历史数据（ts_code模式）
```bash
# 获取个股历史数据 - 适合技术分析
python main.py --codes 600519.SH --start-date 2024-01-01

# 获取多只股票历史数据
python main.py --codes 600519.SH 000858.SZ 000002.SZ
```

#### 按交易日获取横截面数据（trade_date模式）
```bash
# 获取特定交易日的股票数据 - 适合横截面分析
python main.py --trade-date 2024-09-12 --codes 600519.SH 000002.SZ

# 获取最新交易日数据
python main.py --latest --codes 600519.SH 000002.SZ
```

### 高级用法

```bash
# 查询数据库中的数据
python main.py --query

# 查看数据库统计信息
python main.py --stats

# 大批量数据获取（谨慎使用）
python main.py --batch-size 30 --delay 0.15 --start-date 2024-01-01

# 组合使用多个参数
python main.py --codes 000001.SZ 600519.SH --start-date 2024-01-01 --end-date 2024-12-31
```

### 🔥 配置文件使用（新功能）

配置文件预设了8种常用的参数组合，极大简化了命令行操作：

```bash
# 🧪 快速测试（5只股票，近期数据）
python main.py --config test

# 📊 中等规模获取（50只股票，半年数据）
python main.py --config medium

# 📈 获取最新交易日数据（30只股票）
python main.py --config latest

# 🚀 大规模获取（所有主板股票，全年数据）
python main.py --config large

# 📋 仅查询数据库（不获取新数据）
python main.py --config query_only

# 📊 仅显示统计信息
python main.py --config stats_only
```

#### 配置覆盖机制
命令行参数优先级高于配置文件：
```bash
# 使用test配置，但修改股票数量为10只
python main.py --config test --limit 10

# 使用medium配置，但指定特定股票
python main.py --config medium --codes 600519.SH 000858.SZ
```

#### 查看当前配置
```bash
# 显示默认配置
python main.py --show-config

# 显示指定配置模式
python main.py --config test --show-config
```

#### 📋 预设配置模式详情

| 配置模式 | 股票数量 | 时间范围 | 批次大小 | 延迟 | 适用场景 |
|----------|----------|----------|----------|------|----------|
| `test` | 5只 | 2024-09-01~10 | 10 | 0.1s | 🧪 首次测试 |
| `small` | 20只 | 2024-08-01~ | 10 | 0.15s | 📊 小规模分析 |
| `medium` | 50只 | 2024-06-01~ | 20 | 0.2s | 📈 中等规模回测 |
| `large` | 不限制 | 2024-01-01~ | 30 | 0.15s | 🚀 大规模数据建库 |
| `latest` | 30只 | 最新交易日 | - | 0.1s | ⚡ 实时监控 |
| `query_only` | - | - | - | - | 📋 仅查询数据库 |
| `stats_only` | - | - | - | - | 📊 仅显示统计 |
| `default` | 不限制 | 2024-01-01~ | 50 | 0.1s | 🎯 标准配置 |

## 数据表结构

系统会自动创建 `daily_data` 表，包含以下字段：

| 字段名 | 类型 | 说明 |
|--------|------|------|
| id | INT | 主键，自增 |
| ts_code | VARCHAR(20) | 股票代码 |
| trade_date | DATE | 交易日期 |
| open | DECIMAL(10,2) | 开盘价 |
| high | DECIMAL(10,2) | 最高价 |
| low | DECIMAL(10,2) | 最低价 |
| close | DECIMAL(10,2) | 收盘价 |
| pre_close | DECIMAL(10,2) | 昨收价 |
| change_pct | DECIMAL(8,4) | 涨跌幅(%) |
| change_amount | DECIMAL(10,2) | 涨跌额 |
| vol | DECIMAL(15,2) | 成交量(手) |
| amount | DECIMAL(20,2) | 成交额(千元) |
| created_at | TIMESTAMP | 创建时间 |
| updated_at | TIMESTAMP | 更新时间 |

## 🔥 A股主板数据获取详解

### 功能说明
当你不指定 `--codes` 参数时，程序会自动获取所有A股主板股票数据，包括：
- **上交所主板**：600xxx、601xxx、603xxx、605xxx等
- **深交所主板**：000xxx、001xxx、002xxx等（排除300开头的创业板）

### 智能股票列表获取
1. **API优先**：首先尝试从Tushare API获取最新股票列表
2. **缓存机制**：成功获取后缓存到本地文件（有效期7天）
3. **备用方案**：API失败时使用内置的80只主要大盘股列表

### 批量处理优化
- **进度显示**：每10只股票显示一次进度
- **批次控制**：可自定义批次大小和延迟时间
- **成功率统计**：显示获取成功的股票数量和比例
- **错误处理**：单只股票失败不影响整体进程

### 参数说明
| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--limit` | 无限制 | 限制获取的股票数量（用于测试） |
| `--batch-size` | 50 | 批次大小，每批后会长休眠2秒 |
| `--delay` | 0.1 | 每次API调用的延迟时间（秒） |

### 使用建议
1. **首次使用**：建议先用 `--limit 10` 测试
2. **API限制**：免费用户有调用频率限制，可调整 `--delay` 参数
3. **大批量获取**：建议在非交易时间进行，避免影响实时数据获取

## 项目结构

```
stock-data-system/
├── main.py                      # 🚀 主程序入口（支持A股主板自动获取+配置文件）
├── config.py                   # ⚙️ 配置文件（包含8种预设配置模式）
├── database.py                 # 🗄️ 数据库操作类
├── tushare_demo.py             # 📖 完整功能演示程序
├── mainboard_demo.py           # 📋 主板数据获取演示
├── config_demo.py              # 🔧 配置文件使用演示
├── example.py                  # 🔧 快速开始示例
├── requirements.txt            # 📦 依赖包列表
├── README.md                   # 📚 项目说明文档
├── main_board_stocks_cache.txt # 💾 股票列表缓存文件（自动生成）
└── stock_data.log             # 📜 程序运行日志（自动生成）
```

## 核心类说明

### StockDataFetcher
负责从Tushare API获取股票数据的核心类。

主要方法：
- `get_daily_data()`: 获取单只股票日线数据
- `get_multiple_stocks_data()`: 批量获取多只股票数据
- `get_stock_basic()`: 获取股票基础信息

### StockDatabase
负责MySQL数据库操作的核心类。

主要方法：
- `create_database()`: 创建数据库
- `create_daily_table()`: 创建数据表
- `insert_daily_data()`: 批量插入数据
- `query_data()`: 查询数据
- `get_stats()`: 获取统计信息

## 常见问题

### Q1: Tushare API调用频率限制
A: 免费用户有调用频率限制，程序已内置休眠机制。如需更高频率，请考虑升级Tushare会员。

### Q2: MySQL连接失败
A: 请检查：
- MySQL服务是否启动
- 数据库连接配置是否正确
- 用户是否有足够的权限

### Q3: 数据重复问题
A: 程序使用 `ON DUPLICATE KEY UPDATE` 机制，相同股票和日期的数据会自动更新而不重复插入。

### Q4: 日期格式问题
A: 支持多种日期格式输入：
- YYYY-MM-DD (如: 2024-01-01)
- YYYYMMDD (如: 20240101)
- YYYY/MM/DD (如: 2024/01/01)

## 日志文件

程序运行时会生成 `stock_data.log` 日志文件，记录详细的运行信息，包括：
- 数据获取过程
- 数据库操作结果
- 错误和异常信息
- 统计信息

## 扩展建议

1. **数据分析模块**: 基于存储的数据进行技术分析
2. **定时任务**: 使用cron或其他调度工具实现定期数据更新
3. **Web界面**: 开发Web界面进行数据查看和管理
4. **数据备份**: 实现数据库定期备份功能
5. **性能优化**: 对大量数据的场景进行性能优化

## 许可证

本项目采用MIT许可证。

## 贡献

欢迎提交Issue和Pull Request来改进这个项目！
