# 数据同步模块 (Data Synchronization Module)

本目录包含了每日定时拉取和更新数据的同步脚本。

## 核心脚本

- `daily_sync.py`: 核心同步逻辑实现，包含股票、指数、ETF、同花顺概念及财务数据的同步功能。
- `run_sync.py`: 命令行入口工具，支持灵活选择同步内容。

## 使用说明

### 1. 手动运行同步

同步所有数据：
```bash
python sync_data/run_sync.py --all
```

仅同步股票日线和指数数据：
```bash
python sync_data/run_sync.py --stock-daily --index --days 5
```

同步最近 2 年的财务数据：
```bash
python sync_data/run_sync.py --financial --years 2
```

### 2. 设置定时任务 (Crontab)

建议在每个交易日收盘后（如 18:30）运行同步脚本。可以使用 `crontab -e` 添加以下配置：

```cron
# 每天 18:30 自动执行全量数据同步
30 18 * * 1-5 /usr/bin/python3 /home/leonfyang/workspace/leontest/sync_data/run_sync.py --all >> /home/leonfyang/workspace/leontest/sync_data/sync.log 2>&1
```

## 数据内容说明

1. **股票基础信息**: 每日刷新上市状态和基本属性。
2. **行情数据 (日线/周线)**: 采用增量同步方式，默认回溯 5 天以确保数据完整性。
3. **指数数据**: 包含指数基本面、行情及主要指数权重。
4. **ETF 数据**: 包含 ETF 列表及其日线行情。
5. **同花顺概念**: 定期更新同花顺行业/概念分类及其成分股。
6. **财务数据**: 包含利润表、现金流量表及分红数据。

## 注意事项

- 财务数据和同花顺成分股同步耗时较长，且受 Tushare 积分限制，请根据实际积分情况调整频率。
- 同步脚本依赖 `config.py` 中的 Tushare Token 和 MySQL 配置。

