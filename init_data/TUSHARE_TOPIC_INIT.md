# Tushare 通用初始化入口

## 推荐入口

当前项目已经统一为以下两个通用入口：

- `init_data/bulk_init_tushare_stock_data.py`
- `init_data/init_tushare_stock_data_interfaces.py`

它们现在同时支持：

- `stock` 股票数据专题
- `etf` ETF 专题
- `index` 指数专题

## 生成模板

```bash
# 股票专题
python init_data/bulk_init_tushare_stock_data.py \
  --write-template init_data/tushare_bulk_init.template.json \
  --topics stock

# ETF 专题
python init_data/bulk_init_tushare_stock_data.py \
  --write-template init_data/tushare_etf_init.template.json \
  --topics etf

# 指数专题
python init_data/bulk_init_tushare_stock_data.py \
  --write-template init_data/tushare_index_init.template.json \
  --topics index
```

## 执行批量初始化

```bash
python init_data/bulk_init_tushare_stock_data.py \
  --config init_data/tushare_bulk_init.template.json

python init_data/bulk_init_tushare_stock_data.py \
  --config init_data/tushare_etf_init.template.json

python init_data/bulk_init_tushare_stock_data.py \
  --config init_data/tushare_index_init.template.json
```

## 单接口探测/落库

```bash
# 查看 ETF 已注册接口
python init_data/init_tushare_stock_data_interfaces.py --topic etf --list

# 抓取指数日线并落库到固定表
python init_data/init_tushare_stock_data_interfaces.py \
  --topic index \
  --interface index_daily \
  --params-json '{"ts_codes":"000001.SH,000300.SH","start_date":"20260101","end_date":"20260324"}' \
  --to-db
```

## 兼容性说明

- `stock` 专题默认继续写入 `ts_raw_*` 动态表。
- `etf` 与 `index` 专题默认继续写入既有固定表，例如 `etf_basic`、`etf_daily`、`index_basic`、`index_daily`。
- 旧的 ETF/指数固定初始化脚本已被新的通用入口替代。
