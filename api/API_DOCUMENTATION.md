# Stock Analyzer API 接口文档

本文档汇总了后端服务提供的所有股票策略筛选接口。

## 1. 基础信息
- **服务 URL**: `http://localhost:8000`
- **交互式文档 (Swagger UI)**: [http://localhost:8000/docs](http://localhost:8000/docs) (推荐用于在线测试)

---

## 2. 策略筛选接口

### 2.1 周线连续阳线策略
查询周线级别连续收阳线的千亿市值主板股票。该策略适合寻找走势稳健、处于上升趋势的大盘蓝筹股。

- **接口路径**: `/consecutive_yang_lines`
- **HTTP方法**: `GET`
- **请求参数**:
    - `min_consecutive` (int, 可选): 最少连续阳线周数，默认 `3`。
- **返回字段**: `ts_code`, `名称`, `市值(亿)`, `行业`, `consecutive_yang_weeks`, `latest_close`, `total_return_during_yang` 等。
- **返回示例**:
```json
{
  "count": 1,
  "data": [
    {
      "ts_code": "600519.SH",
      "stock_name": "贵州茅台",
      "market_cap": "20000+",
      "industry": "白酒",
      "consecutive_yang_weeks": 3,
      "latest_close": 1550.0,
      "total_return_during_yang": 4.5
    }
  ]
}
```

---

### 2.2 周线底部放量反转策略
查询前期连续下跌（至少 3 周）后，在最近一周出现放量收阳反转信号的主板股票。适合抄底潜伏。

- **接口路径**: `/weekly_bottom_reversal`
- **HTTP方法**: `GET`
- **请求参数**:
    - `min_mv` (float, 可选): 最小总市值（万元），默认 `1000000` (100亿)。
    - `min_drop_weeks` (int, 可选): 反转前最少连续下跌周数，默认 `3`。
    - `vol_ratio` (float, 可选): 成交量放大倍数阈值，默认 `1.5`。
- **返回示例**:
```json
{
  "count": 1,
  "data": [
    {
      "ts_code": "600737.SH",
      "代码": "600737.SH",
      "名称": "中粮糖业",
      "市值(亿)": 366.8,
      "现价": 17.18,
      "本周涨幅%": 0.02,
      "放量倍数": 2.16,
      "连续下跌周数": 3,
      "最近周线日期": "2025-12-20"
    }
  ]
}
```

---

### 2.3 低 PE + 周线放量策略
筛选主板中市值较大、估值较低且本周出现成交量显著放大的股票。

- **接口路径**: `/low_pe_volume_surge`
- **HTTP方法**: `GET`
- **请求参数**:
    - `min_mv` (float, 可选): 最小总市值（万元），默认 `2000000` (200亿)。
    - `max_pe` (float, 可选): 最大市盈率 (TTM)，默认不限制。
    - `min_ratio` (float, 可选): 周线放量倍数阈值，默认 `1.3`。
- **返回字段**: `ts_code`, `名称`, `市值(亿)`, `PE(TTM)`, `现价`, `周放量倍数`, `是否刚启动`。

---

### 2.4 均线下大盘股策略
查询市值巨大（千亿级）且当前价格低于最近一年周线均价的股票。

- **接口路径**: `/large_cap_below_1y_avg_price`
- **HTTP方法**: `GET`
- **请求参数**:
    - `min_mv` (float, 可选): 最小总市值（万元），默认 `10000000` (1000亿)。
    - `max_pe` (float, 可选): 最大市盈率 (TTM)，默认 `30.0`。
- **返回字段**: `ts_code`, `name`, `total_mv_10k`, `pe_ttm`, `current_close`, `avg_close_1y`。

---

## 3. 数据查询接口

### 3.1 单股一年历史行情
获取指定股票最近 1 年的日线价格和成交量数据，常用于前端绘制 K 线图或趋势图。

- **接口路径**: `/price_volume_1y`
- **HTTP方法**: `GET`
- **请求参数**:
    - `ts_code` (str, 必选): 股票代码，如 `600519.SH`。
- **返回字段**: `trade_date`, `open`, `high`, `low`, `close`, `vol`, `amount`。

---

## 4. 统一响应格式说明
所有接口均返回标准 JSON 格式，结构如下：

```json
{
  "count": 10,
  "data": [
    { ... 数据对象1 ... },
    { ... 数据对象2 ... }
  ]
}
```
如果接口出错（如数据库连接失败），会返回相应的 `error` 字段及 `500` 状态码。

