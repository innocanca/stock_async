#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
初始化 Tushare 注册接口，并提供通用探测能力。

示例:
python init_data/init_tushare_stock_data_interfaces.py --topic stock --list
python init_data/init_tushare_stock_data_interfaces.py --topic etf --category 行情数据 --only-fetchable
python init_data/init_tushare_stock_data_interfaces.py --topic index --interface index_daily --params-json '{"ts_codes":"000001.SH,000300.SH","start_date":"20260101","end_date":"20260324"}'
"""

import argparse
import json
import os
import sys
import time
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import StockDatabase
from fetcher import StockDataFetcher
from log_config import get_logger
from tushare_stock_data_registry import (
    get_tushare_interface,
    get_tushare_interface_categories,
)

logger = get_logger(__name__)

NON_RETRYABLE_INTERFACE_ERRORS = {
    "permission_denied",
    "missing_param",
    "invalid_api",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tushare 接口初始化/探测工具")
    parser.add_argument(
        "--topic",
        default="stock",
        help="专题范围，可选 stock/etf/index，默认 stock",
    )
    parser.add_argument("--list", action="store_true", help="列出已注册的专题接口")
    parser.add_argument("--category", help="按分类筛选，如：基础数据/行情数据/财务数据")
    parser.add_argument(
        "--only-fetchable",
        action="store_true",
        help="仅列出当前支持通用抓取的接口",
    )
    parser.add_argument(
        "--interface",
        help="抓取指定接口，支持 key/title/api_name 三种写法",
    )
    parser.add_argument(
        "--fields",
        help="可选字段列表，原样透传给 Tushare",
    )
    parser.add_argument(
        "--params-json",
        help='接口参数 JSON，例如: \'{"trade_date":"20260105"}\'',
    )
    parser.add_argument(
        "--output",
        help="将抓取结果保存到文件；.csv 保存 CSV，其他后缀保存 JSON",
    )
    parser.add_argument(
        "--to-db",
        action="store_true",
        help="将抓取结果自动建表并写入 MySQL",
    )
    parser.add_argument(
        "--table-name",
        help="指定目标表名；默认使用 ts_raw_<interface_key>",
    )
    parser.add_argument(
        "--table-prefix",
        default="ts_raw",
        help="自动表名的前缀，默认 ts_raw",
    )
    parser.add_argument(
        "--unique-keys",
        help="唯一键字段，逗号分隔，例如 ts_code,trade_date",
    )
    parser.add_argument(
        "--iterate-trade-dates",
        action="store_true",
        help="当传入 start_date/end_date 时，按交易日逐天抓取并合并结果",
    )
    parser.add_argument(
        "--calendar-exchange",
        default="SSE",
        help="按交易日逐天抓取时使用的交易所日历，默认 SSE",
    )
    return parser


def print_interfaces(fetcher: StockDataFetcher, topic: str = None, category: str = None, only_fetchable: bool = False) -> None:
    interfaces = fetcher.list_registered_interfaces(
        topic=topic,
        category=category,
        only_fetchable=only_fetchable,
    )

    if not interfaces:
        logger.warning("没有匹配到任何接口")
        return

    logger.info(f"共匹配到 {len(interfaces)} 个接口")
    for item in interfaces:
        status = item.get("status", "unknown")
        api_name = item.get("api_name") or "-"
        note = item.get("note") or ""
        print(
            f"[{item['category']}] {item['title']} | key={item['key']} | "
            f"api_name={api_name} | status={status}"
        )
        if note:
            print(f"  note: {note}")


def fetch_interface(
    fetcher: StockDataFetcher,
    identifier: str,
    topic: str = None,
    fields: str = None,
    params_json: str = None,
    output: str = None,
    to_db: bool = False,
    table_name: str = None,
    table_prefix: str = "ts_raw",
    unique_keys: str = None,
    iterate_trade_dates: bool = False,
    calendar_exchange: str = "SSE",
) -> int:
    params = {}
    if params_json:
        params = json.loads(params_json)
    interface = get_tushare_interface(identifier, topic=topic)
    if interface is None:
        raise ValueError(f"未找到已注册的 Tushare 接口: {identifier}")

    if iterate_trade_dates and to_db and interface.persistence_mode == "dynamic":
        persisted_rows = persist_interface_by_trade_dates(
            fetcher=fetcher,
            identifier=identifier,
            fields=fields,
            params=params,
            table_name=table_name,
            table_prefix=table_prefix,
            unique_keys=unique_keys,
            calendar_exchange=calendar_exchange,
        )
        if persisted_rows <= 0:
            logger.warning("接口返回为空")
            return 1
        logger.info(f"抓取并分批落库成功，共 {persisted_rows} 条记录")
        return 0

    if iterate_trade_dates:
        df = fetch_interface_by_trade_dates(
            fetcher=fetcher,
            identifier=identifier,
            fields=fields,
            params=params,
            calendar_exchange=calendar_exchange,
        )
    elif interface.fetch_strategy == "week_end_dates":
        df = fetch_interface_by_week_end_dates(
            fetcher=fetcher,
            identifier=identifier,
            fields=fields,
            params=params,
            calendar_exchange=calendar_exchange,
        )
    else:
        df = fetcher.fetch_registered_interface(
            identifier=identifier,
            fields=fields,
            **params,
        )

    if df is None or df.empty:
        logger.warning("接口返回为空")
        return 1

    logger.info(f"抓取成功，共 {len(df)} 条记录")
    preview = df.head(10)
    print(preview.to_string(index=False))

    if to_db:
        persist_to_database(
            identifier=identifier,
            df=df,
            table_name=table_name,
            table_prefix=table_prefix,
            unique_keys=unique_keys,
        )

    if output:
        if output.lower().endswith(".csv"):
            df.to_csv(output, index=False)
        else:
            df.to_json(output, orient="records", force_ascii=False, date_format="iso")
        logger.info(f"结果已保存到: {output}")

    return 0


def infer_unique_keys(df, explicit_unique_keys: str = None) -> list:
    if explicit_unique_keys:
        return [item.strip() for item in explicit_unique_keys.split(",") if item.strip()]

    candidate_groups = [
        ["ts_code", "trade_date"],
        ["ts_code", "ann_date"],
        ["ts_code", "end_date"],
        ["index_code", "con_code", "trade_date"],
        ["trade_date", "ts_code", "hm_name"],
        ["trade_date", "ts_code", "rank_time"],
        ["month", "broker", "ts_code"],
        ["ts_code", "namechange_date"],
    ]
    for group in candidate_groups:
        if all(column in df.columns for column in group):
            return group
    return []


def fetch_interface_by_trade_dates(
    fetcher: StockDataFetcher,
    identifier: str,
    fields: str,
    params: dict,
    calendar_exchange: str = "SSE",
):
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    if not start_date or not end_date:
        raise ValueError("按交易日逐天抓取时，必须提供 start_date 和 end_date")

    base_params = {
        key: value
        for key, value in params.items()
        if key not in {"start_date", "end_date", "trade_date"}
    }

    trade_cal = fetcher.get_trade_calendar(
        start_date=start_date,
        end_date=end_date,
        exchange=calendar_exchange,
    )
    if trade_cal is None or trade_cal.empty:
        return None

    trading_days = trade_cal["cal_date"].astype(str).tolist()
    total_days = len(trading_days)
    all_frames = []
    abort_remaining_days = False

    for index, trade_date in enumerate(trading_days, 1):
        if abort_remaining_days:
            break

        logger.info(f"按交易日抓取 {identifier}: {trade_date} ({index}/{total_days})")

        day_df = None
        for attempt in range(3):
            day_df = fetcher.fetch_registered_interface(
                identifier=identifier,
                fields=fields,
                trade_date=trade_date,
                **base_params,
            )
            if day_df is not None and not day_df.empty:
                break

            last_error = fetcher.get_last_interface_error()
            if not last_error:
                break

            error_type = last_error.get("type")
            if error_type in NON_RETRYABLE_INTERFACE_ERRORS:
                logger.warning(
                    f"{identifier} 因 `{error_type}` 跳过剩余交易日: {last_error.get('message')}"
                )
                abort_remaining_days = True
                break

            if error_type == "rate_limit" and attempt < 2:
                wait_seconds = 5 * (attempt + 1)
                logger.warning(
                    f"{identifier} 命中频率限制，{wait_seconds} 秒后重试当前交易日: {trade_date}"
                )
                time.sleep(wait_seconds)
                continue

            if error_type == "rate_limit":
                logger.warning(f"{identifier} 持续命中频率限制，跳过剩余交易日")
                abort_remaining_days = True
                break

            break

        if day_df is not None and not day_df.empty:
            all_frames.append(day_df)

        if not abort_remaining_days and index < total_days:
            time.sleep(0.7)

    if not all_frames:
        return None

    merged_df = pd.concat(all_frames, ignore_index=True)
    return dedupe_interface_dataframe(merged_df)


def dedupe_interface_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """按常见主键字段对接口结果去重。"""
    dedupe_keys = [
        column for column in ["ts_code", "trade_date", "rank_time", "hm_name", "index_code", "con_code"]
        if column in df.columns
    ]
    if dedupe_keys:
        return df.drop_duplicates(subset=dedupe_keys, keep="last")
    return df.drop_duplicates()


def persist_interface_by_trade_dates(
    fetcher: StockDataFetcher,
    identifier: str,
    fields: str,
    params: dict,
    table_name: str = None,
    table_prefix: str = "ts_raw",
    unique_keys: str = None,
    calendar_exchange: str = "SSE",
    flush_every_days: int = 5,
) -> int:
    """
    按交易日抓取并分批落库，避免低配机器把所有结果一次性堆在内存中。
    """
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    if not start_date or not end_date:
        raise ValueError("按交易日逐天分批落库时，必须提供 start_date 和 end_date")

    base_params = {
        key: value
        for key, value in params.items()
        if key not in {"start_date", "end_date", "trade_date"}
    }

    trade_cal = fetcher.get_trade_calendar(
        start_date=start_date,
        end_date=end_date,
        exchange=calendar_exchange,
    )
    if trade_cal is None or trade_cal.empty:
        return 0

    trading_days = trade_cal["cal_date"].astype(str).tolist()
    total_days = len(trading_days)
    batch_frames = []
    flushed_rows = 0
    abort_remaining_days = False

    for index, trade_date in enumerate(trading_days, 1):
        if abort_remaining_days:
            break

        logger.info(f"按交易日抓取并落库 {identifier}: {trade_date} ({index}/{total_days})")

        day_df = None
        for attempt in range(3):
            day_df = fetcher.fetch_registered_interface(
                identifier=identifier,
                fields=fields,
                trade_date=trade_date,
                **base_params,
            )
            if day_df is not None and not day_df.empty:
                break

            last_error = fetcher.get_last_interface_error()
            if not last_error:
                break

            error_type = last_error.get("type")
            if error_type in NON_RETRYABLE_INTERFACE_ERRORS:
                logger.warning(
                    f"{identifier} 因 `{error_type}` 跳过剩余交易日: {last_error.get('message')}"
                )
                abort_remaining_days = True
                break

            if error_type == "rate_limit" and attempt < 2:
                wait_seconds = 5 * (attempt + 1)
                logger.warning(
                    f"{identifier} 命中频率限制，{wait_seconds} 秒后重试当前交易日: {trade_date}"
                )
                time.sleep(wait_seconds)
                continue

            if error_type == "rate_limit":
                logger.warning(f"{identifier} 持续命中频率限制，跳过剩余交易日")
                abort_remaining_days = True
                break

            break

        if day_df is not None and not day_df.empty:
            batch_frames.append(day_df)

        should_flush = (
            len(batch_frames) >= flush_every_days
            or index == total_days
            or abort_remaining_days
        )
        if should_flush and batch_frames:
            batch_df = dedupe_interface_dataframe(pd.concat(batch_frames, ignore_index=True))
            persist_to_database(
                identifier=identifier,
                df=batch_df,
                table_name=table_name,
                table_prefix=table_prefix,
                unique_keys=unique_keys,
            )
            flushed_rows += len(batch_df)
            logger.info(
                f"{identifier} 已分批落库 {flushed_rows} 条，"
                f"当前批次 {len(batch_df)} 条"
            )
            batch_frames = []

        if not abort_remaining_days and index < total_days:
            time.sleep(0.7)

    return flushed_rows


def fetch_interface_by_week_end_dates(
    fetcher: StockDataFetcher,
    identifier: str,
    fields: str,
    params: dict,
    calendar_exchange: str = "SSE",
):
    start_date = params.get("start_date")
    end_date = params.get("end_date")
    if not start_date or not end_date:
        raise ValueError("按周末交易日抓取时，必须提供 start_date 和 end_date")

    trade_cal = fetcher.get_trade_calendar(
        start_date=start_date,
        end_date=end_date,
        exchange=calendar_exchange,
    )
    if trade_cal is None or trade_cal.empty:
        return None

    calendar = trade_cal.copy()
    calendar["cal_dt"] = pd.to_datetime(calendar["cal_date"], format="%Y%m%d")
    iso = calendar["cal_dt"].dt.isocalendar()
    calendar["year"] = iso.year
    calendar["week"] = iso.week
    week_end_dates = (
        calendar.groupby(["year", "week"])["cal_dt"]
        .max()
        .sort_values()
        .dt.strftime("%Y%m%d")
        .tolist()
    )
    total_weeks = len(week_end_dates)
    all_frames = []

    base_params = {
        key: value
        for key, value in params.items()
        if key not in {"start_date", "end_date", "trade_date"}
    }

    for index, trade_date in enumerate(week_end_dates, 1):
        logger.info(f"按周末交易日抓取 {identifier}: {trade_date} ({index}/{total_weeks})")
        week_df = fetcher.fetch_registered_interface(
            identifier=identifier,
            fields=fields,
            trade_date=trade_date,
            **base_params,
        )
        if week_df is not None and not week_df.empty:
            all_frames.append(week_df)
        if index < total_weeks:
            time.sleep(0.7)

    if not all_frames:
        return None

    merged_df = pd.concat(all_frames, ignore_index=True)
    return dedupe_interface_dataframe(merged_df)


def build_default_table_name(identifier: str, table_prefix: str) -> str:
    interface = get_tushare_interface(identifier)
    interface_key = interface.key if interface else identifier
    if interface and interface.target_table:
        return interface.target_table
    safe_key = interface_key.replace("-", "_").replace(" ", "_")
    return f"{table_prefix}_{safe_key}"


def persist_to_database(
    identifier: str,
    df,
    table_name: str = None,
    table_prefix: str = "ts_raw",
    unique_keys: str = None,
) -> None:
    interface = get_tushare_interface(identifier)
    resolved_table_name = table_name or build_default_table_name(identifier, table_prefix)
    resolved_unique_keys = infer_unique_keys(
        df,
        unique_keys or (interface.unique_keys if interface else None),
    )
    table_comment = (
        f"Tushare接口落库表: {interface.title} ({interface.api_name})"
        if interface else f"Tushare接口落库表: {identifier}"
    )

    logger.info(f"准备落库到数据表: {resolved_table_name}")
    if resolved_unique_keys:
        logger.info(f"使用唯一键: {', '.join(resolved_unique_keys)}")
    else:
        logger.warning("未识别到唯一键，将按纯插入方式写入")

    db = StockDatabase()
    if not db.create_database():
        logger.warning("创建数据库失败，将尝试直接连接既有数据库继续执行")

    with db:
        if not db.connection:
            raise RuntimeError("数据库连接失败，请检查 config.py 中的 MYSQL_CONFIG")

        if interface and interface.persistence_mode == "fixed":
            create_method = getattr(db, interface.create_method or "", None)
            insert_method = getattr(db, interface.insert_method or "", None)
            if not create_method or not insert_method:
                raise RuntimeError(f"固定表落库配置不完整: {identifier}")

            if resolved_table_name != interface.target_table:
                logger.warning(
                    f"固定表接口 `{identifier}` 将写入既有表 `{interface.target_table}`，忽略自定义表名 `{resolved_table_name}`"
                )

            if not create_method():
                raise RuntimeError("固定表建表失败")
            if not insert_method(df):
                raise RuntimeError("固定表入库失败")
            logger.info(f"接口数据已成功落库到 `{interface.target_table}`")
            return

        if not db.create_dynamic_table(
            table_name=resolved_table_name,
            df=df,
            unique_keys=resolved_unique_keys,
            table_comment=table_comment,
        ):
            raise RuntimeError("动态建表失败")

        if not db.insert_dynamic_data(
            table_name=resolved_table_name,
            df=df,
            unique_keys=resolved_unique_keys,
        ):
            raise RuntimeError("动态入库失败")

    logger.info(f"接口数据已成功落库到 `{resolved_table_name}`")


def main() -> int:
    args = build_parser().parse_args()
    fetcher = StockDataFetcher()

    if args.interface:
        return fetch_interface(
            fetcher=fetcher,
            identifier=args.interface,
            topic=args.topic,
            fields=args.fields,
            params_json=args.params_json,
            output=args.output,
            to_db=args.to_db,
            table_name=args.table_name,
            table_prefix=args.table_prefix,
            unique_keys=args.unique_keys,
            iterate_trade_dates=args.iterate_trade_dates,
            calendar_exchange=args.calendar_exchange,
        )

    if args.list or not args.interface:
        categories = get_tushare_interface_categories(topic=args.topic)
        if args.category and args.category not in categories:
            logger.warning(
                f"未知分类: {args.category}，可选值: {', '.join(categories)}"
            )
        print_interfaces(
            fetcher=fetcher,
            topic=args.topic,
            category=args.category,
            only_fetchable=args.only_fetchable,
        )
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
