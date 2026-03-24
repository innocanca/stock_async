#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tushare 专题接口批量初始化脚本。

能力：
1. 生成全量初始化模板配置
2. 根据配置批量抓取已注册接口
3. 自动落库到 MySQL

示例：
python init_data/bulk_init_tushare_stock_data.py --write-template init_data/tushare_bulk_init.template.json --topics stock
python init_data/bulk_init_tushare_stock_data.py --write-template init_data/tushare_etf_init.template.json --topics etf
python init_data/bulk_init_tushare_stock_data.py --write-template init_data/tushare_index_init.template.json --topics index
python init_data/bulk_init_tushare_stock_data.py --config init_data/tushare_bulk_init.template.json
"""

import argparse
import json
import os
import sys
from copy import deepcopy
from datetime import datetime
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from fetcher import StockDataFetcher
from init_data.init_tushare_stock_data_interfaces import (
    build_default_table_name,
    fetch_interface_by_trade_dates,
    fetch_interface_by_week_end_dates,
    persist_to_database,
)
from log_config import get_logger
from tushare_stock_data_registry import (
    get_tushare_interface,
    list_tushare_interfaces,
)

logger = get_logger(__name__)

SKIPPABLE_ERROR_TYPES = {
    "permission_denied",
    "missing_param",
    "invalid_api",
}


REALTIME_SAMPLE_KEYS = {
    "rt_daily",
    "rt_min",
    "realtime_tick",
    "realtime_deal",
    "realtime_rank",
    "ths_hot",
    "dc_hot",
}

DEFAULT_UNIQUE_KEYS = {
    "stock_basic": "ts_code",
    "trade_cal": "cal_date",
    "stk_st": "ts_code",
    "risk_warning_stocks": "ts_code,imp_date,pub_date",
    "hs_const": "ts_code,is_new",
    "namechange": "ts_code,start_date,end_date",
    "stock_company": "ts_code",
    "stk_managers": "ts_code,ann_date,name,title",
    "stk_rewards": "ts_code,ann_date,name,title",
    "bse_mapping": "o_code,n_code",
    "new_share": "ts_code,ipo_date",
    "stock_history_list": "ts_code,trade_date",
    "daily": "ts_code,trade_date",
    "weekly": "ts_code,trade_date",
    "monthly": "ts_code,trade_date",
    "pro_bar_adj": "ts_code,trade_date",
    "daily_basic": "ts_code,trade_date",
    "moneyflow_dc": "ts_code,trade_date",
    "ths_hot": "trade_date,ts_code,rank_time",
    "dc_hot": "trade_date,ts_code,rank_time",
    "hot_money_trade_detail": "trade_date,ts_code,hm_name",
}

DATE_RANGE_ITERATION_KEYS = {
    "daily_share",
    "daily",
    "weekly",
    "monthly",
    "week_month_daily_refresh",
    "week_month_adj_daily_refresh",
    "adj_factor",
    "daily_basic",
    "stk_limit",
    "suspend_d",
    "backup_quotes",
    "forecast",
    "express",
    "abnormal_movement",
    "serious_abnormal_movement",
    "exchange_key_security_notice",
    "top10_holders",
    "top10_floatholders",
    "pledge_stat",
    "pledge_detail",
    "repurchase",
    "share_float",
    "block_trade",
    "stk_holdernumber",
    "stk_holdertrade",
    "report_rc",
    "cyq_perf",
    "cyq_chips",
    "stk_factor_pro",
    "hk_hold",
    "stk_surv",
    "margin",
    "margin_detail",
    "margin_target",
    "seclending_summary",
    "seclending_finance_summary",
    "seclending_detail",
    "market_making_lending_summary",
    "moneyflow",
    "moneyflow_ths",
    "moneyflow_dc",
    "moneyflow_sector_ths",
    "moneyflow_ind_ths",
    "moneyflow_sector_dc",
    "moneyflow_mkt_dc",
    "moneyflow_hsgt",
    "top_list",
    "top_inst",
    "limit_list_ths",
    "limit_list_d",
    "limit_step",
    "limit_strong",
    "ths_daily",
    "dc_daily",
    "hot_money_trade_detail",
    "kpl_list",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Tushare 专题数据批量初始化脚本")
    parser.add_argument("--config", help="批量初始化配置文件(JSON)")
    parser.add_argument("--write-template", help="将默认模板写入指定路径")
    parser.add_argument("--topics", help="专题列表，逗号分隔，如 stock,etf,index；生成模板时默认 stock")
    parser.add_argument("--start-date", default="20260101", help="模板默认开始日期 YYYYMMDD")
    parser.add_argument("--end-date", default=datetime.now().strftime("%Y%m%d"), help="模板默认结束日期 YYYYMMDD")
    parser.add_argument("--month", help="模板默认月份 YYYYMM；不填则从 end_date 推导")
    parser.add_argument("--sample-ts-code", default="000001.SZ", help="需要样本股票代码的接口默认值")
    parser.add_argument("--calendar-exchange", default="SSE", help="逐交易日抓取时的交易所日历")
    parser.add_argument("--table-prefix", default="ts_raw", help="落库表名前缀")
    parser.add_argument("--stop-on-error", action="store_true", help="遇到错误立即停止")
    parser.add_argument("--interfaces", help="仅运行指定接口，逗号分隔")
    parser.add_argument("--categories", help="仅运行指定分类，逗号分隔")
    return parser


def default_month(end_date: str, explicit_month: str = None) -> str:
    if explicit_month:
        return explicit_month
    return end_date[:6]


def build_default_task(interface, global_config: dict) -> dict:
    key = interface.key
    task = {
        "id": key,
        "interface": key,
        "topic": interface.topic,
        "enabled": True,
        "fields": None,
        "params": {},
        "iterate_trade_dates": False,
        "calendar_exchange": global_config["calendar_exchange"],
        "table_name": build_default_table_name(key, global_config["table_prefix"]),
        "unique_keys": None,
        "note": interface.note or "",
    }

    start_date = global_config["start_date"]
    end_date = global_config["end_date"]
    sample_ts_code = global_config["sample_ts_code"]
    month = default_month(end_date, global_config.get("month"))

    if key == "etf_basic":
        task["params"] = {"list_status": "L"}
    elif key == "etf_daily":
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key == "index_basic":
        task["params"] = {}
    elif key == "index_daily":
        task["params"] = {"start_date": start_date, "end_date": end_date}
    elif key == "index_weekly":
        task["params"] = {"start_date": start_date, "end_date": end_date}
    elif key == "index_weight":
        task["params"] = {"start_date": start_date, "end_date": end_date}
    elif key == "index_dailybasic":
        task["params"] = {"start_date": start_date, "end_date": end_date}
    elif key == "broker_montly_pick":
        task["params"] = {"month": month}
        task["unique_keys"] = "month,broker,ts_code"
    elif key == "broker_earning_forecast":
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
        task["unique_keys"] = "ts_code,report_date,org_name,quarter"
    elif key == "hist_min":
        task["params"] = {
            "ts_code": sample_ts_code,
            "freq": "1min",
            "start_date": f"{start_date} 09:30:00",
            "end_date": f"{end_date} 15:00:00",
        }
        task["unique_keys"] = "ts_code,trade_time"
    elif key == "rt_min":
        task["params"] = {"ts_code": sample_ts_code, "freq": "1MIN"}
        task["unique_keys"] = "ts_code,time"
    elif key == "rt_daily":
        task["params"] = {"ts_code": "3*.SZ,6*.SH,0*.SZ,9*.BJ"}
        task["unique_keys"] = "ts_code,trade_time"
    elif key == "realtime_tick":
        task["params"] = {"ts_code": sample_ts_code, "src": "dc"}
        task["unique_keys"] = "ts_code,time"
    elif key == "realtime_deal":
        task["params"] = {"ts_code": sample_ts_code, "src": "dc"}
        task["unique_keys"] = "time,price,volume"
    elif key == "realtime_rank":
        task["params"] = {"src": "dc"}
        task["unique_keys"] = "ts_code,name"
    elif key == "week_month_daily_refresh":
        task["params"] = {"start_date": start_date, "end_date": end_date, "freq": "week"}
        task["iterate_trade_dates"] = True
        task["unique_keys"] = "ts_code,trade_date,freq"
    elif key == "week_month_adj_daily_refresh":
        task["params"] = {"start_date": start_date, "end_date": end_date, "freq": "week"}
        task["iterate_trade_dates"] = True
        task["unique_keys"] = "ts_code,trade_date,freq"
    elif key == "moneyflow_sector_dc":
        task["params"] = {"start_date": start_date, "end_date": end_date, "content_type": "概念"}
        task["iterate_trade_dates"] = True
        task["unique_keys"] = "trade_date,ts_code,content_type"
    elif key == "ths_hot":
        task["params"] = {"trade_date": end_date, "market": "热股", "is_new": "Y"}
        task["unique_keys"] = "trade_date,ts_code,rank_time"
    elif key == "dc_hot":
        task["params"] = {"trade_date": end_date, "market": "A股市场", "hot_type": "人气榜", "is_new": "Y"}
        task["unique_keys"] = "trade_date,ts_code,rank_time"
    elif key in DATE_RANGE_ITERATION_KEYS:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key in {"income", "balancesheet", "cashflow", "dividend", "fina_indicator", "fina_audit", "fina_mainbz", "disclosure_date"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
    elif key in {"daily_share", "daily", "weekly", "monthly", "adj_factor", "daily_basic", "stk_limit", "suspend_d", "backup_quotes"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key in {"top_list", "top_inst", "limit_list_ths", "limit_list_d", "limit_step", "limit_strong", "hot_money_trade_detail"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key in {"moneyflow", "moneyflow_ths", "moneyflow_dc", "moneyflow_sector_ths", "moneyflow_ind_ths", "moneyflow_mkt_dc", "moneyflow_hsgt"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key in {"ggt_daily", "ggt_monthly", "hsgt_top10", "ggt_top10"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True
    elif key in {"new_share", "stock_history_list"}:
        task["params"] = {"start_date": start_date, "end_date": end_date}
        task["iterate_trade_dates"] = True

    if task["unique_keys"] is None and interface.unique_keys:
        task["unique_keys"] = interface.unique_keys
    if task["unique_keys"] is None and key in DEFAULT_UNIQUE_KEYS:
        task["unique_keys"] = DEFAULT_UNIQUE_KEYS[key]

    if key in REALTIME_SAMPLE_KEYS:
        task["note"] = (task["note"] + " 使用实时/样本参数，建议交易时段执行。").strip()

    return task


def build_default_template(args: argparse.Namespace) -> dict:
    selected_topics = parse_csv_arg(args.topics) or {"stock"}
    global_config = {
        "topics": sorted(selected_topics),
        "start_date": args.start_date,
        "end_date": args.end_date,
        "month": default_month(args.end_date, args.month),
        "sample_ts_code": args.sample_ts_code,
        "calendar_exchange": args.calendar_exchange,
        "table_prefix": args.table_prefix,
        "stop_on_error": args.stop_on_error,
        "to_db": True,
    }

    tasks = [
        build_default_task(interface, global_config)
        for interface in list_tushare_interfaces(only_fetchable=True)
        if interface.topic in selected_topics
    ]

    return {
        "global": global_config,
        "tasks": tasks,
    }


def write_template(path: str, template: dict) -> None:
    with open(path, "w", encoding="utf-8") as file:
        json.dump(template, file, ensure_ascii=False, indent=2)
    logger.info(f"模板已写入: {path}")


def load_config(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)


def parse_csv_arg(value: str):
    if not value:
        return None
    return {item.strip() for item in value.split(",") if item.strip()}


def should_run_task(task: dict, interface, selected_interfaces, selected_categories, selected_topics) -> bool:
    if not task.get("enabled", True):
        return False
    if selected_interfaces and task["interface"] not in selected_interfaces:
        return False
    if selected_topics and interface.topic not in selected_topics:
        return False
    if selected_categories and interface.category not in selected_categories:
        return False
    return True


def fetch_dataframe_for_task(fetcher: StockDataFetcher, task: dict, interface):
    fields = task.get("fields")
    params = deepcopy(task.get("params", {}))

    if interface.key == "hs_const" and not params.get("hs_type"):
        frames = []
        for hs_type in ["SH", "SZ"]:
            df = fetcher.fetch_registered_interface(
                identifier=task["interface"],
                fields=fields,
                hs_type=hs_type,
                **params,
            )
            if df is not None and not df.empty:
                frames.append(df)
        if not frames:
            return None
        return pd.concat(frames, ignore_index=True).drop_duplicates()

    if interface.fetch_strategy == "week_end_dates":
        return fetch_interface_by_week_end_dates(
            fetcher=fetcher,
            identifier=task["interface"],
            fields=fields,
            params=params,
            calendar_exchange=task.get("calendar_exchange", "SSE"),
        )

    if task.get("iterate_trade_dates") or interface.fetch_strategy == "trade_dates":
        return fetch_interface_by_trade_dates(
            fetcher=fetcher,
            identifier=task["interface"],
            fields=fields,
            params=params,
            calendar_exchange=task.get("calendar_exchange", "SSE"),
        )

    return fetcher.fetch_registered_interface(
        identifier=task["interface"],
        fields=fields,
        **params,
    )


def run_tasks(config: dict, selected_interfaces=None, selected_categories=None, selected_topics=None) -> dict:
    fetcher = StockDataFetcher()
    summary = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "skipped": 0,
        "results": [],
    }

    global_config = config.get("global", {})
    to_db = global_config.get("to_db", True)
    stop_on_error = global_config.get("stop_on_error", False)

    for task in config.get("tasks", []):
        interface = get_tushare_interface(task["interface"], topic=task.get("topic"))
        if interface is None:
            summary["skipped"] += 1
            summary["results"].append({"interface": task["interface"], "status": "skipped", "reason": "unknown_interface"})
            continue

        if not should_run_task(task, interface, selected_interfaces, selected_categories, selected_topics):
            summary["skipped"] += 1
            summary["results"].append({"interface": task["interface"], "status": "skipped", "reason": "filtered_or_disabled"})
            continue

        summary["total"] += 1
        logger.info(f"开始批量初始化: {task['interface']} ({interface.title})")

        try:
            df = fetch_dataframe_for_task(fetcher, task, interface)
            if df is None or df.empty:
                last_error = fetcher.get_last_interface_error()
                if last_error and last_error.get("type") in SKIPPABLE_ERROR_TYPES:
                    summary["skipped"] += 1
                    summary["results"].append(
                        {
                            "interface": task["interface"],
                            "status": "skipped",
                            "reason": last_error.get("type"),
                            "error": last_error.get("message"),
                        }
                    )
                    logger.warning(
                        f"{task['interface']} 已跳过: {last_error.get('type')} - {last_error.get('message')}"
                    )
                    continue

                summary["failed"] += 1
                summary["results"].append({"interface": task["interface"], "status": "empty"})
                logger.warning(f"{task['interface']} 返回空数据")
                if stop_on_error:
                    break
                continue

            if to_db:
                persist_to_database(
                    identifier=task["interface"],
                    df=df,
                    table_name=task.get("table_name"),
                    table_prefix=global_config.get("table_prefix", "ts_raw"),
                    unique_keys=task.get("unique_keys"),
                )

            summary["success"] += 1
            summary["results"].append(
                {
                    "interface": task["interface"],
                    "status": "success",
                    "records": len(df),
                    "table_name": task.get("table_name") or interface.target_table,
                }
            )
            logger.info(f"{task['interface']} 初始化成功，记录数: {len(df)}")
        except Exception as e:
            summary["failed"] += 1
            summary["results"].append(
                {
                    "interface": task["interface"],
                    "status": "failed",
                    "error": str(e),
                }
            )
            logger.error(f"{task['interface']} 初始化失败: {e}")
            if stop_on_error:
                break

    return summary


def main() -> int:
    args = build_parser().parse_args()

    if args.write_template:
        template = build_default_template(args)
        write_template(args.write_template, template)
        return 0

    if not args.config:
        raise SystemExit("请提供 --config，或使用 --write-template 先生成模板")

    config = load_config(args.config)
    selected_interfaces = parse_csv_arg(args.interfaces)
    selected_categories = parse_csv_arg(args.categories)
    selected_topics = parse_csv_arg(args.topics)
    summary = run_tasks(
        config=config,
        selected_interfaces=selected_interfaces,
        selected_categories=selected_categories,
        selected_topics=selected_topics,
    )

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
