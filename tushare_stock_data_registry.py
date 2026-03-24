from dataclasses import asdict, dataclass
from typing import Dict, List, Optional


@dataclass(frozen=True)
class TushareStockInterface:
    key: str
    title: str
    category: str
    api_name: Optional[str] = None
    call_style: str = "query"
    status: str = "registry_only"
    note: str = ""
    topic: str = "stock"
    fetch_strategy: str = "direct"
    persistence_mode: str = "dynamic"
    target_table: Optional[str] = None
    create_method: Optional[str] = None
    insert_method: Optional[str] = None
    unique_keys: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


STOCK_DATA_INTERFACES: List[TushareStockInterface] = [
    # 基础数据
    TushareStockInterface("stock_basic", "股票列表", "基础数据", "stock_basic", status="implemented"),
    TushareStockInterface(
        "daily_share",
        "每日股本（盘前）",
        "基础数据",
        "stk_premarket",
        status="implemented",
        note="文档接口名为 stk_premarket。",
    ),
    TushareStockInterface("trade_cal", "交易日历", "基础数据", "trade_cal", status="implemented"),
    TushareStockInterface("stk_st", "ST股票列表", "基础数据", "stk_st", status="implemented"),
    TushareStockInterface(
        "risk_warning_stocks",
        "ST风险警示板股票",
        "基础数据",
        "st",
        status="implemented",
        note="文档接口名为 st。",
    ),
    TushareStockInterface("hs_const", "沪深港通股票列表", "基础数据", "hs_const", status="implemented"),
    TushareStockInterface("namechange", "股票曾用名", "基础数据", "namechange", status="implemented"),
    TushareStockInterface("stock_company", "上市公司基本信息", "基础数据", "stock_company", status="implemented"),
    TushareStockInterface("stk_managers", "上市公司管理层", "基础数据", "stk_managers", status="implemented"),
    TushareStockInterface("stk_rewards", "管理层薪酬和持股", "基础数据", "stk_rewards", status="implemented"),
    TushareStockInterface(
        "bse_mapping",
        "北交所新旧代码对照",
        "基础数据",
        "bse_mapping",
        status="implemented",
        note="文档接口名为 bse_mapping。",
    ),
    TushareStockInterface("new_share", "IPO新股上市", "基础数据", "new_share", status="implemented"),
    TushareStockInterface(
        "stock_history_list",
        "股票历史列表",
        "基础数据",
        "bak_basic",
        status="implemented",
        note="文档接口名为 bak_basic。",
    ),
    # 行情数据
    TushareStockInterface("daily", "历史日线", "行情数据", "daily", status="implemented"),
    TushareStockInterface(
        "rt_daily",
        "实时日线",
        "行情数据",
        "rt_k",
        status="implemented",
        note="文档接口名为 rt_k（A股实时日线）。",
    ),
    TushareStockInterface(
        "hist_min",
        "历史分钟",
        "行情数据",
        "stk_mins",
        status="implemented",
        note="文档接口名为 stk_mins。",
    ),
    TushareStockInterface(
        "rt_min",
        "实时分钟",
        "行情数据",
        "rt_min",
        status="implemented",
        note="文档接口名为 rt_min。",
    ),
    TushareStockInterface("weekly", "周线行情", "行情数据", "weekly", status="implemented"),
    TushareStockInterface("monthly", "月线行情", "行情数据", "monthly", status="implemented"),
    TushareStockInterface("pro_bar_adj", "复权行情", "行情数据", call_style="pro_bar", status="implemented", note="通过 ts.pro_bar 调用。"),
    TushareStockInterface(
        "week_month_daily_refresh",
        "周/月线行情(每日更新)",
        "行情数据",
        "stk_weekly_monthly",
        status="implemented",
        note="文档接口名为 stk_weekly_monthly。",
    ),
    TushareStockInterface(
        "week_month_adj_daily_refresh",
        "周/月线复权行情(每日更新)",
        "行情数据",
        "stk_week_month_adj",
        status="implemented",
        note="文档接口名为 stk_week_month_adj。",
    ),
    TushareStockInterface("adj_factor", "复权因子", "行情数据", "adj_factor", status="implemented"),
    TushareStockInterface(
        "realtime_tick",
        "实时Tick（爬虫）",
        "行情数据",
        "realtime_quote",
        call_style="sdk",
        status="implemented",
        note="文档接口名为 realtime_quote（实时盘口TICK快照）。",
    ),
    TushareStockInterface(
        "realtime_deal",
        "实时成交（爬虫）",
        "行情数据",
        "realtime_tick",
        call_style="sdk",
        status="implemented",
        note="文档接口名为 realtime_tick（实时成交数据）。",
    ),
    TushareStockInterface(
        "realtime_rank",
        "实时排名（爬虫）",
        "行情数据",
        "realtime_list",
        call_style="sdk",
        status="implemented",
        note="通过 tushare 顶层函数 realtime_list 调用。",
    ),
    TushareStockInterface("daily_basic", "每日指标", "行情数据", "daily_basic", status="implemented"),
    TushareStockInterface("stk_limit", "每日涨跌停价格", "行情数据", "stk_limit", status="implemented"),
    TushareStockInterface("suspend_d", "每日停复牌信息", "行情数据", "suspend_d", status="implemented"),
    TushareStockInterface("hsgt_top10", "沪深股通十大成交股", "行情数据", "hsgt_top10", status="implemented"),
    TushareStockInterface("ggt_top10", "港股通十大成交股", "行情数据", "ggt_top10", status="implemented"),
    TushareStockInterface("ggt_daily", "港股通每日成交统计", "行情数据", "ggt_daily", status="implemented"),
    TushareStockInterface("ggt_monthly", "港股通每月成交统计", "行情数据", "ggt_monthly", status="implemented"),
    TushareStockInterface(
        "backup_quotes",
        "备用行情",
        "行情数据",
        "bak_daily",
        status="implemented",
        note="文档接口名为 bak_daily。",
    ),
    # 财务数据
    TushareStockInterface("income", "利润表", "财务数据", "income", status="implemented"),
    TushareStockInterface("balancesheet", "资产负债表", "财务数据", "balancesheet", status="implemented"),
    TushareStockInterface("cashflow", "现金流量表", "财务数据", "cashflow", status="implemented"),
    TushareStockInterface("forecast", "业绩预告", "财务数据", "forecast", status="implemented"),
    TushareStockInterface("express", "业绩快报", "财务数据", "express", status="implemented"),
    TushareStockInterface("dividend", "分红送股数据", "财务数据", "dividend", status="implemented"),
    TushareStockInterface("fina_indicator", "财务指标数据", "财务数据", "fina_indicator", status="implemented"),
    TushareStockInterface("fina_audit", "财务审计意见", "财务数据", "fina_audit", status="implemented"),
    TushareStockInterface("fina_mainbz", "主营业务构成", "财务数据", "fina_mainbz", status="implemented"),
    TushareStockInterface("disclosure_date", "财报披露日期表", "财务数据", "disclosure_date", status="implemented"),
    # 参考数据
    TushareStockInterface(
        "abnormal_movement",
        "个股异常波动",
        "参考数据",
        "stk_shock",
        status="implemented",
        note="文档接口名为 stk_shock。",
    ),
    TushareStockInterface(
        "serious_abnormal_movement",
        "个股严重异常波动",
        "参考数据",
        "stk_high_shock",
        status="implemented",
        note="文档接口名为 stk_high_shock。",
    ),
    TushareStockInterface(
        "exchange_key_security_notice",
        "交易所重点提示证券",
        "参考数据",
        "stk_alert",
        status="implemented",
        note="文档接口名为 stk_alert。",
    ),
    TushareStockInterface("top10_holders", "前十大股东", "参考数据", "top10_holders", status="implemented"),
    TushareStockInterface("top10_floatholders", "前十大流通股东", "参考数据", "top10_floatholders", status="implemented"),
    TushareStockInterface("pledge_stat", "股权质押统计数据", "参考数据", "pledge_stat", status="implemented"),
    TushareStockInterface("pledge_detail", "股权质押明细数据", "参考数据", "pledge_detail", status="implemented"),
    TushareStockInterface("repurchase", "股票回购", "参考数据", "repurchase", status="implemented"),
    TushareStockInterface("share_float", "限售股解禁", "参考数据", "share_float", status="implemented"),
    TushareStockInterface("block_trade", "大宗交易", "参考数据", "block_trade", status="implemented"),
    TushareStockInterface("stk_account", "股票开户数据（停）", "参考数据", "stk_account", status="implemented"),
    TushareStockInterface("stk_account_old", "股票开户数据（旧）", "参考数据", "stk_account_old", status="implemented"),
    TushareStockInterface("stk_holdernumber", "股东人数", "参考数据", "stk_holdernumber", status="implemented"),
    TushareStockInterface("stk_holdertrade", "股东增减持", "参考数据", "stk_holdertrade", status="implemented"),
    # 特色数据
    TushareStockInterface(
        "broker_earning_forecast",
        "券商盈利预测数据",
        "特色数据",
        "report_rc",
        status="implemented",
        note="文档接口名为 report_rc（卖方盈利预测数据）。",
    ),
    TushareStockInterface("cyq_perf", "每日筹码及胜率", "特色数据", "cyq_perf", status="implemented"),
    TushareStockInterface("cyq_chips", "每日筹码分布", "特色数据", "cyq_chips", status="implemented"),
    TushareStockInterface("stk_factor_pro", "股票技术面因子(专业版）", "特色数据", "stk_factor_pro", status="implemented"),
    TushareStockInterface("ccass_hold", "中央结算系统持股统计", "特色数据", "ccass_hold", status="implemented"),
    TushareStockInterface("ccass_hold_detail", "中央结算系统持股明细", "特色数据", "ccass_hold_detail", status="implemented"),
    TushareStockInterface("hk_hold", "沪深股通持股明细", "特色数据", "hk_hold", status="implemented"),
    TushareStockInterface("stk_auction_o", "股票开盘集合竞价数据", "特色数据", "stk_auction_o", status="implemented"),
    TushareStockInterface("stk_auction_c", "股票收盘集合竞价数据", "特色数据", "stk_auction_c", status="implemented"),
    TushareStockInterface("nine_turn", "神奇九转指标", "特色数据", "nine_turn", status="implemented"),
    TushareStockInterface("ah_daily", "AH股比价", "特色数据", "ah_daily", status="implemented"),
    TushareStockInterface("stk_surv", "机构调研数据", "特色数据", "stk_surv", status="implemented"),
    TushareStockInterface(
        "broker_montly_pick",
        "券商月度金股",
        "特色数据",
        "broker_recommend",
        status="implemented",
        note="文档接口名为 broker_recommend。",
    ),
    # 两融及转融通
    TushareStockInterface("margin", "融资融券交易汇总", "两融及转融通", "margin", status="implemented"),
    TushareStockInterface("margin_detail", "融资融券交易明细", "两融及转融通", "margin_detail", status="implemented"),
    TushareStockInterface("margin_target", "融资融券标的（盘前）", "两融及转融通", "margin_target", status="implemented"),
    TushareStockInterface(
        "seclending_summary",
        "转融券交易汇总(停）",
        "两融及转融通",
        "slb_sec",
        status="implemented",
        note="文档接口名为 slb_sec。",
    ),
    TushareStockInterface(
        "seclending_finance_summary",
        "转融资交易汇总",
        "两融及转融通",
        "slb_len",
        status="implemented",
        note="文档接口名为 slb_len。",
    ),
    TushareStockInterface(
        "seclending_detail",
        "转融券交易明细(停）",
        "两融及转融通",
        "slb_sec_detail",
        status="implemented",
        note="文档接口名为 slb_sec_detail。",
    ),
    TushareStockInterface(
        "market_making_lending_summary",
        "做市借券交易汇总(停）",
        "两融及转融通",
        "slb_len_mm",
        status="implemented",
        note="文档接口名为 slb_len_mm。",
    ),
    # 资金流向数据
    TushareStockInterface("moneyflow", "个股资金流向", "资金流向数据", "moneyflow", status="implemented"),
    TushareStockInterface("moneyflow_ths", "个股资金流向（THS）", "资金流向数据", "moneyflow_ths", status="implemented"),
    TushareStockInterface("moneyflow_dc", "个股资金流向（DC）", "资金流向数据", "moneyflow_dc", status="implemented"),
    TushareStockInterface(
        "moneyflow_sector_ths",
        "板块资金流向（THS)",
        "资金流向数据",
        "moneyflow_cnt_ths",
        status="implemented",
        note="文档接口名为 moneyflow_cnt_ths（同花顺概念板块资金流向）。",
    ),
    TushareStockInterface("moneyflow_ind_ths", "行业资金流向（THS）", "资金流向数据", "moneyflow_ind_ths", status="implemented"),
    TushareStockInterface(
        "moneyflow_sector_dc",
        "板块资金流向（DC）",
        "资金流向数据",
        "moneyflow_ind_dc",
        status="implemented",
        note="通过 moneyflow_ind_dc 调用，使用 content_type 区分行业/概念/地域板块。",
    ),
    TushareStockInterface("moneyflow_mkt_dc", "大盘资金流向（DC）", "资金流向数据", "moneyflow_mkt_dc", status="implemented"),
    TushareStockInterface("moneyflow_hsgt", "沪深港通资金流向", "资金流向数据", "moneyflow_hsgt", status="implemented"),
    # 打板专题数据
    TushareStockInterface("top_list", "龙虎榜每日统计单", "打板专题数据", "top_list", status="implemented"),
    TushareStockInterface("top_inst", "龙虎榜机构交易单", "打板专题数据", "top_inst", status="implemented"),
    TushareStockInterface("limit_list_ths", "同花顺涨跌停榜单", "打板专题数据", "limit_list_ths", status="implemented"),
    TushareStockInterface("limit_list_d", "涨跌停和炸板数据", "打板专题数据", "limit_list_d", status="implemented"),
    TushareStockInterface("limit_step", "涨停股票连板天梯", "打板专题数据", "limit_step", status="implemented"),
    TushareStockInterface(
        "limit_strong",
        "涨停最强板块统计",
        "打板专题数据",
        "limit_cpt_list",
        status="implemented",
        note="文档接口名为 limit_cpt_list。",
    ),
    TushareStockInterface("ths_index", "同花顺行业概念板块", "打板专题数据", "ths_index", status="implemented"),
    TushareStockInterface("ths_daily", "同花顺概念和行业指数行情", "打板专题数据", "ths_daily", status="implemented"),
    TushareStockInterface("ths_member", "同花顺行业概念成分", "打板专题数据", "ths_member", status="implemented"),
    TushareStockInterface("dc_index", "东方财富概念板块", "打板专题数据", "dc_index", status="implemented"),
    TushareStockInterface("dc_member", "东方财富概念成分", "打板专题数据", "dc_member", status="implemented"),
    TushareStockInterface("dc_daily", "东财概念和行业指数行情", "打板专题数据", "dc_daily", status="implemented"),
    TushareStockInterface("stk_auction", "开盘竞价成交（当日）", "打板专题数据", "stk_auction", status="implemented"),
    TushareStockInterface(
        "hot_money_roster",
        "市场游资最全名录",
        "打板专题数据",
        "hm_list",
        status="implemented",
        note="文档接口名为 hm_list。",
    ),
    TushareStockInterface(
        "hot_money_trade_detail",
        "游资交易每日明细",
        "打板专题数据",
        "hm_detail",
        status="implemented",
        note="文档接口名为 hm_detail。",
    ),
    TushareStockInterface("ths_hot", "同花顺App热榜数", "打板专题数据", "ths_hot", status="implemented"),
    TushareStockInterface("dc_hot", "东方财富App热榜", "打板专题数据", "dc_hot", status="implemented"),
    TushareStockInterface("tdx_index", "通达信板块信息", "打板专题数据", "tdx_index", status="implemented"),
    TushareStockInterface("tdx_member", "通达信板块成分", "打板专题数据", "tdx_member", status="implemented"),
    TushareStockInterface("tdx_daily", "通达信板块行情", "打板专题数据", "tdx_daily", status="implemented"),
    TushareStockInterface("kpl_list", "榜单数据（开盘啦）", "打板专题数据", "kpl_list", status="implemented"),
    TushareStockInterface("kpl_concept", "题材成分（开盘啦）", "打板专题数据", "kpl_concept", status="implemented"),
    TushareStockInterface("dc_topic", "题材数据（东方财富）", "打板专题数据", "dc_topic", status="implemented"),
    TushareStockInterface("dc_topic_member", "题材成分（东方财富）", "打板专题数据", "dc_topic_member", status="implemented"),
]


ETF_DATA_INTERFACES: List[TushareStockInterface] = [
    TushareStockInterface(
        "etf_basic",
        "ETF基本信息",
        "基础数据",
        "etf_basic",
        status="implemented",
        note="兼容 etf_basic 与 fund_basic(market='E') 的合并结果。",
        topic="etf",
        fetch_strategy="etf_basic_merge",
        persistence_mode="fixed",
        target_table="etf_basic",
        create_method="create_etf_basic_table",
        insert_method="insert_etf_basic",
        unique_keys="ts_code",
    ),
    TushareStockInterface(
        "etf_daily",
        "ETF日线行情",
        "行情数据",
        "fund_daily",
        status="implemented",
        note="通过 fund_daily 接口按交易日抓取全市场 ETF 日线。",
        topic="etf",
        fetch_strategy="trade_dates",
        persistence_mode="fixed",
        target_table="etf_daily",
        create_method="create_etf_daily_table",
        insert_method="insert_etf_daily",
        unique_keys="ts_code,trade_date",
    ),
]


INDEX_DATA_INTERFACES: List[TushareStockInterface] = [
    TushareStockInterface(
        "index_basic",
        "指数基本信息",
        "基础数据",
        "index_basic",
        status="implemented",
        note="通过多市场循环获取全部指数基本信息。",
        topic="index",
        fetch_strategy="index_basic_all_markets",
        persistence_mode="fixed",
        target_table="index_basic",
        create_method="create_index_basic_table",
        insert_method="insert_index_basic",
        unique_keys="ts_code",
    ),
    TushareStockInterface(
        "index_daily",
        "指数日线行情",
        "行情数据",
        "index_daily",
        status="implemented",
        note="默认按指数代码列表批量抓取指数日线行情。",
        topic="index",
        fetch_strategy="index_daily_by_codes",
        persistence_mode="fixed",
        target_table="index_daily",
        create_method="create_index_daily_table",
        insert_method="insert_index_daily",
        unique_keys="ts_code,trade_date",
    ),
    TushareStockInterface(
        "index_weekly",
        "指数周线行情",
        "行情数据",
        "index_weekly",
        status="implemented",
        note="按每周最后一个交易日抓取全市场指数周线。",
        topic="index",
        fetch_strategy="week_end_dates",
        persistence_mode="fixed",
        target_table="index_weekly",
        create_method="create_index_weekly_table",
        insert_method="insert_index_weekly",
        unique_keys="ts_code,trade_date",
    ),
    TushareStockInterface(
        "index_weight",
        "指数成分和权重",
        "参考数据",
        "index_weight",
        status="implemented",
        note="支持指定 index_code，或按时间区间抓取可用指数权重。",
        topic="index",
        fetch_strategy="index_weight_by_codes",
        persistence_mode="fixed",
        target_table="index_weight",
        create_method="create_index_weight_table",
        insert_method="insert_index_weight",
        unique_keys="index_code,con_code,trade_date",
    ),
    TushareStockInterface(
        "index_dailybasic",
        "大盘指数每日指标",
        "特色数据",
        "index_dailybasic",
        status="implemented",
        note="默认抓取主要指数列表的大盘每日指标。",
        topic="index",
        fetch_strategy="index_dailybasic_major",
        persistence_mode="fixed",
        target_table="index_dailybasic",
        create_method="create_index_dailybasic_table",
        insert_method="insert_index_dailybasic",
        unique_keys="ts_code,trade_date",
    ),
]


STOCK_DATA_INTERFACE_MAP: Dict[str, TushareStockInterface] = {
    interface.key: interface for interface in STOCK_DATA_INTERFACES
}

ETF_DATA_INTERFACE_MAP: Dict[str, TushareStockInterface] = {
    interface.key: interface for interface in ETF_DATA_INTERFACES
}

INDEX_DATA_INTERFACE_MAP: Dict[str, TushareStockInterface] = {
    interface.key: interface for interface in INDEX_DATA_INTERFACES
}

ALL_TUSHARE_INTERFACES: List[TushareStockInterface] = (
    STOCK_DATA_INTERFACES + ETF_DATA_INTERFACES + INDEX_DATA_INTERFACES
)

ALL_TUSHARE_INTERFACE_MAP: Dict[str, TushareStockInterface] = {
    interface.key: interface for interface in ALL_TUSHARE_INTERFACES
}


def list_tushare_interfaces(
    topic: Optional[str] = None,
    category: Optional[str] = None,
    only_fetchable: bool = False,
) -> List[TushareStockInterface]:
    interfaces = ALL_TUSHARE_INTERFACES
    if topic:
        interfaces = [item for item in interfaces if item.topic == topic]
    if category:
        interfaces = [item for item in interfaces if item.category == category]
    if only_fetchable:
        interfaces = [item for item in interfaces if item.status == "implemented"]
    return interfaces


def get_tushare_interface(identifier: str, topic: Optional[str] = None) -> Optional[TushareStockInterface]:
    interfaces = list_tushare_interfaces(topic=topic)
    interface_map = {
        item.key: item for item in interfaces
    }
    if identifier in interface_map:
        return interface_map[identifier]

    for item in interfaces:
        if item.api_name == identifier or item.title == identifier:
            return item
    return None


def get_tushare_interface_categories(topic: Optional[str] = None) -> List[str]:
    return sorted({item.category for item in list_tushare_interfaces(topic=topic)})


def list_stock_data_interfaces(
    category: Optional[str] = None,
    only_fetchable: bool = False,
) -> List[TushareStockInterface]:
    return list_tushare_interfaces(
        topic="stock",
        category=category,
        only_fetchable=only_fetchable,
    )


def get_stock_data_interface(identifier: str) -> Optional[TushareStockInterface]:
    return get_tushare_interface(identifier, topic="stock")


def get_stock_data_interface_categories() -> List[str]:
    return get_tushare_interface_categories(topic="stock")
