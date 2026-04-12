from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator, Literal, Mapping, TypedDict, TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from .system_rules import SystemRuleProfile

RunMode = Literal["single", "batch"]
BatchRecordStatus = Literal["SUCCESS", "FAILED", "SKIPPED", "UNKNOWN"]


class BatchSystemConfig(TypedDict, total=False):
    """描述批量模式下单个系统的配置结构。"""

    enabled: bool
    system_id: str
    display_name: str
    data_subdir: str
    sales_files: list[str]
    inventory_file: str
    output_file: str
    carton_factor_file: str
    province_column_enabled: bool


class BatchConfig(TypedDict):
    """描述批量运行的配置结构。"""

    continue_on_error: bool
    summary_output_file: str
    systems: list[BatchSystemConfig]


class AppConfig(TypedDict, total=False):
    """描述主配置在运行期的统一字典结构。"""

    run_mode: RunMode
    display_name: str
    system_id: str
    raw_data_dir: str
    data_subdir: str
    output_file: str
    sales_files: list[str]
    inventory_file: str
    risk_days_high: float
    risk_days_low: float
    sales_window_full_months: int
    sales_window_include_mtd: bool
    sales_window_recent_days: int
    sales_date_dayfirst: bool
    sales_date_format: str
    season_mode: bool | str
    fail_on_empty_window: bool
    strict_auto_scan: bool
    merge_detail_store_cells: bool
    enable_ranked_store_transfer_summary: bool
    stagnant_outbound_mode: str
    stagnant_min_keep_qty: float
    carton_factor_file: str
    brand_keywords: list[str]
    batch: BatchConfig
    province_column_enabled: bool | None
    enabled: bool


class ReportRunResult(TypedDict):
    """描述单系统执行完成后返回的结果结构。"""

    system_id: str
    display_name: str
    status: BatchRecordStatus
    message: str
    error_stage: str
    output_file: str
    input_files_count: int
    loaded_sales_files: int
    missing_sales_files: int
    inventory_file_exists: bool
    detail_rows: int
    missing_sku_rows: int


@dataclass
class BatchSummaryRecord:
    """描述批量模式汇总表中的单条运行记录。"""

    system_id: str
    display_name: str
    enabled: bool = False
    data_subdir: str = ""
    status: BatchRecordStatus = "UNKNOWN"
    message: str = ""
    error_stage: str = ""
    output_file: str = ""
    duration_sec: float = 0.0
    generated_at: str = ""
    input_files_count: int = 0
    loaded_sales_files: int = 0
    missing_sales_files: int = 0
    inventory_file_exists: bool = False
    detail_rows: int = 0
    missing_sku_rows: int = 0

    @classmethod
    def make_status(
        cls,
        *,
        system_id: str,
        display_name: str,
        status: BatchRecordStatus,
        message: str,
        output_file: str,
        error_stage: str = "",
    ) -> "BatchSummaryRecord":
        """创建只包含状态字段的基础记录。"""
        return cls(
            system_id=system_id,
            display_name=display_name,
            status=status,
            message=message,
            error_stage=error_stage,
            output_file=output_file,
        )

    @classmethod
    def from_report_result(cls, result: ReportRunResult | Mapping[str, Any]) -> "BatchSummaryRecord":
        """将单系统运行结果转换成批量汇总记录，并兼容旧测试的部分字段返回。"""
        return cls(
            system_id=str(result.get("system_id", "")),
            display_name=str(result.get("display_name", "")),
            status=result.get("status", "UNKNOWN"),
            message=str(result.get("message", "")),
            error_stage=str(result.get("error_stage", "")),
            output_file=str(result.get("output_file", "")),
            input_files_count=int(result.get("input_files_count", 0)),
            loaded_sales_files=int(result.get("loaded_sales_files", 0)),
            missing_sales_files=int(result.get("missing_sales_files", 0)),
            inventory_file_exists=bool(result.get("inventory_file_exists", False)),
            detail_rows=int(result.get("detail_rows", 0)),
            missing_sku_rows=int(result.get("missing_sku_rows", 0)),
        )

    def finalize(
        self,
        *,
        start_time: datetime,
        enabled: bool,
        data_subdir: str,
        duration_sec: float | None = None,
    ) -> "BatchSummaryRecord":
        """补全运行时字段，形成最终可落表记录。"""
        self.enabled = enabled
        self.data_subdir = data_subdir
        self.duration_sec = duration_sec if duration_sec is not None else round((datetime.now() - start_time).total_seconds(), 3)
        self.generated_at = datetime.now().isoformat(timespec="seconds")
        return self

    def to_row(self) -> dict[str, Any]:
        """转换成 DataFrame 可直接消费的字典。"""
        return asdict(self)


@dataclass(frozen=True)
class SalesLoadResult:
    """描述销售文件加载和标准化后的聚合结果。"""

    sales_df: pd.DataFrame
    loaded_sales_file_count: int
    missing_sales_files: list[str]
    invalid_sales_date_rows: int
    invalid_sales_qty_rows: int


@dataclass(frozen=True)
class WindowContext:
    """描述当前库存日期下两个销售窗口的有效区间。"""

    mtd_start: pd.Timestamp
    mtd_end: pd.Timestamp
    recent_start: pd.Timestamp
    mtd_days: int
    recent_days_effective: int
    has_mtd_window_data: bool
    has_recent_window_data: bool


@dataclass(frozen=True)
class InventoryPreparationResult:
    """描述库存文件标准化后的运行输入。"""

    inventory_df: pd.DataFrame
    output_file: Path
    inventory_date_ts: pd.Timestamp
    inventory_date: str
    invalid_inventory_qty_rows: int


@dataclass(frozen=True)
class BarcodeMappingResult:
    """描述系统级条码映射扩展点返回的结果。"""

    inventory_df: pd.DataFrame
    hits: int = 0
    fallback: int = 0
    conflicts: int = 0
    conflict_samples: str = ""


@dataclass(frozen=True)
class MatchingResult:
    """描述销售库存匹配后的核心产出集合。"""

    detail: pd.DataFrame
    missing_sales: pd.DataFrame
    store_summary: pd.DataFrame
    brand_summary: pd.DataFrame
    mapping_stats: dict[str, float]

    def __iter__(self) -> Iterator[pd.DataFrame | dict[str, float]]:
        """兼容旧调用方按五元组解包匹配结果。"""
        yield self.detail
        yield self.missing_sales
        yield self.store_summary
        yield self.brand_summary
        yield self.mapping_stats


@dataclass(frozen=True)
class StatusFrameInput:
    """描述运行状态页构建所需的全部输入。"""

    program_version: str
    config: AppConfig
    display_name: str
    system_id: str
    inventory_date: str
    input_files_count: int
    loaded_sales_file_count: int
    missing_sales_files: list[str]
    use_peak_mode: bool
    strict_auto_scan: bool
    has_mtd_window_data: bool
    has_recent_window_data: bool
    mtd_days: int
    recent_days_effective: int
    invalid_sales_date_rows: int
    invalid_sales_qty_rows: int
    invalid_inventory_qty_rows: int
    replenish_out: pd.DataFrame
    transfer_out: pd.DataFrame
    mapping_stats: dict[str, float]
    ignored_sales_files: list[str]
    is_wumei_system: bool
    wumei_barcode_map_hits: int
    wumei_barcode_map_fallback: int
    wumei_barcode_map_conflicts: int
    wumei_barcode_conflict_samples: str


@dataclass(frozen=True)
class ReportFrames:
    """描述报表输出阶段生成的各个工作表数据。"""

    usage_guide: pd.DataFrame
    detail: pd.DataFrame
    store_summary: pd.DataFrame
    brand_summary: pd.DataFrame
    out_of_stock: pd.DataFrame
    missing_sku: pd.DataFrame
    replenish: pd.DataFrame
    transfer: pd.DataFrame
    product_code_catalog: pd.DataFrame

    _SHEET_NAME_MAP = {
        "使用说明": "usage_guide",
        "明细": "detail",
        "门店汇总": "store_summary",
        "品牌汇总": "brand_summary",
        "缺货清单": "out_of_stock",
        "库存缺失SKU清单": "missing_sku",
        "建议补货清单": "replenish",
        "建议调货清单": "transfer",
        "商品编码对照清单": "product_code_catalog",
    }

    def __getitem__(self, sheet_name: str) -> pd.DataFrame:
        """兼容旧调用方按工作表名称取值。"""
        return getattr(self, self._SHEET_NAME_MAP[sheet_name])

    def items(self) -> Iterator[tuple[str, pd.DataFrame]]:
        """兼容旧调用方遍历工作表字典。"""
        for sheet_name, attr_name in self._SHEET_NAME_MAP.items():
            yield sheet_name, getattr(self, attr_name)


@dataclass
class ReportRunContext:
    """描述单系统报表生成流程中的共享运行上下文。"""

    config: AppConfig
    base_dir: Path
    program_version: str
    system_id: str
    display_name: str
    rule_profile: SystemRuleProfile
    raw_data_dir: Path | None = None
    configured_sales_files: list[str] | None = None
    inventory_file: str = ""
    carton_factor_file: str = ""
    strict_auto_scan: bool = False
    brand_keywords: list[str] | None = None
    ignored_sales_files: list[str] | None = None
    inventory_file_exists: bool = False
    inventory_date: str = ""
    inventory_date_ts: pd.Timestamp | None = None
    output_file: Path | None = None
    full_months: int = 3
    include_mtd: bool = True
    recent_days: int = 30
    sales_date_dayfirst: bool = False
    sales_date_format: str = ""
    use_peak_mode: bool = False
    fail_on_empty_window: bool = False
    merge_detail_store_cells: bool = True
    enable_ranked_store_transfer_summary: bool = False
    high_days: float = 60.0
    low_days: float = 45.0
    stagnant_outbound_mode: str = "keep_safety_stock"
    stagnant_min_keep_qty: float = 0.0


@dataclass(frozen=True)
class InputStageResult:
    """描述输入准备阶段产出的全部运行数据。"""

    sales_df: pd.DataFrame
    inventory_df: pd.DataFrame
    carton_factor_df: pd.DataFrame
    loaded_sales_file_count: int
    missing_sales_files: list[str]
    invalid_sales_date_rows: int
    invalid_sales_qty_rows: int
    invalid_inventory_qty_rows: int
    wumei_barcode_map_hits: int
    wumei_barcode_map_fallback: int
    wumei_barcode_map_conflicts: int
    wumei_barcode_conflict_samples: str


@dataclass(frozen=True)
class AnalysisStageResult:
    """描述分析阶段产出的核心分析表与状态数据。"""

    detail: pd.DataFrame
    missing_sales: pd.DataFrame
    store_summary: pd.DataFrame
    brand_summary: pd.DataFrame
    mapping_stats: dict[str, float]
    frames: ReportFrames
    product_code_catalog: pd.DataFrame
    store_sales_ranking_transfer_out: pd.DataFrame
    summary_out: pd.DataFrame
    status_out: pd.DataFrame


@dataclass(frozen=True)
class OutputStageResult:
    """描述输出组装阶段生成的工作表和派生结果。"""

    frames: ReportFrames
    detail_out: pd.DataFrame
    missing_sku_out: pd.DataFrame
    out_of_stock_out: pd.DataFrame
    replenish_out: pd.DataFrame
    transfer_out: pd.DataFrame
    executive_overview_out: pd.DataFrame
    store_sales_ranking_transfer_out: pd.DataFrame
