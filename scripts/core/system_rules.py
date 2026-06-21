from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd

from . import io as core_io
from .models import AppConfig, BarcodeMappingResult

SUPPLIER_CARD_PROVINCE_MAP = {
    "153085": "宁夏",
    "680249": "甘肃",
    "153412": "宁夏",
    "152901": "监狱系统",
}
WUMEI_BARCODE_MAPPING_RELATIVE_PATH = Path("data/物美商品条码映射.csv")


@dataclass(frozen=True)
class SystemRuleProfile:
    """描述单个系统在流水线中的特例规则开关。"""

    display_name: str
    is_wumei_system: bool
    enable_province_column: bool


def resolve_system_rule_profile(display_name: str, config: AppConfig) -> SystemRuleProfile:
    """根据系统展示名和配置解析当前运行应使用的规则画像。"""
    normalized_display_name = str(display_name).strip()
    normalized_system_id = str(config.get("system_id", "")).strip().lower()
    is_wumei_system = normalized_system_id.endswith("_wumei") or "wumei" in normalized_system_id or "物美" in normalized_display_name
    province_column_enabled_cfg = config.get("province_column_enabled", None)
    enable_province_column = (
        bool(province_column_enabled_cfg)
        if isinstance(province_column_enabled_cfg, bool)
        else is_wumei_system
    )
    return SystemRuleProfile(
        display_name=normalized_display_name,
        is_wumei_system=is_wumei_system,
        enable_province_column=enable_province_column,
    )


def map_province_by_supplier_card(card: Optional[str]) -> str:
    """根据供商卡号映射省份，未知值统一回退。"""
    normalized = core_io.normalize_supplier_card_value(card)
    if normalized is None:
        return "其他/未知"
    return SUPPLIER_CARD_PROVINCE_MAP.get(str(normalized), "其他/未知")


def apply_inventory_barcode_mapping(
    *,
    inv_df: pd.DataFrame,
    sales_df: pd.DataFrame,
    profile: SystemRuleProfile,
    mapping_df: Optional[pd.DataFrame] = None,
) -> BarcodeMappingResult:
    """仅对物美库存缺失条码应用历史映射，当前销售数据始终优先。"""
    if not profile.is_wumei_system or mapping_df is None:
        return BarcodeMappingResult(inventory_df=inv_df)

    out = inv_df.copy()
    product_codes = out["product_code"].apply(core_io.normalize_barcode_value)
    actual_barcodes = out["actual_barcode"].apply(core_io.normalize_barcode_value)
    reference_map = dict(zip(mapping_df["product_code"], mapping_df["mapped_barcode"]))

    sales_pairs = sales_df[["product_code", "display_barcode"]].copy()
    sales_pairs["product_code"] = sales_pairs["product_code"].apply(core_io.normalize_barcode_value)
    sales_pairs["display_barcode"] = sales_pairs["display_barcode"].apply(core_io.normalize_barcode_value)
    sales_pairs = sales_pairs.dropna().drop_duplicates()
    sales_codes = set(sales_pairs["product_code"])

    conflict_codes: list[str] = []
    for product_code, group in sales_pairs.groupby("product_code"):
        mapped_barcode = reference_map.get(product_code)
        if mapped_barcode is not None and set(group["display_barcode"]) != {mapped_barcode}:
            conflict_codes.append(str(product_code))

    candidates = product_codes.map(reference_map)
    use_mapping = actual_barcodes.isna() & candidates.notna() & ~product_codes.isin(sales_codes)
    out.loc[use_mapping, "actual_barcode"] = candidates[use_mapping]
    fallback = int((actual_barcodes.isna() & ~product_codes.isin(sales_codes) & candidates.isna()).sum())
    return BarcodeMappingResult(
        inventory_df=out,
        hits=int(use_mapping.sum()),
        fallback=fallback,
        conflicts=len(conflict_codes),
        conflict_samples=", ".join(conflict_codes[:10]),
    )
