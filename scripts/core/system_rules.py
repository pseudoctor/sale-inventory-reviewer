from __future__ import annotations

from dataclasses import dataclass
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


@dataclass(frozen=True)
class SystemRuleProfile:
    """描述单个系统在流水线中的特例规则开关。"""

    display_name: str
    is_wumei_system: bool
    enable_province_column: bool


def resolve_system_rule_profile(display_name: str, config: AppConfig) -> SystemRuleProfile:
    """根据系统展示名和配置解析当前运行应使用的规则画像。"""
    normalized_display_name = str(display_name).strip()
    is_wumei_system = "物美" in normalized_display_name
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
) -> BarcodeMappingResult:
    """预留系统级条码映射扩展点，当前默认保持透传。"""
    _ = sales_df
    _ = profile
    return BarcodeMappingResult(inventory_df=inv_df)
