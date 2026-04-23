from __future__ import annotations

import re
from pathlib import Path
from typing import List, Optional, Tuple


def extract_month_key(filename: str) -> Optional[int]:
    match = re.search(r"(\d{4})(\d{2})", filename)
    if not match:
        return None
    year = int(match.group(1))
    month = int(match.group(2))
    if month < 1 or month > 12:
        return None
    return year * 100 + month


def find_column(columns: List[str], candidates: List[str]) -> Optional[str]:
    for c in candidates:
        if c in columns:
            return c
    return None


def resolve_sales_candidates(raw_data_dir: Path, configured_sales_files: List[str]) -> List[Path]:
    if not raw_data_dir.exists() or not raw_data_dir.is_dir():
        raise FileNotFoundError(f"Sales data directory not found: {raw_data_dir}")
    if configured_sales_files:
        base_dir = raw_data_dir.resolve()
        resolved_paths: List[Path] = []
        for name in configured_sales_files:
            candidate = (base_dir / name).resolve()
            try:
                candidate.relative_to(base_dir)
            except ValueError as exc:
                raise ValueError(
                    f"Configured sales file escapes raw_data_dir: {name} (raw_data_dir={raw_data_dir})"
                ) from exc
            resolved_paths.append(candidate)
        return resolved_paths

    candidates: List[Tuple[int, Path]] = []
    for path in raw_data_dir.iterdir():
        if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xls"}:
            continue
        name = path.name.lower()
        if "库存" in name or "inventory" in name:
            continue
        if "销售" not in name and "sale" not in name and "sales" not in name:
            continue
        month_key = extract_month_key(path.name)
        if month_key is None:
            continue
        candidates.append((month_key, path))
    candidates.sort(key=lambda x: x[0])
    return [path for _, path in candidates]


def list_ignored_sales_files(raw_data_dir: Path, configured_sales_files: List[str]) -> List[str]:
    if configured_sales_files or not raw_data_dir.exists() or not raw_data_dir.is_dir():
        return []
    ignored: List[str] = []
    for path in raw_data_dir.iterdir():
        if not path.is_file() or path.suffix.lower() not in {".xlsx", ".xls"}:
            continue
        name = path.name.lower()
        if "库存" in name or "inventory" in name:
            ignored.append(f"{path.name} (inventory-like)")
            continue
        if "销售" not in name and "sale" not in name and "sales" not in name:
            ignored.append(f"{path.name} (missing sales keyword)")
            continue
        if extract_month_key(path.name) is None:
            ignored.append(f"{path.name} (missing YYYYMM)")
    return ignored


def format_ignored_sales_files(ignored_sales_files: List[str], limit: int = 20, max_chars: int = 2000) -> str:
    if not ignored_sales_files:
        return ""
    shown = ignored_sales_files[:limit]
    text = " | ".join(shown)
    hidden = len(ignored_sales_files) - len(shown)
    if len(text) > max_chars:
        text = text[: max_chars - 3] + "..."
    if hidden > 0:
        text = f"{text} | ... (+{hidden} more)"
    return text


def find_sales_amount_column(columns: List[str]) -> Optional[str]:
    """识别销售额字段，兼容不同系统导出的常见列名。"""
    return find_column(
        columns,
        [
            "销售金额",
            "含税销售金额/元",
            "含税销售额/元",
            "销售额",
            "sales_amount",
            "amount",
        ],
    )
