from __future__ import annotations

from pathlib import Path
import importlib

from . import io as core_io


def needs_xls_support(config: dict, raw_data_dir: Path) -> bool:
    def is_xls_file(name: object) -> bool:
        return isinstance(name, str) and name.strip().lower().endswith(".xls")

    if is_xls_file(config.get("inventory_file")) or is_xls_file(config.get("carton_factor_file")):
        return True
    if any(is_xls_file(name) for name in config.get("sales_files", [])):
        return True

    run_mode = str(config.get("run_mode", "single")).lower()
    if run_mode == "batch":
        for system in config.get("batch", {}).get("systems", []):
            if not bool(system.get("enabled", True)):
                continue
            if is_xls_file(system.get("inventory_file")) or is_xls_file(system.get("carton_factor_file")):
                return True
            if any(is_xls_file(name) for name in system.get("sales_files", [])):
                return True
    elif not config.get("sales_files"):
        for path in raw_data_dir.iterdir():
            if path.is_file() and path.suffix.lower() == ".xls":
                return True
    return False


def resolve_config_path(path_value: object, base_dir: Path) -> Path | None:
    """将配置中的路径解析为绝对路径，空值返回 None。"""
    if not isinstance(path_value, str) or not path_value.strip():
        return None
    path = Path(path_value.strip())
    if not path.is_absolute():
        path = (base_dir / path).resolve()
    return path


def check_sales_amount_columns(sales_files: list[Path]) -> list[str]:
    """校验销售文件是否包含门店销量排名调货汇总所需的销售额列。"""
    errors: list[str] = []
    for sales_file in sales_files:
        if not sales_file.exists():
            continue
        try:
            sales_df = core_io.read_excel_first_sheet(sales_file)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"failed to read sales file for amount-column check: {sales_file.name}: {exc}")
            continue
        sales_df.columns = sales_df.columns.astype(str).str.strip()
        sales_amount_col = core_io.find_sales_amount_column(sales_df.columns.tolist())
        if sales_amount_col is None:
            errors.append(
                f"sales file missing sales amount column: {sales_file.name} "
                "(expected one of: 销售金额, 含税销售金额/元, 含税销售额/元, 销售额, sales_amount, amount)"
            )
    return errors


def check_xlrd_dependency(config: dict, raw_data_dir: Path) -> list[str]:
    if not needs_xls_support(config, raw_data_dir):
        return []
    try:
        importlib.import_module("xlrd")
    except Exception as exc:  # noqa: BLE001
        return [f"missing dependency 'xlrd': {exc} (required for .xls inputs)"]
    return []
