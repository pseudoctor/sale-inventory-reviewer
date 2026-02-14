# Pseudoctor Sale & Inventory Reviewer

Generates an Excel risk report based on windowed daily sales and the current inventory snapshot,
with risk levels, summaries, and automatic styling.

## Operations Manual
- Final operations playbook: `docs/运维操作手册.md`

## Inputs
- Monthly sales Excel files (default: `raw_data/YYYYMM.xlsx`)
- Inventory Excel file with store dimension

Required columns in sales files:
- `销售时间`, `门店名称`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `销售数量`
- `品牌` is optional; when missing or blank, brand is auto-derived from `商品名称` using `config.yaml -> brand_keywords` (fallback `其他`).

Required columns in inventory file:
- `门店名称`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `数量`
- `品牌` is optional; when missing or blank, brand is auto-derived from `商品名称` in-memory for current run (source file is not modified).

## Output
- Single mode: one report (default: `reports/inventory_risk_report.xlsx`)
- Batch mode: one report per system + `reports/batch_run_summary.xlsx`

Report filename policy:
- Auto naming (when `output_file` is empty or legacy default): `<系统名><库存日期>库存预警.xlsx`
  - Example: `陕西华润20260208库存预警.xlsx`
- Explicit naming (when `output_file` is provided): use exactly the configured filename.
- Batch safety checks:
  - duplicated `display_name` is rejected
  - duplicated explicit `output_file` is rejected

Each inventory report contains:
  - `明细`: 门店 → 品牌 → 商品层级（含近三月+本月迄今平均日销、近30天平均日销售、风险等级、周转指标、补货/调出建议）
  - `门店汇总`: 门店维度汇总
  - `品牌汇总`: 品牌维度汇总
  - `缺货清单`: 明细中“有销量且库存=0”的 SKU（含建议补货数量、建议补货箱数）
  - `建议补货清单`: 建议补货数量 > 0 的 SKU
  - `建议调货清单`: 建议调出数量 > 0 的 SKU
  - `汇总`: 风险等级统计、缺货 SKU 数、库存总量、近三月+本月迄今日销总量、近30天日销总量、预测日销总量
  - `运行状态`: 程序版本、配置快照、输入文件统计、窗口有效性、无效日期行数、装箱因子缺失行数、自动扫描忽略清单
  - `库存缺失SKU清单`: 销售侧存在但库存侧未匹配到的 SKU（不是“库存=0”，而是“库存数据里缺记录”）

`建议补货清单` 与 `建议调货清单` 会额外包含：
- `装箱数（因子）`
- `建议补货箱数`（仅建议补货清单）
  - 旺季（`season_mode=true`）：`ceil(建议数量 / 装箱数)`
  - 淡季（`season_mode=false`）：`floor(建议数量 / 装箱数)`

Each sheet adds a title in row 1: `<系统名称> | 库存日期：YYYY-MM-DD` (extracted from the inventory file),
and applies header styling, filters, borders, risk color highlights, and out-of-stock row highlights.

## How to Run
```bash
# macOS/Linux
./run.sh

# Windows
run.bat
```

Install pinned dependencies manually if needed:
```bash
python3 -m pip install -r requirements.lock
```

`run_mode` is controlled in `config.yaml`:
- `single`: one system report
- `batch`: multiple system reports in one run

## Example workflow (single mode)
1) Drop recent monthly sales files into `raw_data/` as `YYYYMM.xlsx`.
2) Put the inventory snapshot as `raw_data/库存.xlsx` (with or without `品牌`).
3) Update `config.yaml` if needed (files, thresholds, or brand keywords).
4) Run `./run.sh`.
5) Open `reports/inventory_risk_report.xlsx` to review the results.

## Example workflow (batch mode)
1) Put each system's files into its own subfolder under `raw_data/` (e.g. `raw_data/甘肃物美/`, `raw_data/陕西华润/`).
2) Set `run_mode: "batch"`.
3) Configure each system under `batch.systems` with `enabled`, `data_subdir`, `sales_files`, `inventory_file`, and `output_file`.
4) Run `./run.sh`.
5) Check each output report and `reports/batch_run_summary.xlsx` (includes `SUCCESS` / `FAILED` / `SKIPPED`, plus `error_stage` / `input_files_count` / `loaded_sales_files` / `missing_sales_files` / `inventory_file_exists` diagnostics).

## Health Check
- Run preflight manually:
```bash
python3 scripts/health_check.py
```
- `run.sh` / `run.bat` also execute health check automatically before report generation.

### Recover Broken Virtual Environment (`yaml.safe_load` missing)
If health check shows:
- `invalid config.yaml: module 'yaml' has no attribute 'safe_load'`, or
- `broken dependency 'yaml': missing callable safe_load`

use the minimal recovery flow below (manual only; scripts do not auto-heal environment):

```bash
# 1) remove broken venv
rm -rf venv

# 2) create clean venv
python3 -m venv venv

# 3) ensure pip works in this venv
./venv/bin/python -m pip install --upgrade pip

# 4) reinstall pinned dependencies
./venv/bin/python -m pip install -r requirements.lock

# 5) verify PyYAML is complete
./venv/bin/python -c "import yaml; print(yaml.__file__, hasattr(yaml, 'safe_load'))"

# 6) re-run health check
./venv/bin/python scripts/health_check.py
```

Windows (CMD):
```bat
:: 1) remove broken venv
rmdir /s /q venv

:: 2) create clean venv
py -3 -m venv venv

:: 3) ensure pip works in this venv
venv\Scripts\python -m pip install --upgrade pip

:: 4) reinstall pinned dependencies
venv\Scripts\python -m pip install -r requirements.lock

:: 5) verify PyYAML is complete
venv\Scripts\python -c "import yaml; print(yaml.__file__, hasattr(yaml, 'safe_load'))"

:: 6) re-run health check
venv\Scripts\python scripts\health_check.py
```

Windows (PowerShell):
```powershell
# 1) remove broken venv
Remove-Item -Recurse -Force venv

# 2) create clean venv
py -3 -m venv venv

# 3) ensure pip works in this venv
venv\Scripts\python -m pip install --upgrade pip

# 4) reinstall pinned dependencies
venv\Scripts\python -m pip install -r requirements.lock

# 5) verify PyYAML is complete
venv\Scripts\python -c "import yaml; print(yaml.__file__, hasattr(yaml, 'safe_load'))"

# 6) re-run health check
venv\Scripts\python scripts\health_check.py
```

## Testing
- Full regression:
```bash
python3 -m pytest -q
```
- Golden snapshot E2E (stable output contract for key sheets):
```bash
python3 -m pytest -q tests/test_e2e_golden_snapshot.py
```
- CI is provided via GitHub Actions: `.github/workflows/ci.yml` (runs lockfile install + ruff + py_compile + pytest).

## Risk Logic
- `近三月+本月迄今平均日销` = 窗口销量总和 / 窗口有效天数
  - 窗口默认：近三个月完整自然月 + 本月迄今
- `近30天平均日销售` = 近30天销量总和 / 窗口有效天数
- `预测平均日销(季节模式后)`:
  - `season_mode=false`: `min(近三月+本月迄今平均日销, 近30天平均日销售)`
  - `season_mode=true`: `max(近三月+本月迄今平均日销, 近30天平均日销售)`
- `库存周转率` = (`预测平均日销(季节模式后)` * 30) / 库存
- `库存周转天数` = 库存 / `预测平均日销(季节模式后)`
- 当任一销售窗口与可用销售日期完全无重叠时，对应窗口日销按 `0` 处理，并在 `运行状态` 给出告警。
- Risk levels by turnover days:
  - 高: > 60 days
  - 中: 45–60 days
  - 低: < 45 days

## Configuration
Edit `config.yaml` to update the sales file list, inventory file, and output path.
You can also set `brand_keywords` to control how brand names are extracted from inventory product names.

Key fields:
- `run_mode`: `single` or `batch` (default `single`).
- `display_name`: system display name used in sheet title and default output naming.
- `sales_files`: optional explicit list of sales files; if empty, all `raw_data/*.xlsx` with `YYYYMM` in name are auto-detected.
- `inventory_file`: inventory snapshot file name under `raw_data/`.
- `risk_days_high` / `risk_days_low`: thresholds for risk classification.
- `sales_window_full_months`: number of full months before current month (default `3`).
- `sales_window_include_mtd`: include month-to-date sales (default `true`).
- `sales_window_recent_days`: rolling window length for recent daily sales (default `30`).
- `sales_date_dayfirst`: parse ambiguous dates with day-first preference (`false` by default).
- `sales_date_format`: optional explicit date format for `销售时间` (e.g. `%Y-%m-%d`); empty means auto-parse.
- `season_mode`: boolean (`false` / `true`).
  - `false`: use `min(近三月+本月迄今平均日销, 近30天平均日销售)` (off-peak)
  - `true`: use `max(近三月+本月迄今平均日销, 近30天平均日销售)` (peak)
- `fail_on_empty_window`: if `true`, raise error when 3M+MTD or 30-day window has no overlapping sales data.
- `strict_auto_scan`: only for auto-scan mode (`sales_files` empty).
  - `false` (default): normal auto-scan behavior.
  - `true`: when no valid sales files are detected and suspicious candidates were ignored, fail fast with ignored file list.
- `carton_factor_file`: carton factor mapping file path (default `./data/sku装箱数.xlsx`).
  - Required columns: `商品条码`, `商品名称`, `装箱数（因子）`
- `brand_keywords`: list of brand names used to derive `品牌` from `商品名称` when missing.
  - Also used to backfill blank `品牌` cells.
  - If multiple keywords match one product name, the brand whose match appears earliest in the product name is used.
  - Must not be empty. Empty list will fail health check and report generation to avoid silent brand distortion.
- `batch.continue_on_error`: in batch mode, continue remaining systems when one fails (`true`/`false`).
- `batch.summary_output_file`: batch run summary output path.
- `batch.systems`: explicit per-system config list:
  - `enabled`: boolean switch (`false` -> `SKIPPED`, no report generated)
  - `display_name` (Chinese simplified recommended), optional `system_id` machine key
  - `data_subdir`: system-specific data folder under `raw_data_dir`
  - `sales_files`, `inventory_file`, optional `output_file`
  - optional `carton_factor_file` to override global default

## Operations
- Add a new system: create a new subfolder in `raw_data/`, add one entry in `batch.systems`, set `enabled: true`.
- Disable a system temporarily: set `enabled: false` (keeps config/history, status becomes `SKIPPED`).
- Remove a system permanently: delete the entry from `batch.systems`.
