# Pseudoctor Sale & Inventory Reviewer

Generates an Excel risk report based on windowed daily sales and the current inventory snapshot,
with risk levels, summaries, and automatic styling.

## Inputs
- Monthly sales Excel files (default: `raw_data/YYYYMM.xlsx`)
- Inventory Excel file with store dimension

Required columns in sales files:
- `销售时间`, `门店名称`, `品牌`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `销售数量`

Required columns in inventory file:
- `门店名称`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `数量`
- If `品牌` is missing, it will be derived from `商品名称` in-memory for current run (source file is not modified).

## Output
- `reports/inventory_risk_report.xlsx`
  - `明细`: 门店 → 品牌 → 商品层级（含近三月+本月迄今平均日销、近30天平均日销售、风险等级、周转指标、补货/调出建议）
  - `门店汇总`: 门店维度汇总
  - `品牌汇总`: 品牌维度汇总
  - `缺货清单`: 有销量但库存缺失的 SKU（含建议补货数量、建议补货箱数）
  - `建议补货清单`: 建议补货数量 > 0 的 SKU
  - `建议调货清单`: 建议调出数量 > 0 的 SKU
  - `汇总`: 风险等级统计、缺货 SKU 数、库存总量、近三月+本月迄今日销总量、近30天日销总量、预测日销总量
  - `运行状态`: 季节模式、窗口有效性、无效日期行数、装箱因子缺失行数

`建议补货清单` 与 `建议调货清单` 会额外包含：
- `装箱数（因子）`
- `建议补货箱数`（仅建议补货清单）
  - 旺季（`season_mode=true`）：`ceil(建议数量 / 装箱数)`
  - 淡季（`season_mode=false`）：`floor(建议数量 / 装箱数)`

Each sheet adds a title in row 1: `库存日期：YYYY-MM-DD` (extracted from the inventory file),
and applies header styling, filters, borders, risk color highlights, and out-of-stock row highlights.

## How to Run
```bash
# macOS/Linux
./run.sh

# Windows
run.bat
```

## Example workflow
1) Drop recent monthly sales files into `raw_data/` as `YYYYMM.xlsx`.
2) Put the inventory snapshot as `raw_data/库存.xlsx` (with or without `品牌`).
3) Update `config.yaml` if needed (files, thresholds, or brand keywords).
4) Run `./run.sh`.
5) Open `reports/inventory_risk_report.xlsx` to review the results.

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
- `carton_factor_file`: carton factor mapping file path (default `./data/华润单品装箱数.xlsx`).
  - Required columns: `商品条码`, `商品名称`, `装箱数（因子）`
- `brand_keywords`: list of brand names used to derive `品牌` from `商品名称` when missing.
