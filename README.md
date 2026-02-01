# Pseudoctor Sale & Inventory Reviewer

Generates an Excel risk report based on the last two months of sales and the current inventory snapshot.

## Inputs
- Monthly sales Excel files (default: `raw_data/YYYYMM.xlsx`)
- Inventory Excel file with store dimension

Required columns in sales files:
- `门店名称`, `品牌`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `销售数量`

Required columns in inventory file:
- `门店名称`, `品牌`, `商品名称`, `商品条码` (or `商品编码`/`商品编码.1`), `数量`

## Output
- `reports/inventory_risk_report.xlsx`
  - `Detail`: Store → Brand → Product level
  - `Store_Summary`: Store-level aggregation
  - `Brand_Summary`: Brand-level aggregation

## How to Run
```bash
# macOS/Linux
./run.sh

# Windows
run.bat
```

## Risk Logic
- Average monthly sales = total sales of last two months / number of available months
- Inventory turnover rate = avg monthly sales / inventory
- Inventory turnover days = 30 / turnover rate
- Risk levels by turnover days:
  - Red: > 60 days
  - Yellow: 45–60 days
  - Green: < 45 days

## Configuration
Edit `config.yaml` to update the sales file list, inventory file, and output path.
