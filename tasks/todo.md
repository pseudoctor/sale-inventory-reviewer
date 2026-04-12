# 模块化重构计划

- [x] 确认当前测试基线可运行
- [x] 拆分 `scripts/core/pipeline.py` 的输入准备、推荐计算、汇总输出职责
- [x] 拆分 `scripts/core/report_writer.py` 的样式处理职责
- [x] 运行最相关测试并记录结果

## review

- 新增 `pipeline_inputs.py`、`pipeline_outputs.py`、`pipeline_transfer.py`，将 `pipeline.py` 保留为兼容编排入口。
- 新增 `report_styles.py`，将 Excel 样式规则从 `report_writer.py` 中下沉。
- 新增 `system_rules.py`，将系统画像、物美默认规则和供商卡号省份映射从主流程中抽离。
- 增加系统规则直接单测，并重新验证全量测试，结果为 `95 passed in 2.80s`。
- 新增 `models.py`，将主配置、批量系统配置、单系统运行结果收敛为 TypedDict，并将批量汇总记录改为 dataclass。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `95 passed in 2.89s`。
- 继续将销售加载结果、窗口上下文、库存准备结果、条码映射结果收敛为 dataclass，主流程已不再依赖字符串键读取这些中间结果。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `97 passed in 2.92s`。
- 已将 `matching.py` 的五元组收敛为 `MatchingResult`，同时保留旧式解包兼容。
- 已将状态页长参数收敛为 `StatusFrameInput`，调用点不再需要手工维护长参数列表。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `99 passed in 2.90s`。
- 已将 `build_report_frames()` 的多 sheet 匿名字典收敛为 `ReportFrames`，并保留按 sheet 名读取和遍历的兼容接口。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.95s`。
- 已将单系统执行过程中的共享运行状态收敛为 `ReportRunContext`，主流程不再依赖大量散落的局部变量。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.93s`。
- 已将 `generate_report_for_system()` 拆分为 `_prepare_input_stage()`、`_build_analysis_stage()`、`_build_output_stage()`、`_write_report_stage()` 四个阶段函数。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 3.14s`。
- 已消除阶段之间的重复计算，`frames`、`product_code_catalog`、门店销量排名调货汇总仅在分析阶段构建一次。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.90s`。
- 已将 `output_tables.py` 的 `build_report_frames()` 拆分为更小的工作表构建函数，降低单函数复杂度。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.93s`。
- 已收缩 `pipeline.py` 的兼容别名，仅保留入口和测试仍实际依赖的少量符号，其余内部逻辑改为直接使用所属模块。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.93s`。
- 已将 `models.py` 中大量 `Any` 收紧为 `pd.DataFrame`、`Path`、`pd.Timestamp` 和明确的结果对象类型，减少类型噪音。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `100 passed in 2.93s`。
- 已新增 `frame_schema.py`，并把销售、库存、匹配结果、报表工作表接入列级 schema 校验。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `101 passed in 2.94s`。
- 已新增 `frame_columns.py`，将关键内部表与工作表列集合提炼为命名常量，并让 schema、构造代码、测试共用同一份列契约。
- 再次验证 `./venv/bin/python -m pytest -q`，结果为 `103 passed in 3.07s`。
