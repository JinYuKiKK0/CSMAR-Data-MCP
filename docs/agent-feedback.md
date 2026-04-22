# csmar-mcp Agent 使用反馈

> 反馈来源：Claude Code Agent 在一次"金融科技对商业银行信贷风险影响"实证项目中调用 csmar-mcp 的全过程
> 反馈日期：2026-04-22
> 核心约束：CSMAR 后端每日 API 限额非常严苛（个位数到十位数级别）

## 一、本次会话的真实调用序列

| #    | 工具                            | 传参摘要                        | 结果                                      |
| ---- | ------------------------------- | ------------------------------- | ----------------------------------------- |
| 1    | `csmar_list_databases`          | 无                              | 返回 62 个库名                            |
| 2    | `csmar_list_tables`             | `database_name="银行财务"`      | 返回 40 个表的 `{table_code, table_name}` |
| 3    | `csmar_get_table_schema`        | `BANK_Index`                    | 字段列表（仅英文+类型）                   |
| 4    | `csmar_get_table_schema`        | `BANK_CreditRisks`              | 同上                                      |
| 5    | `csmar_get_table_schema`        | `BANK_Loan`                     | 同上                                      |
| 6    | `csmar_get_table_schema`        | `BANK_Combas`                   | 字段为代码（如 `A001000000`），无业务名   |
| 7    | `csmar_get_table_schema`        | `BANK_Info`                     | 同上                                      |
| 8–12 | `csmar_probe_query` × 5（并行） | 5 张表，2011-01-01 ~ 2022-12-31 | **全部 `daily_limit_exceeded`**           |

`probe_query` 一次调用即一次性传入该表所有候选字段（如 BANK_Loan 的 9 个字段），不存在"按字段一次调用"的浪费。

## 二、设计原则：最大化每次调用的信息密度 + 尽量不调 API

CSMAR 限额严苛是不可控外部因素，MCP 应在两个方向上对冲：

1. **信息密度最大化**：每次远程调用尽量带回更多可复用元数据。
2. **本地缓存优先**：能从本地拿的就不打 CSMAR。

下文按优先级列出对应改进项。

## 三、改进建议

### P0-1. 本地 schema 缓存（覆盖 `get_table_schema` 与未来的 search）

**问题**：当前 `get_table_schema` 每次都打 CSMAR；如果 CSMAR 把 schema 也计入数据配额，则每次表结构探查都在烧额度。

**建议**：

- MCP 启动时（或首次访问某表时）拉一次 schema，落本地 SQLite/JSON。
- TTL 30 天或手动刷新即可（CSMAR 表结构变更频率极低）。
- 后续 `get_table_schema` 直接读本地，永不打 CSMAR。
- 同一份缓存可以复用给后续的 `search_field` 接口（见 P1-1）。

**收益**：把"schema 探查"这一类高频元数据调用从 API 配额里彻底拿掉。

### P0-2. 字段中文名 / 业务释义反向映射

**问题**：

- `BANK_Loan.Nplra`、`Lpvra`、`Ttdra`、`Lirra` 这种缩写 Agent 必须靠经验+二次验证才能确认含义。
- `BANK_Combas.A001000000` 这种纯代码字段更糟，没有业务名，Agent 必须额外 probe 确认是否就是"资产总计"，纯属浪费配额。

**建议（按可行性排序）**：

1. **首选**：`materialize` 返回的 `[DES][csv].txt` 文件里通常带字段中文名释义，MCP 内部把这个文件解析后并入本地缓存（P0-1 的同一份缓存）。
2. **次选**：对 `BANK_*` 等高频/重要表，人工或半自动维护一份 `field_code → 中文名 / 释义` 字典（CSMAR 官网"数据字典"页面可批量爬取），作为 MCP 的 resource 文件随包发布。
3. 在 `get_table_schema` 输出里追加 `field_name_cn` 与 `description` 两列。Agent 看到中文名就不需要二次验证，节省 1 次 probe。

**收益**：消灭"猜字段→probe 验证"的浪费链路。

### P1-1. `csmar_search_field` 全库字段检索（基于本地缓存）

**问题**：Agent 经常只知道业务概念（"不良贷款率"），不知道在哪张表的哪列。当前必须 `list_databases → list_tables → 挨个 get_table_schema`，O(表数) 次调用。

**建议**：

- 这个接口**完全跑在本地缓存上，不打任何 CSMAR API**——这正是 P0-1 缓存的杀手锏应用。
- 输入：业务关键词（中/英），可选 `database` 过滤。
- 输出：命中的 `{database, table_code, field_name, field_name_cn, description}` 列表。
- 冷启动：可以"懒加载"——只缓存被访问过的库/表，长期下来覆盖高频集；也可以提供一个一次性脚本把全库 schema 爬完落盘。

**关于你之前的顾虑（"search 容易遍历调用打爆 API"）**：
那是因为把 search 实现成"在线检索"了。改为"本地缓存检索"后，search 与 CSMAR 配额完全解耦，不存在这个问题。

**收益**：把字段定位从 O(N) 远程调用降到 O(1) 本地查询。

### P1-2. 批量元数据接口 `csmar_bulk_schema`

**问题**：本次 5 次连续 `get_table_schema` 是典型的批量需求，被强行拆成 5 次调用。

**建议**：

- 新增 `csmar_bulk_schema(table_codes: [...])`，内部并行/合并请求 CSMAR，对外只算 1 次 MCP 调用。
- 配合 P0-1 缓存，多数情况下这个接口直接走本地缓存，零 API 消耗。

### P1-3. `list_databases` 返回三元组

**问题**：当前只返回中文名，`list_tables` 必须传中文名，Agent 容易拼错（如"商业银行研究数据库" vs "银行财务"）。

**建议**：返回 `{name_cn, name_en, code}`，并允许 `list_tables` 接受任一字段作为查询键。

### P2-1. 库内表分组标签

**问题**：`list_tables` 返回的 40 个表是平铺的，Agent 必须靠表名猜业务用途。

**建议**：在表元数据里加 `category` 字段（`资产负债 / 损益 / 风险 / 信息 / 计算指标 / 监管报告` 等），客户端可按类筛选，减少无效 schema 探测。

### P2-2. probe 时按"信息密度"返回更多上下文

**问题**：当前 probe 只返回 `validation_id` + `sample_rows`。

**建议**：probe 响应里顺便带上：

- 该表/该列在样本期内的覆盖率（非空比例）——Agent 可以及早发现某字段大量缺失，避免做完 materialize 才发现数据稀疏。
- 字段类型分布提示（连续/分类/日期）——可指导后续清洗。

零额外 CSMAR 调用，全在 MCP 侧从 `sample_rows` 推导即可。

## 四、Agent 端的最佳实践（已在本次会话验证有效）

供未来调用 csmar-mcp 的 Agent 参考：

1. `probe_query` 一次性把所有候选字段塞进 `columns`，不要按字段拆分。
2. 多个表的 `probe`/`materialize` 优先并行；但若已知配额紧张，改为串行并在每次后判断是否继续。
3. 遇到代码型字段（`A0xxxxxx`）先尝试匹配 CSMAR 通用代码字典，再决定要不要 probe 验证。
4. 每张表的 `materialize` 结果包含 `[DES][csv].txt`，应保存以便后续无需再调 schema 接口。

## 五、总结

CSMAR 限额是硬约束，MCP 的核心战略只有一个：**把绝大多数调用挡在本地缓存层，远程调用只用于真正必须拉数的 `materialize` 与第一次 schema 拉取**。

按 P0 → P1 → P2 顺序推进，预计可以把同等任务的 CSMAR API 消耗降到当前的 1/3 ~ 1/5。
