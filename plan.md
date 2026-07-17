# 需求说明：股票日线数据常驻内存服务（v3）

## 背景与目标

量化策略程序会频繁修改和重复运行，如果每次启动都重新从数据源或磁盘读取、解析全部股票日线数据，会产生较大的等待时间。

本项目需要实现一个**独立常驻进程**：

- 使用 **DuckDB 文件**持久化股票日线、股票列表和更新元数据；
- 常驻进程启动后，将查询数据加载到 DuckDB 内存数据库中；
- 策略进程通过 Arrow Flight 或 Arrow IPC 快速查询；
- 每日收盘后使用 **Baostock** 自动增量更新；
- 更新过程必须避免重复数据，并保证缺失交易日不会被跳过；
- 更新成功后重新加载内存数据，使后续查询立即使用最新快照。

## 整体架构

```text
Baostock
   ↓ 首次导入 / 按缺失交易日增量抓取
磁盘 DuckDB：data/stock_daily.duckdb
   ├─ main_board_daily
   ├─ gem_board_daily
   ├─ main_board_stock_list
   ├─ gem_board_stock_list
   ├─ adjustment_events
   └─ meta
   ↓ 启动加载 / 更新成功后重新加载
常驻进程中的 DuckDB 内存快照
   ↓ Arrow Flight / Arrow IPC（TCP）
策略进程：连接 → 查询 → 断开
```

磁盘 DuckDB 是唯一持久化数据源。内存 DuckDB 只用于快速查询，可以随时从磁盘数据库重新构建。

## 具体功能需求

### 1. 数据范围

- 只采集 **主板** 和 **创业板** 股票；
- 不采集科创板、北交所等其他板块；
- 历史行情从 **2018-01-01** 开始；
- 数据源固定使用 **Baostock**，不再预留 AkShare、Tushare 等其他数据源实现。

### 2. DuckDB 持久化文件

所有数据统一存入一个 DuckDB 文件：

```text
data/stock_daily.duckdb
```

不再使用 Parquet 文件，也不再按股票拆分目录或文件。

数据库至少包含以下六张正式表：

```text
main_board_daily          主板日线数据
gem_board_daily           创业板日线数据
main_board_stock_list     主板股票列表
gem_board_stock_list      创业板股票列表
adjustment_events         根据 preclose 识别出的复权调整事件
meta                      系统元数据，key/value 结构
```

日线表名称必须带有 `daily`，避免与股票列表或其他周期数据混淆。

### 3. 日线表结构

`main_board_daily` 和 `gem_board_daily` 使用相同结构：

```sql
CREATE TABLE IF NOT EXISTS main_board_daily (
    symbol         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    preclose       DOUBLE,
    volume         BIGINT,
    amount         DOUBLE,
    trade_status   TINYINT,
    qfq_factor     DOUBLE NOT NULL DEFAULT 1.0,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS gem_board_daily (
    symbol         VARCHAR NOT NULL,
    date           DATE NOT NULL,
    open           DOUBLE,
    high           DOUBLE,
    low            DOUBLE,
    close          DOUBLE,
    preclose       DOUBLE,
    volume         BIGINT,
    amount         DOUBLE,
    trade_status   TINYINT,
    qfq_factor     DOUBLE NOT NULL DEFAULT 1.0,
    PRIMARY KEY (symbol, date)
);
```

要求：

- `symbol + date` 必须唯一；
- `open`、`high`、`low`、`close`、`preclose` 始终保存 Baostock `adjustflag=3` 返回的不复权价格；
- `preclose` 是计算复权调整比例的必要字段，不能省略；
- `qfq_factor` 是从不复权数据推导出的前复权因子缓存，不是新的行情真源；
- `trade_status` 用于区分正常交易、停牌和接口遗漏；
- Baostock 返回的字符串字段在写入前统一转换为正确的数据类型；
- 只有文档允许为空的可选字段才能将空字符串转换为 `NULL`；`symbol`、`date`、OHLC、`preclose`、`adjustflag`、`trade_status` 等必需字段为空、类型错误或数值非法时，必须将整轮任务判定为致命数据错误；
- 不允许为了让批次继续写入而静默丢弃、填零、修正或跳过异常行情记录；
- 如后续需要保存涨跌幅、是否 ST 等 Baostock 字段，可以在此结构上扩展，但不能破坏唯一键。

#### 3.1 不复权存储与前复权因子

磁盘中的不复权 OHLC 和 `preclose` 是唯一价格真源。前复权价格不从 Baostock 单独保存一份完整副本，而是使用 `qfq_factor` 计算：

```text
qfq_open     = open     × qfq_factor
qfq_high     = high     × qfq_factor
qfq_low      = low      × qfq_factor
qfq_close    = close    × qfq_factor
qfq_preclose = preclose × qfq_factor
```

`volume` 和 `amount` 保持 Baostock 原始值，不乘价格复权因子。

对单只股票按日期升序排列，设第 `i` 个有效交易日的不复权收盘价为 `C[i]`、Baostock 返回的昨收价为 `P[i]`、前复权因子为 `F[i]`。以当前最后一个有效交易日为基准：

```text
F[n] = 1
F[i-1] = F[i] × P[i] / C[i-1]
```

其中 `C[i-1]` 必须取该股票在 `P[i]` 之前最近一条非空、有效的不复权收盘价，不能简单取前一个自然日。计算中使用配置项 `factor_epsilon` 判断比例是否可视为 `1`，避免浮点转换噪声被误判为调整事件。首次导入完成单只股票的不复权数据后，使用上述公式从后向前计算该股票全部 `qfq_factor`。

增量更新时，对每只股票按日期顺序处理新数据。设本地最近有效收盘价为 `previous_close`，新交易日返回的昨收价为 `preclose`：

```text
event_factor = preclose / previous_close
```

- `event_factor` 在配置误差范围内等于 `1`：没有新的价格基准调整，历史因子不变；
- `event_factor` 不等于 `1`：识别为新的除权除息或价格基准调整事件，事件日之前的因子都需要乘以该比例；
- 一轮补数包含多个交易日时，必须严格按日期升序计算，使多个调整事件可以连续累积；
- 除已核实为新股上市首条行情的情况外，`preclose`、`previous_close` 为空、为零或无法衔接时，不允许猜测因子；该情况属于致命数据错误，必须回滚并终止程序；
- 新股上市首条行情没有本地 `previous_close`，该行作为该股票的初始锚点，不生成调整事件；待完整历史区间抓取后再从最后一个有效交易日向前统一计算因子。

为便于审计和保证重复执行的幂等性，保存调整事件：

```sql
CREATE TABLE IF NOT EXISTS adjustment_events (
    symbol          VARCHAR NOT NULL,
    ex_date         DATE NOT NULL,
    previous_close  DOUBLE NOT NULL,
    preclose        DOUBLE NOT NULL,
    event_factor    DOUBLE NOT NULL,
    detected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, ex_date)
);
```

同一个 `symbol + ex_date` 的调整事件只能应用一次。任务重跑时：

- 事件不存在：插入事件并更新该股票历史因子；
- 事件已存在且比例一致：跳过重复应用；
- 事件已存在但比例变化：视为可能的数据源修订或历史数据冲突，禁止自动覆盖或自动重建，必须触发致命数据错误并等待人工确认；只有显式修复命令才允许根据该股票全部不复权 `close/preclose` 确定性重建因子。

无论首次导入还是增量更新，因子写入、`adjustment_events` 写入、日线合并和 `meta.last_update_trade_date` 推进必须位于同一个数据库事务中。`qfq_factor` 只是一份可重建缓存；如发现异常，可以随时从不复权 `close/preclose` 全量重建指定股票的因子。

#### 3.2 Baostock 日线字段与停牌语义

首次全量导入、新股历史回补和单股修复使用 `query_history_k_data_plus()`，并且只使用它的日线能力：

```python
bs.query_history_k_data_plus(
    symbol,
    "date,code,open,high,low,close,preclose,volume,amount,adjustflag,turn,tradestatus,pctChg,isST",
    start_date=start_date,
    end_date=end_date,
    frequency="d",
    adjustflag="3",
)
```

Baostock 虽可查询日、周、月和分钟 K 线，也支持不复权、前复权和后复权，但本项目的持久化接口固定为 `frequency="d"`、`adjustflag="3"`。项目开始日期仍为 2018-01-01，不因数据源支持更早历史而改变。

本项目使用的字段语义如下：

| Baostock 字段 | 本地字段 | 使用规则 |
| --- | --- | --- |
| `date` | `date` | 交易所行情日期，转为 `DATE` |
| `code` | `symbol` | 保持 `sh.600000`、`sz.000001` 格式 |
| `open/high/low/close` | 同名字段 | 保存不复权价格 |
| `preclose` | `preclose` | 交易所公布的当日价格基准，也是复权事件计算依据 |
| `volume` | `volume` | 成交股数，转为 `BIGINT` |
| `amount` | `amount` | 成交额，单位为人民币元 |
| `adjustflag` | 不单独持久化 | 必须校验为 `3`，否则拒绝写入 |
| `tradestatus` | `trade_status` | `1` 为正常交易，`0` 为停牌 |
| `turn/pctChg/isST` | 暂不持久化 | 可用于校验或后续扩展 |

`preclose` 不是简单复制上一条实际收盘价：

- 普通交易日通常等于上一有效交易日的不复权收盘价；
- 当日发生分红、送股、配股等除权除息时，它是交易所根据股权登记日收盘价和公司行为计算并公布的除权除息基准价；
- 首发日的 `preclose` 等于首发价格，但首发日没有本地上一收盘价，不生成调整事件；
- 因此 `preclose / previous_close` 不等于 `1` 正是本项目识别价格基准调整事件的依据。

Baostock 使用涨跌幅复权法，不同数据系统的复权结果可能不同。验收时以本项目根据 Baostock `close/preclose` 重建后的收益连续性为准，不要求与同花顺、通达信等第三方前复权价格逐值一致。

日线包含停牌证券。停牌记录的处理规则：

- `trade_status = 0`；
- `open`、`high`、`low`、`close` 相同，通常为前一有效交易日收盘价；
- `volume = 0`、`amount = 0`；
- `turn` 可能为空；
- 上述组合是合法停牌数据，不能按异常值删除，也不能把空换手率强制解释为行情缺失。

### 4. 股票列表表结构

DuckDB 中使用两张独立表保存股票列表：

```sql
CREATE TABLE IF NOT EXISTS main_board_stock_list (
    symbol                 VARCHAR PRIMARY KEY,
    name                   VARCHAR,
    ipo_date               DATE,
    out_date               DATE,
    listing_status         VARCHAR,
    trade_status           TINYINT,
    first_seen_trade_date  DATE,
    last_seen_trade_date   DATE,
    updated_at             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS gem_board_stock_list (
    symbol                 VARCHAR PRIMARY KEY,
    name                   VARCHAR,
    ipo_date               DATE,
    out_date               DATE,
    listing_status         VARCHAR,
    trade_status           TINYINT,
    first_seen_trade_date  DATE,
    last_seen_trade_date   DATE,
    updated_at             TIMESTAMP
);
```

证券列表使用 `query_all_stock(day=target_trade_date)` 获取。一次增量任务只对本轮最新目标交易日调用一次；该接口返回的 A 股和指数必须先筛选为主板、创业板，不能直接写入股票列表。

1. 调用一次 `query_all_stock(day=target_trade_date)` 获取最新证券集合；
2. 与本地证券列表比较，每只新增股票调用一次 `query_history_k_data_plus()` 回补历史；
3. 每个缺失交易日只调用 `query_daily_history_k_AStock(date)` 获取当日日 K；
4. 仅在最新目标日使用 `code` 关联证券列表与日 K，校验代码集合和交易状态一致；历史补数日不能拿最新列表强行比对；
5. 新股历史、最新证券列表、目标日日 K、复权因子和调整事件在目标日事务中原子提交。

字段含义必须区分：

- `trade_status` 来自 `query_all_stock.tradeStatus`，只表示该日正常交易或停牌；
- `listing_status` 表示上市生命周期，例如 `active`、`pending_out`、`delisted`；
- 停牌股票仍是 `listing_status=active`，不能因为 `trade_status=0` 标记为退市；
- `query_all_stock()` 不返回 `ipo_date` 和 `out_date`，不能仅凭该接口伪造这两个日期；未知值允许为 `NULL`，需要由可靠的证券基本信息或后续已验证的生命周期流程补充；
- 某股票单日未出现在证券列表中时，先标记并复核，不能只凭一次缺失立即判定退市。

每次执行更新任务时，将筛选后的证券列表与本地两张股票列表表进行比较：

- 新上市股票：写入股票列表，并补拉该股票自上市日或 2018-01-01 起的历史日线；
- 股票名称、交易状态等信息变化：更新对应记录；
- 已退市股票：更新状态和退市日期，历史日线默认保留；
- 股票列表没有变化：跳过列表写入，继续执行日线更新。

股票板块归类逻辑必须集中在 `data_fetcher.py` 中，避免服务端和初始化脚本各自维护不同规则。

### 5. Meta 表

使用 `meta` 表保存系统级状态：

```sql
CREATE TABLE IF NOT EXISTS meta (
    key         VARCHAR PRIMARY KEY,
    value       VARCHAR,
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

至少保存以下键：

```text
last_update_trade_date    已完整更新并校验成功的最后一个交易日
```

可选保存：

```text
initial_import_completed
initial_import_last_symbol
stock_list_last_update_date
price_mode                    固定为 raw，表示 OHLC 持久化为不复权价格
qfq_algorithm                 固定为 preclose_ratio
schema_version
```

`last_update_trade_date` 只能在一个完整增量批次全部抓取、校验和提交成功后更新。不能在处理中提前修改，否则程序中断后可能跳过尚未完成的交易日。

### 6. 首次导入

本地 DuckDB 不存在或日线表为空时，执行首次导入：

1. 登录 Baostock；
2. 固定本轮导入的 `target_trade_date`，调用 `query_all_stock(day=target_trade_date)` 获取证券集合，排除指数和非目标板块后写入两张股票列表表；
3. 对股票列表逐只调用 `query_history_k_data_plus()`，固定使用 `frequency="d"`、`adjustflag="3"`，并至少请求 `date`、`code`、OHLC、`preclose`、`volume`、`amount`、`adjustflag`、`tradestatus`，获取从以下日期开始的数据：
   - 上市日在 2018-01-01 之后：从上市日开始；
   - 上市日在 2018-01-01 之前：从 2018-01-01 开始；
4. 对单只股票按日期升序校验不复权数据，并使用 `preclose` 公式从最后一个有效交易日向前计算全部 `qfq_factor`；
5. 将识别出的调整事件写入 `adjustment_events` staging，将日线写入对应的 `*_daily` staging；
6. 单只股票获取和因子计算成功后，按批次写入正式表；
7. 使用 `symbol + date` 唯一键或反连接插入，保证重复执行不会产生重复记录；
8. 记录导入进度，支持程序中断后继续执行；
9. 全部股票的不复权数据、前复权因子和调整事件均导入并校验成功后，写入 `last_update_trade_date`。

`initial_import_target_date` 在首次导入开始时固定，续传不得改成新的交易日。单股历史、调整事件和该股票的完成进度必须在同一事务提交：中断发生在下载/计算阶段时不写入该股票；发生在事务阶段时整体回滚；只有提交成功的股票才允许在重启后跳过。首次导入全部完成后，必须在构建查询快照前立即运行增量补数，将固定目标日至当前最新完整交易日之间的数据补齐。

首次导入必须使用 `query_history_k_data_plus()` 按股票逐只获取，重点保证稳定性、错误重试和断点续传，不使用 `query_daily_history_k_AStock()` 代替首次全量导入。

### 7. 启动时校验数据是否最新

常驻进程启动时，不需要逐只股票检查最后日期，只使用 `meta.last_update_trade_date` 作为本地整体更新进度。

启动流程：

1. 读取 `meta.last_update_trade_date`；
2. 使用 Baostock 交易日历或指数日线获取当前真实的最后一个交易日；
3. 比较两个日期：
   - 相同：直接加载数据并启动查询服务；
   - 本地落后：计算中间所有缺失交易日，按日期顺序执行增量补数；
   - 本地日期异常或晚于数据源日期：记录错误并停止自动推进，避免覆盖问题。

校验重点是**整体交易日进度**，不在启动时对每只股票逐个请求或逐个验证。

### 8. 增量更新与批量获取

日常增量更新中，`query_all_stock(day)` 只负责获取最新目标交易日的证券列表，`query_daily_history_k_AStock(date)` 负责获取每个缺失交易日的全部 A 股日 K。

- 一次更新任务覆盖一个或多个缺失交易日；
- 对最新目标交易日调用一次 `query_all_stock(day)`，严格按缺失交易日升序，每个交易日调用一次 `query_daily_history_k_AStock(date)`；
- 即使本地缺少约一年的数据，证券列表接口仍只调用一次，日 K 接口通常调用约 240 至 250 次，而不是股票数量乘以交易日数量；
- 将最新证券列表与本地列表比较；发现新增股票时，每只新增股票调用一次 `query_history_k_data_plus()`，从 2018-01-01 或上市后首个可用交易日回补历史；
- 接口返回全体 A 股后，由 `data_fetcher.py` 使用统一板块规则筛选主板和创业板，排除科创板、北交所及其他证券；
- 返回结果必须包含 `preclose`、`tradestatus` 和 `adjustflag`，并校验持久化价格确实为不复权口径；
- 每个交易日的结果先整体写入 DataFrame、Arrow Table 或 DuckDB staging 表，不允许逐行提交；
- 单日网络、登录或 Baostock 临时服务失败时只重试该交易日；已经成功写入 staging 且通过单日校验的其他交易日不需要重新请求；如果返回了结构或内容异常的行情，则不重试、不跳过，直接进入失败关闭流程；
- `query_history_k_data_plus(symbol, ...)` 只用于首次历史导入、新上市股票回补、单股修复或日级接口无法完成的异常修复，不作为日常全市场增量更新的主路径。

建议接口：

```python
fetch_market_daily(trade_date: str) -> pandas.DataFrame

fetch_market_daily_dates(
    trade_dates: list[str],
) -> pandas.DataFrame
```

其中 `fetch_market_daily()` 直接封装 `query_daily_history_k_AStock(date)`；`fetch_market_daily_dates()` 按日期升序调用前者并汇总结果。一次更新任务只调用 `fetch_stock_list(target_trade_date)`，不为每个历史补数日重复获取证券列表。

### 9. 防重复、防遗漏和事务要求

增量更新必须遵循以下流程：

1. 根据 `last_update_trade_date` 和 Baostock 最新交易日，生成完整的缺失交易日列表；
2. 先获取一次最新目标日证券列表并识别、下载新增股票历史，再严格按日期顺序调用 `query_daily_history_k_AStock(date)`，不允许直接跳到最新日期；
3. 将本轮证券列表和不复权行情结果分别写入临时 staging 表；
4. 对 staging 数据进行去重，唯一键为 `symbol + date`；
5. 校验以下内容：
   - 缺失交易日列表中的每个交易日都已经执行抓取；
   - 每次响应中的 `date` 必须全部等于本次请求的交易日；
   - 最新目标日 K 线中的主板和创业板代码必须与最新 `query_all_stock()` 筛选结果覆盖一致；历史补数日不使用最新列表做集合判断；
   - 最新目标日两个接口中的交易状态必须一致；停牌证券应有 `trade_status=0` 的合法日线记录；
   - 同一个交易日的同一股票不能出现多条记录；
   - `adjustflag` 必须符合不复权存储要求；
   - 不存在无法解释的日期断层；
   - 停牌、未上市、已退市等无行情情况必须有明确状态，不能被误判为接口遗漏；
6. 对每只股票按日期升序使用 `preclose / previous_close` 检测本轮调整事件；
7. 对无新事件的股票直接追加新行，新行 `qfq_factor` 初始为 `1`；对有新事件的股票更新事件日前的历史因子，并处理本轮后续可能出现的其他事件；
8. 校验 `adjustment_events`，确保同一事件没有重复应用；若已存事件的比例发生变化，则确定性重建该股票全部因子；
9. 在一个数据库事务中，将 staging 数据插入正式表，同时提交因子变更和调整事件；
10. 插入时使用唯一键约束、`NOT EXISTS`、`ANTI JOIN` 或等价方式过滤已有记录；
11. 再次校验正式表已覆盖本轮交易日期，并抽查前复权连续性；
12. 全部成功后，才更新 `meta.last_update_trade_date` 并提交事务；
13. 任一步骤发现数据完整性异常，立即回滚整个批次，日线、因子、调整事件和 `last_update_trade_date` 均保持原状态，然后按下述“失败关闭”规则终止整个服务进程。

推荐插入方式示例：

```sql
INSERT INTO main_board_daily
SELECT s.*
FROM main_board_daily_staging AS s
WHERE NOT EXISTS (
    SELECT 1
    FROM main_board_daily AS d
    WHERE d.symbol = s.symbol
      AND d.date = s.date
);
```

唯一键负责最终防重，staging 校验和事务负责防止中间遗漏及半批次提交。

#### 9.1 失败关闭、致命错误标记和日志

本项目以行情可靠性优先，采用强制的 **fail-closed（失败关闭）** 策略。只要下载结果无法通过完整性校验，就不能继续提供查询服务，也不能等待下一次定时任务静默重试。

以下情况属于致命数据错误，发现任意一项即停止本轮并终止程序：

- Baostock 返回成功状态，但缺少必需字段、字段类型无法转换或返回空结果且无法解释；
- 返回日期不是请求日期，出现未来日期、区间外日期或日期顺序异常；
- 证券代码格式错误、板块分类无法确定、同一 `symbol + date` 重复；
- 最新目标日的 `query_all_stock()` 与 `query_daily_history_k_AStock()` 目标股票集合或交易状态不一致；
- `adjustflag` 不是 `3`，或同一批次混入不同复权口径；
- 正常交易记录的 OHLC、`preclose` 非正数，`high/low` 关系非法，成交量或成交额为负；
- 停牌记录不符合 `trade_status=0` 的约束，或被错误地当作缺失记录；BaoStock 在停牌、OHLC 相等且成交额为 0 时返回的空成交量或残留成交量可确定性规范化为 0，其他异常不得放行；
- `preclose` 无法与上一有效收盘价衔接，复权事件重复、比例冲突或重建后收益连续性校验失败；
- staging 与正式表的行数、唯一键、日期覆盖或校验摘要不一致；
- 数据库提交后构建新内存快照失败，导致磁盘版本与对外快照版本不一致。

致命数据错误的处理顺序固定为：

1. 停止调度器，不再接受新的 Flight 查询；
2. 如果磁盘事务尚未提交，则回滚并保持 `last_update_trade_date` 不变；如果磁盘已经提交但新内存快照构建失败，则不得伪造回滚，保留已提交磁盘版本并在致命错误标记中同时记录磁盘版本与旧快照版本；
3. 将错误同时打印到标准错误输出和滚动错误日志，并强制刷新日志；
4. 原子写入 `runtime/FATAL_ERROR.json`，至少包含发生时间、阶段、请求日期、股票代码、校验规则、期望值、实际值、Baostock 错误码、`run_id`、异常堆栈和日志文件位置；
5. 数据完整性错误以退出码 `4`、磁盘或快照错误以退出码 `5` 终止整个进程；之前已验证并提交的数据保留，但服务不再继续运行。

程序启动时必须先检查 `runtime/FATAL_ERROR.json`。标记存在时，打印上次致命错误摘要并拒绝启动、拒绝自动下载，避免 Windows 或 Linux 的开机自启动策略反复写入或反复请求。人工处理流程为：

```text
python server.py doctor --config config.yaml
python server.py clear-fatal --config config.yaml --confirm
python server.py serve --config config.yaml
```

`doctor` 必须以只读方式检查配置、DuckDB schema、唯一键、日期覆盖、复权因子和最后一次已提交快照；只有检查通过后，`clear-fatal --confirm` 才允许删除致命错误标记。删除标记不修改行情数据和 `last_update_trade_date`。

网络超时、连接中断、登录失败以及 Baostock 明确返回的临时服务错误不属于“错误行情已返回”，可以按配置重试；达到最大重试次数后仍失败，则记录 `CRITICAL` 日志并以退出码 `3` 终止进程。配置/schema 错误使用退出码 `2`，数据完整性错误使用退出码 `4`，磁盘或快照错误使用退出码 `5`，正常主动停止使用退出码 `0`。

日志要求：

- 控制台和文件同时输出，文件使用 UTF-8；
- 使用按大小滚动的主日志和独立错误日志，保留多个历史文件；
- 每轮导入或更新生成唯一 `run_id`，所有抓取、校验、事务和快照日志都携带该字段；
- 错误日志必须包含请求接口、请求日期/区间、股票代码、响应字段、响应行数、Baostock `error_code/error_msg`、失败规则和异常堆栈；
- 可记录异常响应的有限样本和摘要哈希用于排查，但不能把未验证的响应写入正式行情表；
- 进程退出前必须显式刷新并关闭日志处理器。

单一数据源无法从数学上证明 Baostock 上游数据本身“绝对真实”；本方案保证的是：任何未通过结构、范围、覆盖、状态、价格关系、复权连续性和事务一致性校验的数据都不会进入正式表，也不会继续对外服务。如果需要验证 Baostock 上游数值本身，还必须引入独立第二数据源交叉核对，这不在当前“数据源固定为 Baostock”的范围内。

### 10. 定时更新任务

更新时间通过配置文件指定：

```yaml
# config.yaml
database_path: "data/stock_daily.duckdb"
start_date: "2018-01-01"
update_time: "18:30"
retry_interval_minutes: 5
max_retries: 12
initial_import_batch_size: 100
factor_epsilon: 1.0e-10
flight_host: "127.0.0.1"
flight_port: 8815
runtime_dir: "runtime"
log_path: "logs/qstockdataserver.log"
error_log_path: "logs/qstockdataserver.error.log"
log_level: "INFO"
log_max_bytes: 10485760
log_backup_count: 10
```

每天到配置时间后执行：

1. 从 Baostock 获取最新交易日；
2. 根据 `meta.last_update_trade_date` 计算全部缺失交易日；
3. 对每个缺失交易日配对获取证券列表和全市场日 K，并同步更新股票列表；
4. 没有缺失日期：记录“数据已是最新”，结束本次任务；
5. 有缺失日期：执行批量抓取、staging 校验和事务写入；
6. 网络异常或接口错误：按配置间隔重试；
7. 当天为非交易日且没有缺失历史交易日：正常跳过，不进入失败重试；
8. 达到最大重试次数仍失败：保留原 `last_update_trade_date`，写入 `CRITICAL` 日志并以退出码 `3` 终止程序，不继续提供查询服务。

每日行情更新使用 **APScheduler** 的 cron 方式并嵌入常驻进程，不依赖操作系统调度器；Windows 任务计划程序或 Linux systemd 只负责开机启动和进程管理。

### 11. 更新成功后重新加载数据

磁盘更新完成后，必须重新加载查询数据，不能只修改磁盘库后继续使用旧内存快照。

推荐使用“构建新快照后原子切换”的方式：

1. 更新线程完成磁盘 DuckDB 事务并提交；
2. 创建一个新的 DuckDB 内存连接；
3. 从 `data/stock_daily.duckdb` 重新加载以下表：
   - `main_board_daily`
   - `gem_board_daily`
   - `main_board_stock_list`
   - `gem_board_stock_list`
   - `adjustment_events`
   - `meta`
4. 新内存库加载和基础校验成功后，获取写锁；
5. 将查询服务当前连接原子替换为新连接；
6. 释放写锁并关闭旧连接。

在新快照加载期间，已开始的查询可以完成；切换成功后新查询使用新快照。如果新快照加载或校验失败，立即停止接受新查询、写入致命错误标记并以退出码 `5` 终止程序，不能长期继续对外提供旧快照。

也可以使用等价的读写锁方案，但必须满足：

- 查询和重载不能同时修改同一连接；
- 重载期间不得关闭仍被活动查询引用的旧快照；
- 只有磁盘提交和新快照加载都成功，才对外切换最新数据。

### 12. 内存表加载

内存 DuckDB 中使用与磁盘库相同的正式表名：

```sql
ATTACH 'data/stock_daily.duckdb' AS disk_db (READ_ONLY);

CREATE TABLE main_board_daily AS
SELECT * FROM disk_db.main_board_daily;

CREATE TABLE gem_board_daily AS
SELECT * FROM disk_db.gem_board_daily;

CREATE TABLE main_board_stock_list AS
SELECT * FROM disk_db.main_board_stock_list;

CREATE TABLE gem_board_stock_list AS
SELECT * FROM disk_db.gem_board_stock_list;

CREATE TABLE adjustment_events AS
SELECT * FROM disk_db.adjustment_events;

CREATE TABLE meta AS
SELECT * FROM disk_db.meta;

DETACH disk_db;

CREATE VIEW main_board_daily_qfq AS
SELECT
    symbol,
    date,
    open * qfq_factor AS open,
    high * qfq_factor AS high,
    low * qfq_factor AS low,
    close * qfq_factor AS close,
    preclose * qfq_factor AS preclose,
    volume,
    amount,
    trade_status
FROM main_board_daily;

CREATE VIEW gem_board_daily_qfq AS
SELECT
    symbol,
    date,
    open * qfq_factor AS open,
    high * qfq_factor AS high,
    low * qfq_factor AS low,
    close * qfq_factor AS close,
    preclose * qfq_factor AS preclose,
    volume,
    amount,
    trade_status
FROM gem_board_daily;
```

策略进程查询 `main_board_daily`、`gem_board_daily` 获得不复权价格，查询 `main_board_daily_qfq`、`gem_board_daily_qfq` 获得前复权价格。两个板块都可以使用 `UNION ALL` 合并查询。

### 13. 进程间通信

- 优先使用 **Arrow Flight**；
- 也可使用 Arrow IPC + TCP socket；
- 不使用 pickle 或 `multiprocessing.managers`；
- 必须同时支持 Windows 和 Linux，统一使用 TCP，不依赖 Unix Domain Socket；
- 客户端允许频繁连接和断开，不要求长连接。

客户端调用示例：

```python
from client import StockDataClient

client = StockDataClient()

df = client.query("""
    SELECT *
    FROM main_board_daily
    WHERE symbol = 'sh.600519'
      AND date >= '2024-01-01'
    ORDER BY date
""")
```

查询主板和创业板全市场：

```python
df = client.query("""
    SELECT * FROM main_board_daily WHERE date = '2024-06-28'
    UNION ALL
    SELECT * FROM gem_board_daily WHERE date = '2024-06-28'
""")
```

股票代码格式以 Baostock 原始格式为准，例如：

```text
sh.600519
sz.000001
sz.300750
```

## Windows 和 Linux 开机自启动

服务必须同时支持 Windows 和 Linux。Python 代码、配置格式、数据库格式和 Flight TCP 协议保持一致，平台差异只放在启动脚本和服务管理配置中。所有示例都必须使用绝对路径，避免开机启动时当前工作目录不同导致数据库或日志写到错误位置。

### Windows：任务计划程序

普通 Python 程序不能直接通过 `sc.exe create` 伪装成 Windows Service；本项目使用 Windows 任务计划程序在系统启动时拉起常驻进程。任务计划程序只负责开机启动，日常行情更新仍由进程内 APScheduler 执行。

以管理员身份打开 PowerShell，将路径替换为实际安装目录后执行：

```powershell
$ProjectRoot = "C:\QStockDataServer"
$Python = "$ProjectRoot\.venv\Scripts\python.exe"
$Server = "$ProjectRoot\server.py"
$Config = "$ProjectRoot\config.yaml"

$Action = New-ScheduledTaskAction `
    -Execute $Python `
    -Argument "`"$Server`" serve --config `"$Config`"" `
    -WorkingDirectory $ProjectRoot

$Trigger = New-ScheduledTaskTrigger -AtStartup

$Settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -RestartCount 3 `
    -RestartInterval (New-TimeSpan -Minutes 1) `
    -ExecutionTimeLimit ([TimeSpan]::Zero) `
    -MultipleInstances IgnoreNew

Register-ScheduledTask `
    -TaskName "QStockDataServer" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings `
    -User "SYSTEM" `
    -RunLevel Highest `
    -Force

Start-ScheduledTask -TaskName "QStockDataServer"
```

常用管理命令：

```powershell
Get-ScheduledTask -TaskName "QStockDataServer"
Get-ScheduledTaskInfo -TaskName "QStockDataServer"
Stop-ScheduledTask -TaskName "QStockDataServer"
Start-ScheduledTask -TaskName "QStockDataServer"
Unregister-ScheduledTask -TaskName "QStockDataServer" -Confirm:$false
Get-Content "C:\QStockDataServer\logs\qstockdataserver.error.log" -Wait
```

任务最多自动重启三次。若存在 `runtime/FATAL_ERROR.json`，进程每次都会拒绝启动；必须先人工运行 `doctor` 并清除致命错误标记。

### Linux：systemd

交付 `deploy/qstockdataserver.service` 模板，示例内容：

```ini
[Unit]
Description=QStockDataServer
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=300
StartLimitBurst=3

[Service]
Type=simple
User=qstock
Group=qstock
WorkingDirectory=/opt/QStockDataServer
ExecStart=/opt/QStockDataServer/.venv/bin/python /opt/QStockDataServer/server.py serve --config /opt/QStockDataServer/config.yaml
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=10
RestartPreventExitStatus=2 4 5
TimeoutStopSec=30

[Install]
WantedBy=multi-user.target
```

安装和启用命令：

```bash
id -u qstock >/dev/null 2>&1 || sudo useradd --system --home /opt/QStockDataServer --shell /usr/sbin/nologin qstock
sudo chown -R qstock:qstock /opt/QStockDataServer
sudo cp deploy/qstockdataserver.service /etc/systemd/system/qstockdataserver.service
sudo systemctl daemon-reload
sudo systemctl enable --now qstockdataserver.service
sudo systemctl status qstockdataserver.service
sudo journalctl -u qstockdataserver.service -f
```

常用管理命令：

```bash
sudo systemctl stop qstockdataserver.service
sudo systemctl start qstockdataserver.service
sudo systemctl restart qstockdataserver.service
sudo systemctl disable --now qstockdataserver.service
```

systemd 对临时退出码 `3` 可以有限重启，但 `RestartPreventExitStatus=2 4 5` 禁止对配置错误、数据完整性错误和磁盘/快照错误自动重启。即使服务管理配置被误改，`runtime/FATAL_ERROR.json` 仍提供第二层启动保护。

## 模块划分

### `server.py`

常驻进程主程序，负责：

- 初始化磁盘 DuckDB 表结构；
- 首次导入或启动补数；
- 加载内存 DuckDB 快照；
- 启动 Arrow Flight 查询服务；
- 启动 APScheduler 定时任务；
- 控制更新锁、查询读锁和内存快照原子切换；
- 启动前检查致命错误标记；
- 提供 `serve`、`doctor`、`clear-fatal` 命令；
- 发生数据完整性或快照错误时停止服务、写入标记并按约定退出码终止进程。

### `client.py`

客户端封装，至少提供：

```python
query(sql: str) -> pandas.DataFrame
```

要求：

- 自动连接 Arrow Flight 服务；
- 发送 SQL；
- 接收 Arrow 数据并转换为 pandas DataFrame；
- 请求完成后允许立即断开；
- 对服务不可用、SQL 错误提供清晰异常信息。

### `data_fetcher.py`

只负责 Baostock 数据访问和字段标准化，至少提供：

```python
login() -> None
logout() -> None
fetch_trade_dates(start_date: str, end_date: str) -> list[str]
fetch_last_trading_date() -> str
fetch_stock_list(trade_date: str, board: str | None = None) -> pandas.DataFrame
fetch_stock_history(symbol: str, start_date: str, end_date: str) -> pandas.DataFrame
fetch_market_daily(trade_date: str) -> pandas.DataFrame
fetch_market_daily_dates(trade_dates: list[str]) -> pandas.DataFrame
```

其中：

- `fetch_last_trading_date()` 优先使用 Baostock 交易日历确定最后交易日；
- `fetch_stock_list()` 封装 `query_all_stock(day=trade_date)`，过滤指数和非目标板块，标准化 `code`、`code_name`、`tradeStatus`；
- `fetch_stock_history()` 封装 `query_history_k_data_plus()`，用于首次导入、新股历史回补和单股票修复；
- `fetch_market_daily()` 封装 `query_daily_history_k_AStock(date)`，用于获取单个交易日全部 A 股日 K 线并筛选目标板块；
- `fetch_market_daily_dates()` 按日期升序批量补齐一个或多个缺失交易日；仅临时网络/API 故障允许按日重试，数据校验失败必须立即抛出致命错误；
- 所有日线接口必须获取不复权数据，并且请求或返回 `preclose`、`tradestatus` 和 `adjustflag`；
- 全市场日级接口返回后必须检查 `adjustflag`，不能把未知或不同复权口径的数据写入不复权正式表；
- `data_fetcher.py` 不直接计算或持久化前复权因子，因子计算由掌握本地上一有效收盘价和事务状态的 `db_manager.py` 负责；
- 所有接口返回统一字段名和数据类型；
- 登录会话、错误码、错误信息和重试逻辑统一封装，不能散落在 `server.py` 中；临时传输错误和致命数据错误必须使用不同异常类型，禁止对致命数据错误自动重试。

### `db_manager.py`

建议新增数据库管理模块，负责：

- 创建表和 schema 版本检查；
- staging 表管理；
- 批量插入、去重和事务提交；
- 读取与更新 meta；
- 数据完整性校验；
- 使用 `preclose / previous_close` 计算、校验和确定性重建 `qfq_factor`；
- 管理 `adjustment_events`，保证调整事件不会被重复应用；
- 构建新的内存快照；
- 为 `doctor` 提供只读的全库一致性检查。

### `logging_config.py`

统一配置控制台日志、滚动主日志和滚动错误日志，负责：

- UTF-8 输出和统一日志格式；
- 注入 `run_id`、任务阶段、交易日期和股票代码上下文；
- `ERROR`、`CRITICAL` 进入独立错误日志；
- 生成和原子写入 `runtime/FATAL_ERROR.json`；
- 退出前刷新并关闭全部日志处理器。

### `config.yaml`

保存：

- DuckDB 文件路径；
- 历史数据起始日期；
- 每日更新时间；
- 重试间隔和最大重试次数；
- 首次逐股历史导入的落库批次大小；
- 复权因子比较误差 `factor_epsilon`；
- Arrow Flight 地址和端口；
- runtime 目录和致命错误标记位置；
- 主日志、错误日志、日志级别、滚动大小和保留数量。

## 技术选型总结

| 模块       | 技术选型                                        |
| ---------- | ----------------------------------------------- |
| 数据源     | Baostock                                        |
| 证券列表接口 | `query_all_stock(day)`，每次更新只获取最新目标交易日一次 |
| 日常增量接口 | `query_daily_history_k_AStock(date)`，每个缺失交易日一次 |
| 单股历史接口 | `query_history_k_data_plus(symbol, ...)`，用于首次导入、新股回补和修复 |
| 持久化存储 | DuckDB 文件 `data/stock_daily.duckdb`           |
| 日线表     | `main_board_daily`、`gem_board_daily`           |
| 价格存储   | 不复权 OHLC、`preclose` 和可重建的 `qfq_factor` |
| 前复权查询 | 内存视图 `main_board_daily_qfq`、`gem_board_daily_qfq` |
| 调整事件   | `adjustment_events`                             |
| 股票列表   | `main_board_stock_list`、`gem_board_stock_list` |
| 更新元数据 | `meta(key, value, updated_at)`                  |
| 查询层     | 常驻进程中的 DuckDB 内存快照                    |
| 进程间传输 | Arrow Flight 优先，或 Arrow IPC TCP             |
| 定时调度   | APScheduler cron                                |
| 错误策略   | fail-closed、滚动错误日志、致命错误标记和非零退出码 |
| 配置管理   | YAML                                            |
| 运行平台   | Windows 和 Linux，使用 TCP loopback             |
| 开机启动   | Windows Task Scheduler / Linux systemd          |

## 交付物要求

1. `server.py`
   - 初始化磁盘 DuckDB；
   - 首次导入；
   - 启动时根据 `meta.last_update_trade_date` 自动补数；
   - 启动 Arrow Flight 服务；
   - 运行 APScheduler 定时更新；
   - 更新成功后重新加载并原子切换内存快照；
   - 提供 `serve`、`doctor`、`clear-fatal` 命令和规定的进程退出码；
   - 致命错误时停止 Flight、写日志和错误标记后退出。

2. `client.py`
   - 提供 `query(sql) -> pandas.DataFrame` 接口。

3. `data_fetcher.py`
   - 完整对接 Baostock；
   - 提供交易日历、股票列表、单股票历史和按交易日获取全体 A 股的增量接口；
   - 证券列表使用 `query_all_stock(day)`，每次更新只获取最新目标日一次；过滤指数后与目标日日 K 做代码及交易状态校验；发现新增股票时用 `query_history_k_data_plus()` 回补历史；
   - 日常更新必须使用 `query_daily_history_k_AStock(date)`，并在返回后筛选主板和创业板；
   - 日线固定获取不复权价格、`preclose` 和 `tradestatus`。

4. `db_manager.py`
   - 负责表结构、事务、staging、去重、完整性校验、meta 和内存重载；
   - 负责 `qfq_factor` 的首次计算、增量调整、幂等校验和指定股票全量重建；
   - 负责维护 `adjustment_events`。

5. `logging_config.py`
   - 配置控制台、滚动主日志、滚动错误日志和致命错误标记。

6. `config.yaml`
   - 提供可直接修改的配置示例。

7. `requirements.txt`
   - 至少包含 `duckdb`、支持 `query_daily_history_k_AStock()` 的 `baostock` 版本、`pandas`、`pyarrow`、`apscheduler`、`pyyaml`。

8. `deploy/windows-autostart.ps1` 和 `deploy/qstockdataserver.service`
   - 分别提供 Windows 任务计划程序和 Linux systemd 的安装、启用、查看日志、停止和卸载命令；
   - 所有路径使用可配置的绝对路径。

9. `tests/`
   - 覆盖错误日期、错误代码、重复行、字段缺失、错误复权口径、停牌、集合不一致、因子冲突、事务回滚、快照失败、致命标记和退出码；
   - 使用模拟 Baostock 响应验证任何异常行情都不会进入正式表。

10. `README.md`
   - 安装依赖；
   - 初始化和首次导入；
   - 启动 server；
   - 策略程序调用 client；
   - 配置字段说明；
   - 手动触发补数和常见错误处理；
   - Windows/Linux 开机自启动与管理命令；
   - 错误日志位置、退出码、`doctor` 和致命错误恢复流程。
