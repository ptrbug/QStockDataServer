# QStockDataServer

QStockDataServer 是一个面向 A 股主板、创业板、科创板的可配置本地日线行情服务。它只在磁盘保存 BaoStock 返回的**不复权**行情，利用 `preclose` 在本地确定性计算前复权因子，并通过 DuckDB 内存快照和 Arrow Flight 向策略程序提供只读 SQL 查询。默认启用主板和创业板。

项目采用 fail-closed 策略：日期、字段、代码集合、交易状态、价格、复权口径或连续性只要有一项无法验证，本次事务就会回滚；数据完整性错误会留下致命标记并停止服务，未验证的数据不会进入正式表。

> “绝对可靠”不能由单一上游数据源从业务事实层面证明。本项目保证的是：BaoStock 返回值必须通过本地结构、一致性和连续性校验，写入必须是原子事务；任何无法确认的数据都会被拒绝，不会静默接受。

## 源码结构

```text
QStockDataServer/
├─ qstockdataserver/      # 配置、数据获取、存储、Flight、客户端和服务逻辑
├─ tests/                 # 自动化测试
├─ deploy/                # Windows/Linux 部署脚本
├─ server.py              # 唯一命令行启动入口
├─ config.yaml            # 默认运行配置
└─ README.md
```

源码运行统一使用 `python server.py <command>`，不需要安装本地包，也不提供 `python -m qstockdataserver` 入口。

## 数据流程

- 首次导入：先用 `query_all_stock(day)` 获取目标证券集合，再逐只调用 `query_history_k_data_plus(..., frequency="d", adjustflag="3")`，默认从 2018-01-01 开始。
- 增量更新：只对最新目标交易日调用一次 `query_all_stock(day)` 更新证券列表；然后按缺失交易日逐日调用一次 `query_daily_history_k_AStock(date)` 获取全市场日 K。该接口包含主板、创业板和科创板，程序再按 `boards` 过滤，因此启用更多板块不会增加每日全市场接口的调用次数。即使一年未更新，也只需调用一次证券列表接口和约 240 次全市场日线接口。
- 新增股票：将最新证券列表与本地列表比较，每只新增股票调用一次 `query_history_k_data_plus(..., frequency="d", adjustflag="3")` 回补完整历史。历史回补、最新证券列表和目标日日 K 在同一事务提交；目标日逐字段不一致会整体回滚。
- 正式存储：磁盘表 `daily` 保存所有已启用板块的不复权 OHLC、`preclose`、成交量、成交额及 `qfq_factor`；`stock_list` 保存统一证券列表。
- 查询：内存表 `daily_qfq` 保存所有已启用板块的前复权行情；默认同时提供 `zb_daily_qfq`、`cyb_daily_qfq`、`zb_stock_list`、`cyb_stock_list`。启用 `kcb` 后还会提供 `kcb_daily_qfq` 和 `kcb_stock_list`。行情查询对象不包含 `preclose`，`pct_chg` 在构建快照时按 `(close/preclose-1)*100` 计算。

BaoStock 的接口实际会把部分停牌证券的 `volume`、`amount` 返回为空，个别历史停牌日还会在 `amount=0` 时残留上一交易日的非零 `volume`。程序仅在 `tradestatus=0`、OHLC 相等且成交额为 0 时把停牌成交量规范化为 0，随后仍强制检查停牌 OHLC 相等且量额为 0；正常交易证券出现空量额，或停牌日存在非零成交额/不同价格，都会立即中止。

前复权因子以最新一日为 1，向前递推：

```text
F[i-1] = F[i] * preclose[i] / close[i-1]
```

新增除权除息日时，只调整该股票的历史因子，不重写原始行情。调整事件同时写入 `adjustment_events`，并验证相邻日的复权收盘价与复权 `preclose` 连续。

## 数据库与查询对象

磁盘 DuckDB 使用统一表，不按板块拆分：

| 对象 | 用途 |
| ---- | ---- |
| `daily` | 所有已采集板块的不复权日线和前复权因子 |
| `stock_list` | 所有已采集板块的证券列表 |
| `adjustment_events` | 除权除息事件 |
| `initial_import_progress` | 首次导入断点 |
| `meta` | 最近更新日期等运行状态 |

Arrow Flight 只开放内存快照中的查询对象。默认 `boards: [zb, cyb]` 时共有以下 6 个：

```text
daily_qfq
zb_daily_qfq
cyb_daily_qfq
stock_list
zb_stock_list
cyb_stock_list
```

行情对象的列为 `symbol`、`date`、`open`、`high`、`low`、`close`、`pct_chg`、`volume`、`amount`、`trade_status`。其中 OHLC 已前复权，`pct_chg` 由磁盘行情的 `close/preclose` 计算并四舍五入到两位小数；不对外提供 `preclose` 和 `qfq_factor`。

将配置改为 `boards: [zb, cyb, kcb]` 后，统一对象 `daily_qfq` 和 `stock_list` 会包含科创板，并增加 `kcb_daily_qfq`、`kcb_stock_list` 两个分板块查询对象。每日仍只调用一次 `query_daily_history_k_AStock(date)`。

## 安装

建议 Python 3.11 或 3.12。

Windows PowerShell：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

Linux：

```bash
python3 -m venv .venv
./.venv/bin/python -m pip install -r requirements.txt
```

## 配置

默认配置位于 `config.yaml`。相对路径都按配置文件所在目录解析，因此开机启动时不会把数据库或日志写入错误目录。

默认获取主板和创业板：

```yaml
boards: [zb, cyb]
```

需要同时获取科创板时改为：

```yaml
boards: [zb, cyb, kcb]
```

| 配置项                 | 默认值                            | 说明                                           |
| ---------------------- | --------------------------------- | ---------------------------------------------- |
| `database_path`        | `data/stock_daily.duckdb`         | 磁盘 DuckDB                                    |
| `boards`               | `[zb, cyb]`                       | 启用板块；可选 `zb`、`cyb`、`kcb`              |
| `start_date`           | `2018-01-01`                      | 首次导入起始日                                 |
| `update_time`          | `18:30`                           | 每日调度时间                                   |
| `timezone`             | `Asia/Shanghai`                   | 调度时区                                       |
| `retry_delays_seconds` | `[3, 30, 120, 300]`               | 各次失败后的退避秒数；列表耗尽后复用最后一个值 |
| `max_retries`          | `12`                              | 每次 API 调用最大尝试次数                      |
| `session_max_minutes`  | `30`                              | BaoStock 会话最长连续使用时间                  |
| `factor_epsilon`       | `1e-10`                           | 复权因子比较误差                               |
| `flight_host`          | `127.0.0.1`                       | 只允许回环地址；服务没有远程认证               |
| `flight_port`          | `8815`                            | Arrow Flight TCP 端口                          |
| `query_max_rows`       | `50000000`                        | 单次查询最大返回行数                           |
| `runtime_dir`          | `runtime`                         | 锁文件和致命标记目录                           |
| `log_path`             | `logs/qstockdataserver.log`       | 滚动主日志                                     |
| `error_log_path`       | `logs/qstockdataserver.error.log` | ERROR/CRITICAL 日志                            |

临时网络或 BaoStock API 错误会立即废弃旧会话，依次等待 3 秒、30 秒、2 分钟，之后每次等待 5 分钟；只在下一次请求前重新登录。即使没有错误，会话使用超过 30 分钟后，也会在两次请求之间主动轮换。结构、日期或行情内容错误不会自动重试。

## 首次启动与日常运行

首次启动会执行全量历史导入，耗时取决于股票数和 BaoStock 响应速度。首次目标日一旦写入 `initial_import_target_date` 就固定不变；即使导入跨越到下一个交易日，续传仍使用原目标日和原证券集合。全量完成后，程序会在开放查询服务前自动用增量接口补到最新完整交易日。

每只股票成功后都会在同一事务中提交日线、复权事件和 `initial_import_progress=completed`。如果下载或计算中断，该股票尚未写入；如果数据库事务中断，DuckDB 会整体回滚；只有事务提交成功后重启才会跳过该股票。因此不会把“只下载了一半”的单股历史当作完成，最多重新下载当时正在处理的一只。

Windows：

```powershell
.\.venv\Scripts\python.exe server.py serve
```

Linux：

```bash
./.venv/bin/python server.py serve
```

启动顺序是：初始化 schema、首次导入或补齐缺失交易日、构建内存快照、启动 Flight 和每日调度。更新期间旧快照可以完成已有查询；只有磁盘事务和新快照均成功后才原子切换。

## 客户端查询

默认 `boards: [zb, cyb]` 支持查询以下对象：

| 查询对象 | 内容 |
| -------- | ---- |
| `daily_qfq` | 主板和创业板全部前复权日线 |
| `zb_daily_qfq` | 主板前复权日线 |
| `cyb_daily_qfq` | 创业板前复权日线 |
| `stock_list` | 主板和创业板全部证券列表 |
| `zb_stock_list` | 主板证券列表 |
| `cyb_stock_list` | 创业板证券列表 |

配置包含 `kcb` 时，`daily_qfq`、`stock_list` 会自动包含科创板，同时增加 `kcb_daily_qfq` 和 `kcb_stock_list`。

例如查询贵州茅台的全部前复权日线：

```python
from qstockdataserver.client import StockDataClient

with StockDataClient() as client:
    data = client.query("""
        SELECT date, open, high, low, close, pct_chg
        FROM daily_qfq
        WHERE symbol = 'sh.600519'
        ORDER BY date
    """)

    print(data)
    print(client.status())
```

服务只接受单条 `SELECT`/`WITH ... SELECT`，拒绝写 SQL、多语句和 `read_csv` 等外部访问函数。返回大量数据时可调用 `query_arrow()` 避免立即转成 pandas。

手动触发补数：

```python
from qstockdataserver.client import StockDataClient

with StockDataClient() as client:
    accepted = client.trigger_update()
    print("已接受" if accepted else "已有更新任务在运行")
```

## 错误处理与恢复

日志位置：

- 主日志：`logs/qstockdataserver.log`
- 错误日志：`logs/qstockdataserver.error.log`
- 致命标记：`runtime/FATAL_ERROR.json`

退出码：

| 退出码 | 含义                       |
| ------ | -------------------------- |
| `2`    | 配置错误                   |
| `3`    | 网络/API 临时错误重试耗尽  |
| `4`    | 行情数据校验或一致性错误   |
| `5`    | DuckDB、快照或其他存储错误 |

退出码 4、5 会写入致命标记。只要标记存在，`serve` 就拒绝启动，防止任务调度器反复运行或继续提供无法确认的快照。恢复流程：

```powershell
# 1. 阅读致命标记和错误日志
Get-Content .\runtime\FATAL_ERROR.json
Get-Content .\logs\qstockdataserver.error.log -Tail 200

# 2. 对数据库执行只读完整性检查
.\.venv\Scripts\python.exe server.py doctor

# 3. 只有确认上游/磁盘问题已经处理后才清除标记
.\.venv\Scripts\python.exe server.py clear-fatal --confirm

# 4. 重新启动；首次导入中断时会续传
.\.venv\Scripts\python.exe server.py serve
```

Linux 使用 `./.venv/bin/python` 执行相同子命令。不要直接删除数据库或致命标记；`clear-fatal` 会先强制运行 `doctor`。

## Windows 打包与开机自启动

PyInstaller 只能为当前操作系统生成可执行文件，因此 Windows 版本需要在 Windows 上构建。目录版产物不会在每次启动时解压，也便于将配置、数据和日志保留在程序外部。

先安装开发依赖并构建：

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
powershell -NoProfile -ExecutionPolicy Bypass -File .\deploy\build-windows.ps1
```

构建结果位于 `dist\QStockDataServer`。把整个 `QStockDataServer` 文件夹复制到自己选择的安装目录，不要只复制 exe，因为 `_internal` 中包含运行依赖。

进入安装目录，确认 `config.yaml` 后，以管理员身份打开 PowerShell，执行一条命令安装并启动开机任务：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\install-autostart.ps1
```

安装目录中的 `config.yaml`、`data`、`logs`、`runtime` 都位于可执行文件外部，升级时应保留它们。任务的工作目录固定为安装目录，实际命令是 `QStockDataServer.exe serve`，所以会使用同目录下默认的 `config.yaml`。安装脚本会立即启动任务。常用命令：

```powershell
Get-ScheduledTask -TaskName "QStockDataServer"
Get-ScheduledTaskInfo -TaskName "QStockDataServer"
Stop-ScheduledTask -TaskName "QStockDataServer"
Start-ScheduledTask -TaskName "QStockDataServer"
Unregister-ScheduledTask -TaskName "QStockDataServer" -Confirm:$false
Get-Content .\logs\qstockdataserver.error.log -Wait
```

## Linux systemd 开机自启动

Linux 长期服务建议直接使用 `.venv + systemd`，不额外封装可执行文件，依赖更新和故障排查更清楚。模板默认项目路径为 `/opt/QStockDataServer`、用户为 `qstock`；如果安装位置不同，先编辑 `deploy/qstockdataserver.service` 中的绝对路径。

```bash
id -u qstock >/dev/null 2>&1 || sudo useradd --system \
  --home /opt/QStockDataServer --shell /usr/sbin/nologin qstock
sudo chown -R qstock:qstock /opt/QStockDataServer
sudo cp deploy/qstockdataserver.service /etc/systemd/system/qstockdataserver.service
sudo systemctl daemon-reload
sudo systemctl enable --now qstockdataserver.service
sudo systemctl status qstockdataserver.service
sudo journalctl -u qstockdataserver.service -f
```

常用命令：

```bash
sudo systemctl stop qstockdataserver.service
sudo systemctl start qstockdataserver.service
sudo systemctl restart qstockdataserver.service
sudo systemctl disable --now qstockdataserver.service
```

systemd 不会对退出码 2、4、5 自动重启；Windows 即使尝试重启，也会被致命标记挡住。

## 测试

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m compileall -q .
```

测试覆盖 BaoStock 错误响应、字段和日期错误、重复数据、停牌规则、最新股票集合不一致、新股历史回补、前复权因子、事务回滚、首次导入恢复、致命标记、只读 SQL 和真实 Arrow Flight 客户端/服务端通信。
