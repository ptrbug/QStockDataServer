"""Interactive SQL client for QStockDataServer."""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

from qstockdataserver.client import StockDataClient, StockDataClientError
from qstockdataserver.config import load_config
from qstockdataserver.exceptions import ConfigurationError


QUERY_OBJECTS = (
    "daily_qfq",
    "zb_daily_qfq",
    "cyb_daily_qfq",
    "stock_list",
    "zb_stock_list",
    "cyb_stock_list",
)
PRINT_ROW_LIMIT = 100


def _print_status(host: str, port: int, status: dict[str, object]) -> None:
    print(f"Connected to QStockDataServer {host}:{port}")
    print("Service status:")
    print(json.dumps(status, ensure_ascii=False, indent=2, default=str))


def _print_query_objects(client: StockDataClient) -> None:
    quoted_names = ", ".join(f"'{name}'" for name in QUERY_OBJECTS)
    schema = client.query(
        f"""
        SELECT table_name, column_name, data_type, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name IN ({quoted_names})
        ORDER BY table_name, ordinal_position
        """
    )
    found = set(schema["table_name"]) if not schema.empty else set()

    print("\n支持的查询对象及字段结构：")
    for name in QUERY_OBJECTS:
        print(f"\n{name}")
        columns = schema.loc[
            schema["table_name"].eq(name), ["column_name", "data_type"]
        ]
        if columns.empty:
            print("  当前服务中不存在")
            continue
        fields = ", ".join(
            f"{row.column_name} {row.data_type}"
            for row in columns.itertuples(index=False)
        )
        print(f"  字段：{fields}")

    missing = set(QUERY_OBJECTS) - found
    if missing:
        print(f"\n注意：当前服务缺少查询对象：{sorted(missing)}")


def _limited_sql(sql: str) -> str:
    statement = sql.strip().removesuffix(";").strip()
    return f"""
        WITH clientcli_source AS (
            {statement}
        ), clientcli_numbered AS (
            SELECT clientcli_source.*,
                   row_number() OVER () AS __clientcli_row_number,
                   count(*) OVER () AS __clientcli_total_rows
            FROM clientcli_source
        )
        SELECT * EXCLUDE (__clientcli_row_number)
        FROM clientcli_numbered
        WHERE __clientcli_row_number > __clientcli_total_rows - {PRINT_ROW_LIMIT}
        ORDER BY __clientcli_row_number
    """


def _execute_and_print(client: StockDataClient, sql: str) -> None:
    started = time.perf_counter()
    frame = client.query(_limited_sql(sql))
    elapsed = time.perf_counter() - started

    total_rows = 0 if frame.empty else int(frame.iloc[0, -1])
    visible = frame.iloc[:, :-1]
    if visible.empty:
        print("查询成功，结果为空。")
    else:
        print(visible.to_string(index=False))
    if total_rows > PRINT_ROW_LIMIT:
        print(
            f"\n查询结果共 {total_rows} 行，"
            f"仅显示最后 {PRINT_ROW_LIMIT} 行。"
        )
    else:
        print(f"\n查询结果共 {total_rows} 行。")
    print(f"查询耗时：{elapsed:.4f} 秒")


def _interactive_loop(client: StockDataClient) -> None:
    print(
        "\n请输入单条 SELECT SQL，按回车执行；输入 exit 或 quit 退出。"
        "需要确定最后 100 行时，请在 SQL 中使用 ORDER BY。"
    )
    while True:
        try:
            sql = input("SQL> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return
        if not sql:
            continue
        if sql.lower() in {"exit", "quit"}:
            return
        try:
            _execute_and_print(client, sql)
        except StockDataClientError as exc:
            print(f"查询失败：{exc}", file=sys.stderr)
        except Exception as exc:
            print(f"无法显示查询结果：{exc}", file=sys.stderr)


def main() -> int:
    try:
        config_path = Path(__file__).resolve().with_name("config.yaml")
        config = load_config(config_path)
        with StockDataClient(config.flight_host, config.flight_port) as client:
            status = client.status()
            _print_status(config.flight_host, config.flight_port, status)
            _print_query_objects(client)
            _interactive_loop(client)
        return 0
    except StockDataClientError as exc:
        print(f"连接 QStockDataServer 失败：{exc}", file=sys.stderr)
        return 1
    except ConfigurationError as exc:
        print(f"读取配置失败：{exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print()
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
