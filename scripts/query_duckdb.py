"""
query_duckdb.py
───────────────
Query any Parquet layer using DuckDB — no Spark required.
Works instantly on Windows, Mac, Linux. Zero setup.

Run:
    python scripts/query_duckdb.py
    python scripts/query_duckdb.py --layer gold --table daily_revenue
    python scripts/query_duckdb.py --sql "SELECT * FROM gold.customer_ltv LIMIT 10"
"""
from __future__ import annotations
import argparse
import sys
from pathlib import Path

try:
    import duckdb
except ImportError:
    print("Install duckdb: pip install duckdb")
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[1]


def get_con():
    con = duckdb.connect()
    # Register all Parquet layers as virtual schemas
    for layer in ["bronze", "silver", "gold", "features"]:
        layer_path = ROOT / "data" / layer
        if layer_path.exists():
            for table_dir in layer_path.iterdir():
                if table_dir.is_dir():
                    parquet_files = list(table_dir.glob("**/*.parquet"))
                    if parquet_files:
                        paths = [str(p) for p in parquet_files]
                        view_name = f"{layer}__{table_dir.name}"
                        con.execute(
                            f"CREATE OR REPLACE VIEW {view_name} AS "
                            f"SELECT * FROM read_parquet({paths!r})"
                        )
    return con


def list_tables(con) -> None:
    print("\n Available tables (DuckDB views):\n")
    views = con.execute("SHOW TABLES").fetchall()
    for (v,) in sorted(views):
        layer, name = v.split("__", 1)
        count = con.execute(f"SELECT COUNT(*) FROM {v}").fetchone()[0]
        print(f"  {layer:<12} {name:<30} {count:>10,} rows")
    print()


def query_table(con, layer: str, table: str, limit: int = 10) -> None:
    view = f"{layer}__{table}"
    try:
        df = con.execute(f"SELECT * FROM {view} LIMIT {limit}").df()
        print(f"\n[{view}] — first {limit} rows:\n")
        print(df.to_string(max_cols=10, max_colwidth=20))
        print(f"\nShape: {con.execute(f'SELECT COUNT(*) FROM {view}').fetchone()[0]:,} rows × {len(df.columns)} cols")
    except Exception as e:
        print(f"Error querying {view}: {e}")


def run_sql(con, sql: str) -> None:
    try:
        df = con.execute(sql).df()
        print(df.to_string())
    except Exception as e:
        print(f"SQL error: {e}")


def interactive(con) -> None:
    print("\n NexCart DuckDB Query Shell")
    print(" Type SQL queries or 'list' to see tables, 'exit' to quit.\n")
    while True:
        try:
            query = input("duckdb> ").strip()
            if not query:
                continue
            if query.lower() in ("exit", "quit", "q"):
                break
            if query.lower() == "list":
                list_tables(con)
            else:
                run_sql(con, query)
        except (KeyboardInterrupt, EOFError):
            break


def main():
    parser = argparse.ArgumentParser(description="Query NexCart Parquet layers with DuckDB")
    parser.add_argument("--layer", type=str, help="Layer: bronze|silver|gold|features")
    parser.add_argument("--table", type=str, help="Table name")
    parser.add_argument("--sql",   type=str, help="Run a SQL query directly")
    parser.add_argument("--list",  action="store_true", help="List all available tables")
    parser.add_argument("--limit", type=int, default=10, help="Row limit for table preview")
    args = parser.parse_args()

    con = get_con()

    if args.list or (not args.layer and not args.sql):
        list_tables(con)
        interactive(con)
    elif args.sql:
        run_sql(con, args.sql)
    elif args.layer and args.table:
        query_table(con, args.layer, args.table, args.limit)
    else:
        list_tables(con)


if __name__ == "__main__":
    main()
