#!/usr/bin/env python3
"""
Robust ETL: loads the six legacy CSV files into the DS‑schema of PostgreSQL and
logs every step.  Handles dirty flag columns in **md_ledger_account_s.csv** so
there are no integer‑conversion errors.
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
import psycopg2
import yaml
from dateutil import parser as dtp
from psycopg2 import sql

# ---------------------------------------------------------------------------
# CSV → (table, PK) mapping
# ---------------------------------------------------------------------------
FILE_MAP: Dict[str, Tuple[str, List[str] | None]] = {
    "ft_balance_f.csv":        ("ds.ft_balance_f",        ["on_date", "account_rk"]),
    "ft_posting_f.csv":        ("ds.ft_posting_f",        None),   # delete‑load
    "md_account_d.csv":        ("ds.md_account_d",        ["data_actual_date", "account_rk"]),
    "md_currency_d.csv":       ("ds.md_currency_d",       ["currency_rk", "data_actual_date"]),
    "md_exchange_rate_d.csv":  ("ds.md_exchange_rate_d",  ["data_actual_date", "currency_rk"]),
    "md_ledger_account_s.csv": ("ds.md_ledger_account_s", ["ledger_account", "start_date"]),
}

DATE_COLS: Dict[str, List[str]] = {
    "ft_balance_f.csv":        ["on_date"],
    "ft_posting_f.csv":        ["oper_date"],
    "md_account_d.csv":        ["data_actual_date", "data_actual_end_date"],
    "md_currency_d.csv":       ["data_actual_date", "data_actual_end_date"],
    "md_exchange_rate_d.csv":  ["data_actual_date", "data_actual_end_date"],
    "md_ledger_account_s.csv": ["start_date", "end_date"],
}

# ---------------------------------------------------------------------------
# Generic helpers: CLI, YAML, DB, logging
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load legacy CSVs into DS schema")
    p.add_argument("--config", type=Path, default=Path("config.yaml"),
                   help="YAML with DB creds + paths")
    return p.parse_args()


def read_config(file: Path) -> Dict[str, Any]:
    return yaml.safe_load(file.read_text())


def get_conn(c: Dict[str, Any]):
    return psycopg2.connect(host=c["host"], port=c["port"], dbname=c["database"],
                            user=c["user"], password=c["password"])


def log_event(cur, job: str, status: str, *, run_id: int | None = None,
              rows: int | None = None, msg: str | None = None) -> int | None:
    if status == "START":
        cur.execute("INSERT INTO logs.etl_audit (job_name,status) VALUES (%s,%s) RETURNING run_id", (job, status))
        return cur.fetchone()[0]
    cur.execute("UPDATE logs.etl_audit SET status=%s, rows_processed=%s, finished_at=now(), message=%s WHERE run_id=%s",
                (status, rows, msg, run_id))
    return None

# ---------------------------------------------------------------------------
# CSV parsing helpers
# ---------------------------------------------------------------------------

def parse_date_safe(val):
    if pd.isna(val) or (isinstance(val, str) and not val.strip()):
        return None
    try:
        return dtp.parse(str(val), dayfirst=True, fuzzy=True).date()
    except Exception:
        return None


def read_csv_smart(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "cp1251", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, delimiter=";", encoding=enc, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path}")

# ---------------------------------------------------------------------------
# Core upsert routine (COPY → staging → INSERT/UPDATE)
# ---------------------------------------------------------------------------

def upsert(df: pd.DataFrame, table: str, pk: List[str] | None, conn):
    schema, tbl = table.split(".")
    main = sql.Identifier(schema, tbl)
    stg = sql.Identifier(f"stg_{tbl}")

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {})").format(stg, main))

        with tempfile.NamedTemporaryFile("w+", delete=False) as tmp:
            df.to_csv(tmp.name, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
            tmp_path = tmp.name
            cols_ident = sql.SQL(',').join(map(sql.Identifier, df.columns))
        
        with open(tmp_path) as f:
            cur.copy_expert(
                sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(stg, cols_ident), f
            )
        os.remove(tmp_path)  # Fixed: Removed erroneous (tmp_path) call

        if pk:
            cols = sql.SQL(',').join(map(sql.Identifier, df.columns))
            pk_cols = sql.SQL(',').join(map(sql.Identifier, pk))
            set_clause = sql.SQL(',').join(
                sql.SQL('{} = EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c))
                for c in df.columns if c not in pk
            )
            cur.execute(sql.SQL(
                "INSERT INTO {} ({}) SELECT {} FROM {} ON CONFLICT ({}) DO UPDATE SET {}"
            ).format(main, cols, cols, stg, pk_cols, set_clause))
        else:
            cur.execute(sql.SQL("INSERT INTO {} SELECT * FROM {}").format(main, stg))
    conn.commit()

# ---------------------------------------------------------------------------
# Load a single CSV file
# ---------------------------------------------------------------------------

def load(path: Path, conn) -> int:
    table, pk = FILE_MAP[path.name.lower()]
    df = read_csv_smart(path)

    # normalise column names
    df.columns = [c.strip().lower() for c in df.columns]

    # strip whitespace in string columns
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # parse dates
    for col in DATE_COLS.get(path.name.lower(), []):
        if col in df.columns:
            df[col] = df[col].apply(parse_date_safe)

    # deduplicate on PK
    if pk:
        df = df.drop_duplicates(subset=pk, keep="last")

    # ----- special clean‑up for md_ledger_account_s flag columns -----
    if path.name.lower() == "md_ledger_account_s.csv":
        flag_cols = [c for c in df.columns if c.startswith("is_")]
        for c in flag_cols:
            # keep only digits, anything else → 0, then int
            df[c] = df[c].str.extract(r"(\d+)").fillna("0").astype(int)

    # delete‑load for postings
    if table.endswith("ft_posting_f"):
        with conn.cursor() as cur:
            cur.execute("DELETE FROM ds.ft_posting_f")
            conn.commit()

    upsert(df, table, pk, conn)
    return len(df)

# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> None:
    args = parse_args()
    cfg = read_config(args.config)

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s: %(message)s")

    job = cfg["paths"].get("job_name", "csv_load")
    data_dir = Path(cfg["paths"].get("data_dir", "./data"))
    conn = get_conn(cfg["db"])

    for csv_file in sorted(data_dir.glob("*.csv")):
        with conn.cursor() as cur:
            run_id = log_event(cur, job, "START")
            conn.commit()

        time.sleep(5)

        try:
            rows = load(csv_file, conn)
            with conn.cursor() as cur:
                log_event(cur, job, "END", run_id=run_id, rows=rows)
                conn.commit()
            logging.info(f"{csv_file.name}: OK – {rows} rows")
        except Exception as exc:
            conn.rollback()
            with conn.cursor() as cur:
                log_event(cur, job, "ERROR", run_id=run_id, msg=str(exc))
                conn.commit()
            logging.exception("%s: FAIL", csv_file.name)

    conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit()

