#!/usr/bin/env python3
"""
Robust ETL: loads the six legacy CSV files into the DS‑schema of PostgreSQL and
logs every step.  Extra statistics recorded per file:
  • rows_processed  – как и раньше
  • rows_deduped    – сколько строк отброшено как дубликаты PK
  • date_parse_err  – сколько значений дат не удалось распарсить
Эти цифры пишутся в колонку `message` таблицы LOGS.etl_audit в формате
"deduped=…, date_err=…".
"""
from __future__ import annotations

import argparse, csv, logging, os, sys, tempfile, time
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd, psycopg2, yaml
from dateutil import parser as dtp
from psycopg2 import sql

FILE_MAP: Dict[str, Tuple[str, List[str] | None]] = {
    "ft_balance_f.csv":        ("ds.ft_balance_f",        ["on_date", "account_rk"]),
    "ft_posting_f.csv":        ("ds.ft_posting_f",        None),  # delete‑load
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

# -------------------------- helpers ----------------------------------------

def parse_args():
    p = argparse.ArgumentParser("ETL loader")
    p.add_argument("--config", type=Path, default=Path("config.yaml"))
    return p.parse_args()

def read_config(p: Path):
    return yaml.safe_load(p.read_text())

def get_conn(c: Dict[str, Any]):
    return psycopg2.connect(host=c['host'], port=c['port'], dbname=c['database'],
                            user=c['user'], password=c['password'])

def log_event(cur, job: str, status: str, *, run_id: int | None = None,
              rows: int | None = None, msg: str | None = None):
    if status == 'START':
        cur.execute("INSERT INTO logs.etl_audit(job_name,status) VALUES(%s,%s) RETURNING run_id", (job, status))
        return cur.fetchone()[0]
    cur.execute("UPDATE logs.etl_audit SET status=%s, rows_processed=%s, finished_at=now(), message=%s WHERE run_id=%s",
                (status, rows, msg, run_id))

# -------------------------- CSV utils --------------------------------------

def parse_date_safe(val):
    if pd.isna(val) or (isinstance(val, str) and not val.strip()):
        return None, False
    try:
        return dtp.parse(str(val), dayfirst=True, fuzzy=True).date(), False
    except Exception:
        return None, True

def read_csv_smart(path: Path):
    for enc in ("utf-8", "cp1251", "cp1252", "latin-1"):
        try:
            return pd.read_csv(path, delimiter=';', encoding=enc, dtype=str, keep_default_na=False)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode {path}")

# -------------------------- DB upsert --------------------------------------

def upsert(df: pd.DataFrame, table: str, pk: List[str] | None, conn):
    schema, tbl = table.split('.')
    main = sql.Identifier(schema, tbl)
    stg = sql.Identifier(f'stg_{tbl}')

    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {})").format(stg, main))
        with tempfile.NamedTemporaryFile('w+', delete=False) as tmp:
            df.to_csv(tmp.name, index=False, header=False, quoting=csv.QUOTE_MINIMAL)
            tmp_path = tmp.name
            cols_ident = sql.SQL(',').join(map(sql.Identifier, df.columns))
        with open(tmp_path) as f:
            cur.copy_expert(sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(stg, cols_ident), f)
        os.remove(tmp_path)

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

# -------------------------- load single CSV --------------------------------

def load(path: Path, conn):
    table, pk = FILE_MAP[path.name.lower()]
    df = read_csv_smart(path)
    df.columns = [c.strip().lower() for c in df.columns]

    str_cols = df.select_dtypes(include='object').columns
    df[str_cols] = df[str_cols].apply(lambda c: c.str.strip())

    # --- parse dates & count errors ---
    date_errs = 0
    for col in DATE_COLS.get(path.name.lower(), []):
        if col in df.columns:
            parsed, errs = zip(*df[col].apply(parse_date_safe))
            df[col] = parsed
            date_errs += sum(errs)

    orig = len(df)
    if pk:
        df = df.drop_duplicates(subset=pk, keep='last')
    deduped = orig - len(df)

    if path.name.lower() == 'md_ledger_account_s.csv':
        for c in [col for col in df.columns if col.startswith('is_')]:
            df[c] = pd.to_numeric(df[c].str.extract(r'(\d+)')[0], errors='coerce').fillna(0).astype(int)

    if table.endswith('ft_posting_f'):
        with conn.cursor() as cur:
            cur.execute('DELETE FROM ds.ft_posting_f'); conn.commit()

    upsert(df, table, pk, conn)
    return len(df), deduped, date_errs

# -------------------------- main -------------------------------------------

def main():
    cfg = read_config(parse_args().config)
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

    job = cfg['paths'].get('job_name', 'csv_load')
    data = Path(cfg['paths'].get('data_dir', './data'))
    conn = get_conn(cfg['db'])

    for csv in sorted(data.glob('*.csv')):
        with conn.cursor() as cur:
            rid = log_event(cur, job, 'START'); conn.commit()
        time.sleep(5)
        try:
            rows, deduped, d_err = load(csv, conn)
            msg = f'deduped={deduped}, date_err={d_err}'
            with conn.cursor() as cur:
                log_event(cur, job, 'END', run_id=rid, rows=rows, msg=msg); conn.commit()
            logging.info('%s: OK – %s rows (deduped=%s date_err=%s)', csv.name, rows, deduped, d_err)
        except Exception as e:
            conn.rollback()
            with conn.cursor() as cur:
                log_event(cur, job, 'ERROR', run_id=rid, msg=str(e)); conn.commit()
            logging.exception('%s: FAIL', csv.name)
    conn.close()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(0)

