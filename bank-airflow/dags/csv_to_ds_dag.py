from __future__ import annotations
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from pathlib import Path
import subprocess, logging, os

# Параметры
CSV_DIR = "/opt/airflow/data"          # монтируемая папка с CSV
ETL_SCRIPT = "/opt/airflow/scripts/etl.py"
CONFIG = "/opt/airflow/config.yaml"

default_args = {
    "owner": "de_team",
    "retries": 1,
    "retry_delay": 300,   # 5 мин
}

with DAG(
    dag_id="csv_to_ds",
    start_date=days_ago(1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    tags=["bank", "etl"],
) as dag:

    def check_csv():
        files = list(Path(CSV_DIR).glob("*.csv"))
        if not files:
            raise FileNotFoundError("No CSV files in data dir")
        logging.info("Found %d files: %s", len(files), [f.name for f in files])

    def run_etl():
        cmd = ["python", ETL_SCRIPT, "--config", CONFIG]
        result = subprocess.run(cmd, capture_output=True, text=True)
        logging.info(result.stdout)
        if result.returncode != 0:
            logging.error(result.stderr)
            raise RuntimeError("ETL failed")

    check_task = PythonOperator(
        task_id="check_csv_present",
        python_callable=check_csv,
    )

    etl_task = PythonOperator(
        task_id="run_csv_loader",
        python_callable=run_etl,
    )

    check_task >> etl_task

