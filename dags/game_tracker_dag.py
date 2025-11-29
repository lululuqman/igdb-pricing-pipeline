from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
import os
import sys

# Crucial Step: Add the 'src/' directory to the Python path 
# so Airflow can find your custom modules (extract, transform, load)
AIRFLOW_HOME = os.getenv('AIRFLOW_HOME', '/opt/airflow')
sys.path.insert(0, os.path.join(AIRFLOW_HOME, 'dags', 'src'))

# 1. Import your custom functions from the 'src' folder
from src.extract import run_extraction as extract_game_data_logic # Assuming you rename extract_games to run_extraction
from src.transform import transform_data
from src.load import run_load_pipeline as load_game_data_logic

# Note: You will need to wrap the logic in src/extract.py and src/load.py
# into a single, main function (e.g., run_extraction(), run_load_pipeline())
# so that the PythonOperator can call them easily.

with DAG(
    dag_id="igdb_pricing_etl_pipeline",
    start_date=pendulum.datetime(2025, 11, 28, tz="UTC"),
    schedule="0 12 * * *", # Daily run at 12:00 PM UTC (or 8 PM Malaysia time)
    catchup=False,
    tags=["portfolio", "etl", "pricing"],
    doc_md=__doc__,
) as dag:
    # --- Task 1: Extraction ---
    extract_task = PythonOperator(
        task_id="extract_game_metadata_and_rate",
        python_callable=extract_game_data_logic,
        # op_kwargs can be used to pass parameters if needed
    )

    # --- Task 2: Transformation ---
    transform_task = PythonOperator(
        task_id="transform_and_enrich_data",
        python_callable=transform_data,
    )

    # --- Task 3: Loading (with UPSERT) ---
    load_task = PythonOperator(
        task_id="load_to_postgres_upsert",
        python_callable=load_game_data_logic,
    )

    # --- Define Dependencies (The Flow) ---
    # The ETL process must run sequentially
    extract_task >> transform_task >> load_task