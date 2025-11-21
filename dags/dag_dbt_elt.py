# dags/dag_dbt_elt.py
from __future__ import annotations
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.hooks.base import BaseHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import os

# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_catfish"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_TARGET_ENV = "dev"

# Path to the virtual environment we will create
DBT_VENV_PATH = "/opt/airflow/dbt_venv"
DBT_BIN = f"{DBT_VENV_PATH}/bin/dbt"

@task(task_id="export_dbt_env_vars")
def export_dbt_env_vars():
    """Extracts Snowflake credentials to pass to dbt."""
    conn = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    extra = conn.extra_dejson or {}
    return {
        'DBT_SNOWFLAKE_ACCOUNT': extra.get('account'),
        'DBT_SNOWFLAKE_USER': conn.login,
        'DBT_SNOWFLAKE_PASSWORD': conn.password,
        'DBT_SNOWFLAKE_ROLE': extra.get('role', 'SYSADMIN'),
        'DBT_SNOWFLAKE_WAREHOUSE': extra.get('warehouse'),
        'DBT_SNOWFLAKE_DATABASE': extra.get('database'),
    }

with DAG(
    dag_id="dbt_elt_analytics",
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["elt", "dbt", "analytics"],
) as dag:
    
    dbt_vars = export_dbt_env_vars()

    # 1. NEW TASK: Install dbt into a secure virtual environment
    # This runs once per DAG run but is fast after the first time.
    install_dbt = BashOperator(
        task_id="install_dbt_venv",
        bash_command=f"""
        if [ ! -d "{DBT_VENV_PATH}" ]; then
            echo "Creating Virtual Environment..."
            python -m venv {DBT_VENV_PATH}
        fi
        
        echo "Installing dbt-snowflake..."
        # We install into the venv, isolated from Airflow
        {DBT_VENV_PATH}/bin/pip install --no-cache-dir dbt-snowflake==1.7.2 dbt-core==1.7.13
        """,
    )

    # 2. Run dbt using the venv executable
    dbt_run = BashOperator(
        task_id="dbt_run_analytics",
        # ADDED: --profiles-dir {DBT_PROJECT_DIR}
        bash_command=f"{DBT_BIN} run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {DBT_TARGET_ENV}",
        env=dbt_vars,
    )

    # 3. Test dbt using the venv executable
    dbt_test = BashOperator(
        task_id="dbt_test_models",
        # ADDED: --profiles-dir {DBT_PROJECT_DIR}
        bash_command=f"{DBT_BIN} test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROJECT_DIR} --target {DBT_TARGET_ENV}",
        env=dbt_vars,
    )
    
    trigger_ml = TriggerDagRunOperator(
        task_id="trigger_ml_forecast_dag",
        trigger_dag_id="ml_forecast_tf",
        execution_date="{{ ds }}",
        reset_dag_run=True,  # <--- ADD THIS LINE
    )
    
    # Wiring
    dbt_vars >> install_dbt >> dbt_run >> dbt_test >> trigger_ml
