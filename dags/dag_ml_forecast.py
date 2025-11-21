# dags/dag_ml_forecast.py
# TaskFlow DAG2: Runs ML Forecasting after the ETL and ELT (dbt) steps are complete.
from datetime import datetime, timezone
from airflow.decorators import dag, task
from airflow.models import Variable
# FIX: Use the standard Snowflake Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 
import snowflake.connector
from airflow.exceptions import AirflowSkipException # Used for guards

SNOWFLAKE_CONN_ID = "snowflake_catfish"

# Schemas / tables
RAW, MODEL, ANALYTICS = "RAW", "MODEL", "ANALYTICS"
RAW_STOCK_PRICES = f"{RAW}.STOCK_PRICES"           
MODEL_FORECASTS  = f"{MODEL}.FORECASTS"            
ANALYTICS_FINAL  = f"{ANALYTICS}.FINAL_PRICES_FORECAST"



@dag(
    dag_id="ml_forecast_tf",
    # Change schedule to None so it is triggered by the dbt DAG (Lab 2 Requirement)
    schedule=None,
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["ml", "forecast", "snowflake"],
)
def ml_forecast_tf():

    @task
    def ensure_objects():

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        # Use context manager to handle connection and cursor
        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {MODEL}")
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {ANALYTICS}")
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {MODEL_FORECASTS} (
                      SYMBOL STRING NOT NULL,
                      TS DATE NOT NULL,
                      PREDICTED_CLOSE FLOAT NOT NULL,
                      MODEL_NAME STRING NOT NULL,
                      TRAINED_AT TIMESTAMP_NTZ NOT NULL,
                      HORIZON_D NUMBER(5,0) NOT NULL,
                      LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                      CONSTRAINT PK_FORECASTS PRIMARY KEY (SYMBOL, TS, MODEL_NAME)
                    )
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {ANALYTICS_FINAL} (
                      SYMBOL STRING NOT NULL,
                      TS DATE NOT NULL,
                      CLOSE FLOAT,
                      SOURCE STRING NOT NULL,
                      MODEL_NAME STRING,
                      LOAD_TS TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                      CONSTRAINT PK_FINAL PRIMARY KEY (SYMBOL, TS, SOURCE)
                    )
                """)
                conn.commit()
            except Exception as e:
               
                try: conn.rollback()
                except Exception: pass
                raise e


    @task
    def train_model_and_forecast():
        syms_raw   = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
        lookback   = int(Variable.get("lookback_days", "365"))
        horizon    = int(Variable.get("forecast_horizon_days", "14"))
        syms_json  = syms_raw.replace("'", "''") 

        # FIX: Initialize SnowflakeHook (Lab 1 Fix)
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                # Phase 1: setup (transactional)
                conn.autocommit(False)
                cur.execute(f"USE SCHEMA {MODEL}")

                # Create symbols temp table
                cur.execute(f"""
                    CREATE OR REPLACE TEMP TABLE SYMBOLS AS
                    SELECT value::string AS SYMBOL
                    FROM TABLE(FLATTEN(input => PARSE_JSON('{syms_json}')))
                """)

                # Training data view: Uses RAW.STOCK_PRICES.TRADE_DATE (DATE)
                cur.execute(f"""
                    CREATE OR REPLACE TEMP VIEW TRAINING_DATA AS
                    SELECT
                      TO_VARIANT(sp.SYMBOL)              AS SERIES,
                      sp.TRADE_DATE::TIMESTAMP_NTZ       AS TS,
                      sp.CLOSE                           AS CLOSE
                    FROM {RAW_STOCK_PRICES} sp
                    JOIN SYMBOLS s ON s.SYMBOL = sp.SYMBOL
                    WHERE sp.TRADE_DATE >= DATEADD('day', -{lookback}, CURRENT_DATE())
                      AND sp.CLOSE IS NOT NULL
                """)

                cur.execute("SELECT COUNT(*) FROM TRAINING_DATA")
                if cur.fetchone()[0] == 0:
                    conn.rollback()
                    raise AirflowSkipException("Training skipped: TRAINING_DATA is empty.")

                conn.commit() 

                # Phase 2: ML (autocommit=True is common for ML/Snowflake functions)
                conn.autocommit(True) 

                # Create/replace ML forecast object
                cur.execute("""
                    CREATE OR REPLACE SNOWFLAKE.ML.FORECAST PRICE_FORECASTER (
                      INPUT_DATA        => SYSTEM$QUERY_REFERENCE($$ SELECT SERIES, TS, CLOSE FROM TRAINING_DATA $$),
                      SERIES_COLNAME    => 'SERIES',
                      TIMESTAMP_COLNAME => 'TS',
                      TARGET_COLNAME    => 'CLOSE',
                      CONFIG_OBJECT     => PARSE_JSON('{"method":"fast","on_error":"skip"}')
                    )
                """)

                # Forecast and insert into temp table
                cur.execute(f"""
                    CREATE OR REPLACE TEMP TABLE TMP_FC AS
                    SELECT
                      SERIES::STRING              AS SYMBOL,
                      CAST(TS AS DATE)            AS TS,
                      FORECAST                    AS PREDICTED_CLOSE,
                      'SNOWFLAKE_ML'              AS MODEL_NAME,
                      CURRENT_TIMESTAMP()         AS TRAINED_AT,
                      {horizon}::NUMBER           AS HORIZON_D
                    FROM TABLE(PRICE_FORECASTER!FORECAST(FORECASTING_PERIODS => {horizon}))
                """)

                cur.execute("SELECT COUNT(*) FROM TMP_FC")
                if cur.fetchone()[0] == 0:
                    raise AirflowSkipException("Forecast step produced 0 rows.")

                # Phase 3: upsert into MODEL.FORECASTS (transactional)
                conn.autocommit(False)
                cur.execute(f"""
                    MERGE INTO {MODEL_FORECASTS} t
                    USING TMP_FC s
                    ON  t.SYMBOL = s.SYMBOL
                    AND t.TS     = s.TS
                    AND t.MODEL_NAME = s.MODEL_NAME
                    WHEN MATCHED THEN UPDATE SET
                      PREDICTED_CLOSE = s.PREDICTED_CLOSE,
                      TRAINED_AT      = s.TRAINED_AT,
                      HORIZON_D       = s.HORIZON_D,
                      LOAD_TS         = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT
                      (SYMBOL, TS, PREDICTED_CLOSE, MODEL_NAME, TRAINED_AT, HORIZON_D)
                      VALUES (s.SYMBOL, s.TS, s.PREDICTED_CLOSE, s.MODEL_NAME, s.TRAINED_AT, s.HORIZON_D)
                """)
                conn.commit()
            except Exception as e:

                try: conn.rollback()
                except Exception: pass
                raise e


    @task
    def build_final_union():
        syms_raw  = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
        syms_json = syms_raw.replace("'", "''")

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        
        with hook.get_conn() as conn, conn.cursor() as cur:
            try:
                conn.autocommit(False)
                cur.execute(f"USE SCHEMA {ANALYTICS}")

                # Recreate symbols temp table
                cur.execute(f"""
                    CREATE OR REPLACE TEMP TABLE SYMBOLS AS
                    SELECT value::string AS SYMBOL
                    FROM TABLE(FLATTEN(input => PARSE_JSON('{syms_json}')))
                """)

                # Rebuild final union (actuals + forecasts)
                cur.execute(f"TRUNCATE TABLE {ANALYTICS_FINAL}")

                # ACTUALS
                cur.execute(f"""
                    INSERT INTO {ANALYTICS_FINAL} (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
                    SELECT sp.SYMBOL,
                           sp.TRADE_DATE AS TS,
                           sp.CLOSE,
                           'ACTUAL'      AS SOURCE,
                           NULL          AS MODEL_NAME
                    FROM {RAW_STOCK_PRICES} sp
                    JOIN SYMBOLS s ON s.SYMBOL = sp.SYMBOL
                """)

                # FORECASTS
                cur.execute(f"""
                    INSERT INTO {ANALYTICS_FINAL} (SYMBOL, TS, CLOSE, SOURCE, MODEL_NAME)
                    SELECT f.SYMBOL,
                           f.TS,
                           f.PREDICTED_CLOSE AS CLOSE,
                           'FORECAST'        AS SOURCE,
                           f.MODEL_NAME
                    FROM {MODEL_FORECASTS} f
                    JOIN SYMBOLS s ON s.SYMBOL = f.SYMBOL
                """)

                conn.commit()
            except Exception as e:
                try: conn.rollback()
                except Exception: pass
                raise e

    # TaskFlow wiring
    e = ensure_objects()
    t = train_model_and_forecast()
    u = build_final_union()
    e >> t >> u

ml_forecast_tf()