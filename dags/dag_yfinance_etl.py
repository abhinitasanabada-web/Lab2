# dags/dag_yfinance_etl.py
# Airflow DAG #1 — yfinance → Snowflake (ETL)
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import List

import pandas as pd
import yfinance as yf
import snowflake.connector # Still needed for type hinting/optional custom use

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator 
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook 


# --- Configuration ---
SNOWFLAKE_CONN_ID = "snowflake_catfish"
default_args = {"depends_on_past": False, "retries": 1}

hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)


def _fetch_prices(symbols: List[str], start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch OHLCV with yfinance and return long-form DataFrame.
    (This is the previously incomplete function, now fully defined.)
    """
    if not symbols:
        return pd.DataFrame(
            columns=["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
        )

    # 1. Download data
    raw = yf.download(
        tickers=" ".join(symbols),
        start=start_date,
        end=end_date,
        auto_adjust=False,   # keep Close and Adj Close distinct
        group_by="ticker",
        progress=False,
        threads=True,
    )

    # Normalize possible MultiIndex columns
    if isinstance(raw.columns, pd.MultiIndex):
        raw.columns = ["_".join([str(c) for c in tup if c]) for tup in raw.columns]

    # 2. Process Dates and build base DataFrame
    idx = pd.to_datetime(raw.index, errors="coerce").tz_localize(None)
    df_base = raw.reset_index(drop=True).copy()

    ts = pd.Series(idx, name="TRADE_TS", dtype="datetime64[ns]")
    good = ts.notna()
    df_base = df_base.loc[good].copy()
    ts = ts.loc[good]
    trade_dates = ts.dt.date # Convert to Python date objects

    # 3. Build long-form rows per symbol
    rows = []
    for sym in symbols:
        sym_u = str(sym).upper().strip()
        col_map = {
            "OPEN":      [f"{sym_u}_Open", "Open"],
            "HIGH":      [f"{sym_u}_High", "High"],
            "LOW":       [f"{sym_u}_Low", "Low"],
            "CLOSE":     [f"{sym_u}_Close", "Close"],
            "ADJ_CLOSE": [f"{sym_u}_Adj Close", "Adj Close", f"{sym_u}_Adj_Close", "Adj_Close"],
            "VOLUME":    [f"{sym_u}_Volume", "Volume"],
        }

        def pick(colnames):
            for c in colnames:
                if c in df_base.columns:
                    return df_base[c]
            return pd.Series([None] * len(df_base), index=df_base.index)

        df_sym = pd.DataFrame({
            "TRADE_DATE": trade_dates,
            "SYMBOL":     sym_u,
            "OPEN":       pick(col_map["OPEN"]),
            "HIGH":       pick(col_map["HIGH"]),
            "LOW":        pick(col_map["LOW"]),
            "CLOSE":      pick(col_map["CLOSE"]),
            "ADJ_CLOSE":  pick(col_map["ADJ_CLOSE"]),
            "VOLUME":     pick(col_map["VOLUME"]),
        })

        if df_sym["ADJ_CLOSE"].isna().all():
            df_sym["ADJ_CLOSE"] = df_sym["CLOSE"]

        df_sym = df_sym[df_sym["CLOSE"].notna()].copy()
        rows.append(df_sym)

    if not rows:
        return pd.DataFrame(
            columns=["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]
        )

    df = pd.concat(rows, ignore_index=True)

    
    df["SYMBOL"] = df["SYMBOL"].astype(str).str.upper().str.strip()
    for col in ["OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["VOLUME"] = pd.to_numeric(df["VOLUME"], errors="coerce").astype("Int64")

    df = df.dropna(subset=["TRADE_DATE", "SYMBOL", "CLOSE"]).copy()
    df = df.drop_duplicates(subset=["TRADE_DATE", "SYMBOL"]).sort_values(["SYMBOL", "TRADE_DATE"])

    logging.info("Fetch summary: %d rows for symbols=%s", len(df), symbols)
    return df[["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]

@task
def ensure_objects(**context):
    """Ensure RAW schema/table exist."""
    schema = Variable.get("target_schema_raw", default_var="RAW")
    

    with hook.get_conn() as conn, conn.cursor() as cur:
        try:
            conn.autocommit(False) # Start transaction
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            cur.execute(f"""
            CREATE OR REPLACE TABLE "{schema}"."STOCK_PRICES" (
              TRADE_DATE DATE NOT NULL,
              SYMBOL     STRING NOT NULL,
              OPEN       DOUBLE,
              HIGH       DOUBLE,
              LOW        DOUBLE,
              CLOSE      DOUBLE,
              ADJ_CLOSE  DOUBLE,
              VOLUME     NUMBER(38,0),
              LOAD_TS    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
              CONSTRAINT PK_STOCK_PRICES PRIMARY KEY (SYMBOL, TRADE_DATE)
            );
            """)
            conn.commit()
        except Exception as e:
            # FIX: Ensure error propagation (Lab 1 Fix)
            try: conn.rollback()
            except Exception: pass
            raise e
            
    return f'Ensured "{schema}".STOCK_PRICES exists'

@task
def fetch_and_load_prices(**context):
    """Fetch data, load to temp, and MERGE into target."""
    symbols_var = Variable.get("stock_symbols", default_var='["AAPL","MSFT","TSLA"]')
    try:
        symbols = json.loads(symbols_var)
        if not isinstance(symbols, list):
            raise ValueError
    except Exception:
        symbols = [s.strip() for s in symbols_var.split(",") if s.strip()]
    lookback_days = int(Variable.get("lookback_days", default_var="365"))
    schema = Variable.get("target_schema_raw", default_var="RAW")

    end_dt = datetime.now(timezone.utc).date()
    start_dt = end_dt - timedelta(days=lookback_days)

    df = _fetch_prices(symbols, start_dt.isoformat(), end_dt.isoformat())
    
    if df.empty:
        return f"No rows fetched for symbols={symbols} in {start_dt}..{end_dt}"

    df["TRADE_DATE"] = pd.to_datetime(df["TRADE_DATE"], errors="coerce").dt.date
    rows = list(
        df[["TRADE_DATE", "SYMBOL", "OPEN", "HIGH", "LOW", "CLOSE", "ADJ_CLOSE", "VOLUME"]]
        .itertuples(index=False, name=None)
    )

    target = f'"{schema}"."STOCK_PRICES"'
    tmp_name = f'STOCK_PRICES_LOAD_{int(datetime.now().timestamp())}'
    tmp_table = f'"{schema}"."{tmp_name}"'

    with hook.get_conn() as conn, conn.cursor() as cur:
        try:
            conn.autocommit(False) 
            
            cur.execute(f"CREATE TEMPORARY TABLE {tmp_table} LIKE {target};")

            insert_sql = f"""
                INSERT INTO {tmp_table}
                (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """
            cur.executemany(insert_sql, rows)

        
            cur.execute(f"""
                MERGE INTO {target} t
                USING {tmp_table} s
                ON t.SYMBOL = s.SYMBOL AND t.TRADE_DATE = s.TRADE_DATE
                WHEN MATCHED THEN UPDATE SET
                  t.OPEN      = s.OPEN, t.HIGH      = s.HIGH, t.LOW = s.LOW,
                  t.CLOSE     = s.CLOSE, t.ADJ_CLOSE = s.ADJ_CLOSE, t.VOLUME = s.VOLUME,
                  t.LOAD_TS   = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN INSERT
                  (TRADE_DATE, SYMBOL, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME)
                VALUES
                  (s.TRADE_DATE, s.SYMBOL, s.OPEN, s.HIGH, s.LOW, s.CLOSE, s.ADJ_CLOSE, s.VOLUME)
            """)
            
            conn.commit()

            cur.execute(f"SELECT COUNT(*) FROM {target}")
            total = cur.fetchone()[0]
            return f"Loaded {len(rows)} rows. Total rows now in {schema}.STOCK_PRICES = {total}"
        except Exception as e:
            # FIX: Ensure error propagation (Lab 1 Fix)
            try: conn.rollback()
            except Exception: pass
            raise e
        finally:
            # Drop temp table regardless of success
            try:
                hook.run(f"DROP TABLE IF EXISTS {tmp_table}")
            except Exception:
                pass


with DAG(
    dag_id="yfinance_etl",
    schedule="@daily",
    start_date=datetime(2025, 9, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["etl", "yfinance", "snowflake"],
) as dag:
    t1 = ensure_objects()
    t2 = fetch_and_load_prices()
    
    # Trigger the dbt ELT DAG )
    t3_trigger_dbt = TriggerDagRunOperator(
        task_id="trigger_dbt_analytics_dag",
        trigger_dag_id="dbt_elt_analytics", # Assumes your dbt DAG is named this
        execution_date="{{ ds }}",
    )
    
    t1 >> t2 >> t3_trigger_dbt