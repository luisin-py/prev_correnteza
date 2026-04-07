# build_master_tables.py
"""Consolida várias tabelas brutas em duas tabelas‑mestre no BigQuery

• mestre_5min  – granularidade de 5 minutos
• mestre_hour  – granularidade horária

Regras resumidas
────────────────
update_rawdata_5min_backfill         → 5 min 1:1 | hora = média
inmet (dados_inmet_estacoes_backfill) → hora 1:1 | 5 min = duplicar 12×
openweather (current)                → 5 min 1:1 | hora = média
forecast 3 h                         → copiar nos dois destinos (timestamp original)
air‑pollution 3 h                    → copiar nos dois destinos (timestamp original)

A chave para deduplicação é  (timestamp_utc, cidade/estacao)
"""
from __future__ import annotations

import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import Client, LoadJobConfig
from datetime import datetime, timezone, timedelta
import pytz

# ─────────────────────────── CONFIG ────────────────────────────
PROJECT_ID   = "local-bliss-359814"
DATASET      = "wherehouse"
TABLES_SRC   = {
    "update_rawdata_5min_backfill"        : "update_rawdata_5min_backfill",
    "inmet"                               : "dados_inmet_estacoes_backfill",
    "openweather_now"                     : "dados_openwhather",
    "openweather_forecast"                : "dados_openweather_forecast",
    "openweather_air_pollution"           : "dados_openweather_air_pollution",
}
MASTER_5MIN  = f"{PROJECT_ID}.{DATASET}.mestre_5min"
MASTER_HOUR  = f"{PROJECT_ID}.{DATASET}.mestre_hour"

BQ_CLIENT = Client(project=PROJECT_ID)

# ─────────────────────────── HELPERS ───────────────────────────
LOCAL_TZ   = pytz.timezone("America/Sao_Paulo")
now_local  = lambda: datetime.now(LOCAL_TZ).replace(tzinfo=None)

NUMERIC_FALLBACK = ("FLOAT64", "NUMERIC", "INTEGER")

def _avg_cols(df: pd.DataFrame) -> dict[str, str]:
    """Retorna dicionário coluna:agregador (AVG ou ANY_VALUE)."""
    agg = {}
    for c, dtype in zip(df.columns, df.dtypes):
        if pd.api.types.is_numeric_dtype(dtype):
            agg[c] = "avg"
        else:
            agg[c] = "any_value"
    return agg


def _duplicate_rows(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """Repete cada linha `minutes/5` vezes avançando 5 min no timestamp_utc."""
    if df.empty:
        return df
    reps = minutes // 5
    dfs  = []
    for i in range(reps):
        temp = df.copy()
        temp["timestamp_utc"] = pd.to_datetime(temp["timestamp_utc"]) + pd.to_timedelta(i*5, "min")
        dfs.append(temp)
    return pd.concat(dfs, ignore_index=True)


def _load_to_bq(df: pd.DataFrame, table_id: str):
    if df.empty:
        return
    job_cfg = LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
    BQ_CLIENT.load_table_from_dataframe(df, table_id, job_config=job_cfg).result()

# ───────────────────────── PROCESSADORES ───────────────────────

def process_update_rawdata() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.update_rawdata_5min_backfill`"
    df = BQ_CLIENT.query(query).to_dataframe()
    if df.empty:
        return df, df
    # 5 min → manter
    df5 = df.copy()
    # hour → agregação
    df["hora"] = pd.to_datetime(df["timestamp"]).dt.floor("H")
    agg = _avg_cols(df)
    df_hour = df.groupby("hora").agg(agg).reset_index().rename(columns={"hora": "timestamp_utc"})
    return df5, df_hour


def process_inmet() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.dados_inmet_estacoes_backfill` WHERE estacao='A802'"
    df = BQ_CLIENT.query(query).to_dataframe()
    if df.empty:
        return df, df
    # hora já está correto
    df_hour = df.copy().rename(columns={"dt_utc": "timestamp_utc"})
    # duplicar para 5 min
    df5 = _duplicate_rows(df_hour, 60)
    return df5, df_hour


def process_openweather_now() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.dados_openwhather`"
    df = BQ_CLIENT.query(query).to_dataframe()
    if df.empty:
        return df, df
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"])
    df5 = df.copy()
    df["hora"] = df["timestamp_utc"].dt.floor("H")
    agg = _avg_cols(df)
    df_hour = df.groupby("hora").agg(agg).reset_index().rename(columns={"hora": "timestamp_utc"})
    return df5, df_hour


def _filter_now(df: pd.DataFrame) -> pd.DataFrame:
    """Mantém apenas registros com timestamp_utc dentro da hora corrente."""
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    return df[pd.to_datetime(df["timestamp_utc"]).dt.floor("H") == now_utc]


def process_forecast() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.dados_openweather_forecast`"
    df = BQ_CLIENT.query(query).to_dataframe()
    df = _filter_now(df)
    return df, df.copy()


def process_air() -> tuple[pd.DataFrame, pd.DataFrame]:
    query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.dados_openweather_air_pollution`"
    df = BQ_CLIENT.query(query).to_dataframe()
    df = _filter_now(df)
    return df, df.copy()

# ─────────────────────────── MAIN ──────────────────────────────

def main():
    print("🚀 Iniciando consolidação", now_local())
    df5_all   : list[pd.DataFrame] = []
    dfh_all   : list[pd.DataFrame] = []

    for fn in (process_update_rawdata, process_inmet, process_openweather_now, process_forecast, process_air):
        d5, dh = fn()
        df5_all.append(d5)
        dfh_all.append(dh)

    df5min  = pd.concat(df5_all, ignore_index=True, sort=False)
    dfhour  = pd.concat(dfh_all, ignore_index=True, sort=False)

    _load_to_bq(df5min, MASTER_5MIN)
    _load_to_bq(dfhour, MASTER_HOUR)
    print("✅ Concluído", now_local())

if __name__ == "__main__":
    main()
