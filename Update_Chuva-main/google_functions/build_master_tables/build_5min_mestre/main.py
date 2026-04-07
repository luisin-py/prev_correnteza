#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_5min_mestre.py
────────────────────
• Consolida múltiplas fontes no dataset wherehouse → grava em wherehouse_tratado.mestre_5min
• Remove todas as colunas de validação (prefixo 'valid_') vindas de update_rawdata_5min_backfill
"""

from __future__ import annotations
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

# ───────── Config ─────────
PROJECT   = "local-bliss-359814"
SRC_DS    = "wherehouse"            # dataset de leitura
DEST_DS   = "wherehouse_tratado"    # dataset de escrita
DEST_TBL  = f"{PROJECT}.{DEST_DS}.mestre_5min"

bq = bigquery.Client(project=PROJECT)

SRC = {
    "update_5min": f"{PROJECT}.{SRC_DS}.update_rawdata_5min_backfill",
    "open_now"  :  f"{PROJECT}.{SRC_DS}.dados_openwhather",
    "forecast"  :  f"{PROJECT}.{SRC_DS}.dados_openweather_forecast",
    "air"       :  f"{PROJECT}.{SRC_DS}.dados_openweather_air_pollution",
    "inmet"     :  f"{PROJECT}.{SRC_DS}.dados_inmet_estacoes_backfill"
}

# ───────── Helpers ─────────
def query_df(sql: str) -> pd.DataFrame:
    """Roda a consulta no BigQuery e devolve DataFrame."""
    return bq.query(sql).to_dataframe()

def drop_valid_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas que começam com 'valid_'."""
    return df.loc[:, ~df.columns.str.startswith("valid_")]

def explode_inmet_1h_to_5min(df: pd.DataFrame) -> pd.DataFrame:
    """Duplica cada linha horária (INMET) em 12 blocos de 5 min."""
    if df.empty:
        return df
    replicas = []
    for shift in range(12):  # 0-55 min
        d = df.copy()
        d["timestamp_utc"] = (
            pd.to_datetime(d["dt_utc"]) + pd.to_timedelta(shift * 5, "min")
        )
        replicas.append(d)
    return pd.concat(replicas, ignore_index=True)

# ───────── Pipeline principal ─────────
def main(event=None, context=None):
    dfs: list[pd.DataFrame] = []

    # 1) update_rawdata_5min_backfill (remove valid_*)
    upd = query_df(f"SELECT * FROM `{SRC['update_5min']}`")
    dfs.append(drop_valid_cols(upd))

    # 2) OpenWeather NOW
    dfs.append(query_df(f"SELECT * FROM `{SRC['open_now']}`"))

    # 3) Forecast & Air → pegar bloco da hora corrente
    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    for key in ("forecast", "air"):
        tbl = SRC[key]
        dfs.append(
            query_df(
                f"""
                SELECT *
                FROM `{tbl}`
                WHERE DATETIME_TRUNC(timestamp_utc, HOUR) = TIMESTAMP('{now_utc}')
                """
            )
        )

    # 4) INMET (apenas estação A802 → Rio Grande) 1 h → duplica 5 min
    inmet = query_df(
        f"SELECT * FROM `{SRC['inmet']}` WHERE estacao = 'A802'"
    )
    dfs.append(explode_inmet_1h_to_5min(inmet))

    # 5) Concatena e grava
    df_final = pd.concat(dfs, ignore_index=True, sort=False)
    if df_final.empty:
        print("⚠️  Nada para inserir em mestre_5min")
        return

    cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,           # cria colunas novas se aparecerem
    )
    bq.load_table_from_dataframe(df_final, DEST_TBL, job_config=cfg).result()
    print(f"✅  {len(df_final)} linhas inseridas em {DEST_TBL}")

# Execução local
if __name__ == "__main__":
    main()
