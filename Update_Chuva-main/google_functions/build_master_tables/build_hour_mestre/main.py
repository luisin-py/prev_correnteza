#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_hour_mestre.py (rev. 2025‑06‑03‑BRT‑verbose‑fix)
─────────────────────────────────────────────────────
Monta a tabela‑mestre horária somente com dados da **hora corrente de
Brasília (UTC‑3)**, com logs detalhados.
"""

from __future__ import annotations
import os
import logging
from datetime import datetime, timedelta, timezone
from functools import reduce
from typing import List, Tuple

import numpy as np
import pandas as pd
from google.cloud import bigquery

# ───────────── logging ─────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "DEBUG").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(funcName)s:%(lineno)d | %(message)s",
)
log = logging.getLogger("build_hour_mestre")

import warnings
warnings.filterwarnings("ignore", category=pd.errors.PerformanceWarning)

# ───────────── config ─────────────
PROJECT = "local-bliss-359814"
SRC_DS = "wherehouse"
DEST_DS = "wherehouse_tratado"
DEST_TBL = f"{PROJECT}.{DEST_DS}.mestre_hour"

SRC = {
    "update_5min": f"{PROJECT}.{SRC_DS}.update_rawdata_5min_backfill",
    "open_now": f"{PROJECT}.{SRC_DS}.dados_openwhather",
    "forecast": f"{PROJECT}.{SRC_DS}.dados_openweather_forecast",
    "air": f"{PROJECT}.{SRC_DS}.dados_openweather_air_pollution",
    "inmet": f"{PROJECT}.{SRC_DS}.dados_inmet_estacoes",
}

SUFFIX = {
    "update_5min": "upd5m",
    "open_now": "ow_now",
    "forecast": "ow_fcst",
    "air": "ow_air",
    "inmet": "inmet",
}

TS_MAP: dict[str, Tuple[str, bool]] = {
    "update_5min": ("timestamp", False),
    "open_now": ("timestamp", False),
    "forecast": ("timestamp_execucao", False),
    "air": ("timestamp_execucao", False),
    "inmet": ("timestamp_execucao", False),
}

TS_COL_CAND = {"timestamp", "timestamp_utc", "dt_utc", "dt", "hora", "timestamp_execucao"}

bq = bigquery.Client(project=PROJECT)

# ───────────── helpers ─────────────

def brasília_window() -> Tuple[datetime, datetime]:
    now_brt = datetime.now(timezone(timedelta(hours=-3)))
    start_brt = now_brt.replace(minute=0, second=0, microsecond=0)
    end_brt = start_brt + timedelta(hours=1)
    return start_brt.astimezone(timezone.utc), end_brt.astimezone(timezone.utc)


def build_filter(col: str, start: datetime, end: datetime, epoch: bool) -> str:
    col_ts = f"TIMESTAMP_SECONDS({col})" if epoch else col
    return (
        f"WHERE {col_ts} >= TIMESTAMP('{start.isoformat()}') "
        f"AND   {col_ts} < TIMESTAMP('{end.isoformat()}')"
    )


def log_df(df: pd.DataFrame, name: str):
    if df.empty:
        log.debug("%s → DataFrame vazio", name)
    else:
        log.debug("%s → %d×%d", name, *df.shape)
        log.debug("%s cols: %s", name, list(df.columns))
        log.debug("%s head:\n%s", name, df.head(3).to_string(index=False))


def query(sql: str, name: str) -> pd.DataFrame:
    log.info("SQL %s", name)
    return bq.query(sql).to_dataframe()


def drop_valid(df):
    return df.loc[:, ~df.columns.str.startswith("valid_")]


def drop_empty(df):
    return df.dropna(axis=1, how="all")


def add_suffix(df, sfx):
    df = df.copy()
    df.columns = [c if c in TS_COL_CAND else f"{c}_{sfx}" for c in df.columns]
    return df


def to_hour(df, ts):
    if df.empty:
        return df
    df = df.copy()
    df["hora"] = pd.to_datetime(df[ts]).dt.floor("h")
    agg = {c: ("mean" if pd.api.types.is_numeric_dtype(df[c]) else "first") for c in df.columns if c not in {"hora", ts}}
    return df.groupby("hora").agg(agg).reset_index().rename(columns={"hora": "timestamp_utc"})


def force_str(df):
    df = df.copy()
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"]).dt.floor("s")
    for c in df.columns:
        if c != "timestamp_utc":
            df[c] = df[c].astype(str).replace(["nan", "None", "NaT"], "")
    return df


def schema_from(df):
    return [bigquery.SchemaField(c, "TIMESTAMP" if c == "timestamp_utc" else "STRING") for c in df.columns]


def ensure_table(schema):
    try:
        tbl = bq.get_table(DEST_TBL)
        if tbl.time_partitioning.field != "timestamp_utc":
            raise RuntimeError("partition errado")
    except Exception:
        tbl = bigquery.Table(DEST_TBL, schema=schema)
        tbl.time_partitioning = bigquery.TimePartitioning(field="timestamp_utc")
        bq.create_table(tbl)


def send_batch(df_sub, idx, cfg):
    job = bq.load_table_from_dataframe(df_sub, DEST_TBL, job_config=cfg)
    job.result()
    log.info("Lote %d ok (%d linhas)", idx, len(df_sub))

# ───────────── main ─────────────

def main(event=None, context=None):
    start_utc, end_utc = brasília_window()
    log.info("Janela UTC: %s → %s", start_utc, end_utc)

    dfs: List[pd.DataFrame] = []

    for key, tbl in SRC.items():
        ts_col, is_epoch = TS_MAP[key]
        sql = f"SELECT * FROM `{tbl}` {build_filter(ts_col, start_utc, end_utc, is_epoch)}"
        df = query(sql, key)
        log_df(df, f"{key}_raw")
        if df.empty:
            continue
        if key == "update_5min":
            df = to_hour(drop_empty(add_suffix(drop_valid(df), SUFFIX[key])), ts_col)
        elif key == "open_now":
            df = drop_empty(add_suffix(df, SUFFIX[key]))
        elif key in ("forecast", "air"):
            df = add_suffix(df, SUFFIX[key])
            if "timestamp_utc" not in df.columns:
                df["timestamp_utc"] = pd.to_datetime(df[ts_col])
            df = drop_empty(df)
        elif key == "inmet":
            df = add_suffix(df, SUFFIX[key])
            if "timestamp_utc" not in df.columns:
                df = df.rename(columns={ts_col: "timestamp_utc"})
            df = drop_empty(df)
        log_df(df, f"{key}_proc")
        dfs.append(df)

    if not dfs:
        log.warning("sem dados")
        return

    df = reduce(lambda l, r: pd.merge(l, r, on="timestamp_utc", how="outer"), dfs).copy()
    log_df(df, "merged")

    df = force_str(df)
    df["dia_particao"] = pd.to_datetime(df["timestamp_utc"]).dt.date
    schema = schema_from(df.drop(columns="dia_particao"))
    ensure_table(schema)

    cfg = bigquery.LoadJobConfig(schema=schema, write_disposition="WRITE_APPEND")

    for idx, dia in enumerate(sorted(df["dia_particao"].unique()), 1):
        send_batch(df[df["dia_particao"] == dia].drop(columns="dia_particao"), idx, cfg)

    log.info("✔ finalizado (%d linhas)", len(df))

if __name__ == "__main__":
    main()
