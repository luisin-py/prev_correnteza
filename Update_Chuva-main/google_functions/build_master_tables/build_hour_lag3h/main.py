#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
build_hour_lag3h.py
───────────────────
Gera linhas sintéticas (t+1 h, t+2 h) para preencher lacunas exatas de 3 h
na série horária — média quadrática (RMS) para numéricos.

Tabela origem : wherehouse_tratado.mestre_hour
Tabela destino: wherehouse_tratado.mestre_hour_lag3h
"""

from __future__ import annotations
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from google.cloud import bigquery

# ───────── CONFIG ─────────
PROJECT   = "local-bliss-359814"
DS_READ   = "wherehouse_tratado"
DS_WRITE  = "wherehouse_tratado"
SRC_TBL   = f"{PROJECT}.{DS_READ}.mestre_hour"
DEST_TBL  = f"{PROJECT}.{DS_WRITE}.mestre_hour_lag3h"

DELAY_HOURS = 3           # só processa registros com ≥ 3 h de antiguidade

bq = bigquery.Client(project=PROJECT)

# ───────── HELPERS ─────────
def rms(a: float, b: float) -> float:
    """Root-Mean-Square entre dois números (float)."""
    return float(np.sqrt((a ** 2 + b ** 2) / 2))

def get_key_cols(df: pd.DataFrame) -> list[str]:
    """Retorna as colunas de chave disponíveis (prioridade city_name > estacao)."""
    if "city_name" in df.columns:
        return ["city_name"]
    if "estacao" in df.columns:
        return ["estacao"]
    return []  # fallback: não particiona

def fetch_last_two_per_key() -> pd.DataFrame:
    """Busca, para cada chave, os dois registros mais recentes com atraso ≥ 3 h."""
    cutoff_utc = (
        datetime.utcnow() - timedelta(hours=DELAY_HOURS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    sql = f"""
        SELECT * EXCEPT(rn)
        FROM (
          SELECT *,
                 ROW_NUMBER() OVER (
                     PARTITION BY COALESCE(city_name, estacao)
                     ORDER BY timestamp_utc DESC
                 ) AS rn
          FROM `{SRC_TBL}`
          WHERE timestamp_utc <= TIMESTAMP('{cutoff_utc}')
        )
        WHERE rn <= 2
    """
    return bq.query(sql).to_dataframe()

def build_synthetic(df: pd.DataFrame) -> pd.DataFrame:
    """Para cada lacuna exata de 3 h, gera linhas t+1 h e t+2 h via RMS."""
    if df.empty:
        return df

    key_cols = get_key_cols(df)
    numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()

    synthetic_rows = []
    for _, grp in df.groupby(key_cols, dropna=False):
        if len(grp) < 2:
            continue
        grp = grp.sort_values("timestamp_utc")      # p1 (antigo) → p0 (novo)
        p1, p0 = grp.iloc[-2], grp.iloc[-1]

        t1 = pd.to_datetime(p1["timestamp_utc"])
        t0 = pd.to_datetime(p0["timestamp_utc"])

        # só preenche se gap exato de 3 h
        if (t0 - t1) != timedelta(hours=3):
            continue

        for offset in (1, 2):        # gera t+1 h e t+2 h
            rec = p0.copy()
            rec["timestamp_utc"] = t1 + timedelta(hours=offset)
            for col in numeric_cols:
                # se qualquer extremo nulo, mantém null
                rec[col] = (
                    rms(p1[col], p0[col]) if pd.notna(p1[col]) and pd.notna(p0[col]) else None
                )
            rec["synthetic"] = True
            synthetic_rows.append(rec)

    return pd.DataFrame(synthetic_rows)

def load_to_bq(df: pd.DataFrame):
    if df.empty:
        print("⚠️  Nenhuma linha sintética a inserir (sem lacunas 3 h).")
        return
    cfg = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )
    bq.load_table_from_dataframe(df, DEST_TBL, job_config=cfg).result()
    print(f"✅  {len(df)} linhas sintéticas gravadas em {DEST_TBL}")

# ───────── ENTRY-POINT (CF & local) ─────────
def main(event=None, context=None):
    base_df   = fetch_last_two_per_key()
    synth_df  = build_synthetic(base_df)
    load_to_bq(synth_df)

if __name__ == "__main__":
    main()
