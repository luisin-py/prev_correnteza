#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Previsões recursivas multi-profundidade de correnteza usando DNAs salvos,
com upsert estilo "maré" (DELETE IN + INSERT) para a tabela horária e
PRESERVAÇÃO DE HISTÓRICO EM 5-MIN via STAGE + MERGE (upsert) para a tabela de 5-min.
"""

import os
import json
from datetime import datetime, timedelta
import numpy as np
import pandas as pd
from google.cloud import bigquery

# ========== CONFIG ==========
PROJECT_ID = "local-bliss-359814"

# fonte real (mestre hora)
DATASET_MESTRE = "wherehouse_tratado"
TABLE_HOUR = "mestre_hour_tratada"

# previsões
DATASET_PREV = "wherehouse_previsoes"
TABLE_PREV_WIDE = f"{PROJECT_ID}.{DATASET_PREV}.prev_correnteza"               # HORÁRIA (histórico, upsert)
# 5-min: agora com stage + histórico
TABLE_PREV5_STAGE = f"{PROJECT_ID}.{DATASET_PREV}.prev_correnteza_5min_stage"  # STAGE (TRUNCATE)
TABLE_PREV5_HIST  = f"{PROJECT_ID}.{DATASET_PREV}.prev_correnteza_5min"        # HISTÓRICA (MERGE)

DNA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "best_dna_correnteza_prev")
DEPTHS = ["1_5m", "3m", "6m", "7_5m", "9m", "10_5m", "12m", "13_5m", "superficie"]

HORIZON_MINUTES = 360
STEP_MINUTES = 60
STEP_MINUTES_5 = 5

# ========== Utils ==========
def fix_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "timestamp_br" in df.columns:
        df["timestamp_br"] = pd.to_datetime(df["timestamp_br"], errors="coerce")
        df = df[df["timestamp_br"].notna()]
        df["timestamp_br"] = df["timestamp_br"].astype("datetime64[ns]")
    return df

def make_time_features(df: pd.DataFrame, ts_col: str = "timestamp_br") -> pd.DataFrame:
    ts = pd.to_datetime(df[ts_col])
    hour = ts.dt.hour + ts.dt.minute / 60.0
    df["tod_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["tod_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * ts.dt.dayofweek / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * ts.dt.dayofweek / 7.0)
    return df

def drop_fully_nan_predictions(df: pd.DataFrame) -> pd.DataFrame:
    value_cols = [c for c in df.columns if c.startswith("valor_previsto_")]
    if not value_cols:
        return df
    return df[df[value_cols].notna().any(axis=1)].reset_index(drop=True)

def run_query(client: bigquery.Client, sql: str, params=None):
    cfg = bigquery.QueryJobConfig(query_parameters=params or [])
    return client.query(sql, job_config=cfg).result()

def schema_prev_correnteza():
    return [bigquery.SchemaField("timestamp_br", "DATETIME", mode="REQUIRED")] + [
        bigquery.SchemaField(f"valor_previsto_{d}", "NUMERIC", mode="NULLABLE") for d in DEPTHS
    ]

def ensure_table(client: bigquery.Client, table_fq: str, schema_fields):
    from google.api_core.exceptions import NotFound
    try:
        client.get_table(table_fq)
    except NotFound:
        table = bigquery.Table(table_fq, schema=schema_fields)
        client.create_table(table)

def load_dna(depth: str):
    path = os.path.join(DNA_DIR, f"{depth}_intensity.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def build_base_mapping(feat_cols, df_columns):
    mapping = {}
    for c in {fc.rsplit("_L", 1)[0] for fc in feat_cols}:
        if c in df_columns:
            mapping[c] = c
        elif f"{c}_ajustada" in df_columns:
            mapping[c] = f"{c}_ajustada"
        else:
            mapping[c] = None
    return mapping

def add_or_update_lags(df: pd.DataFrame, mapping, lags):
    lagged = {}
    for base, src in mapping.items():
        series = df[src] if (src is not None and src in df.columns) else pd.Series(np.nan, index=df.index)
        for L in lags:
            lagged[f"{base}_L{L}"] = series.shift(L)
    if lagged:
        df = pd.concat([df, pd.DataFrame(lagged, index=df.index)], axis=1)
    return df

def standardize_apply(X, mu, sigma):
    sigma = np.where(sigma == 0, 1.0, sigma)
    return (X - mu) / sigma

def predict_with_genome(X, genome):
    w = np.array(genome[:-1], dtype=float)
    b = float(genome[-1])
    return X @ w + b

def apply_calibration(y_hat_raw, dna):
    a, b = dna.get("calibration", [1.0, 0.0])
    return a * y_hat_raw + b

# ========== Core ==========
def recursive_forecast(df_hist, feat_cols, dna, base_mapping, target_base, horizon_steps, step_minutes, ts_col):
    lags = dna["lags"]
    mu = np.array(dna["mu"], dtype=float)
    sigma = np.array(dna["sigma"], dtype=float)
    genome = dna["genome"]

    keep_cols = [ts_col] + sorted({src for src in base_mapping.values() if src is not None})
    df_roll = df_hist[keep_cols].copy()
    last_ts = pd.to_datetime(df_roll[ts_col].iloc[-1])
    preds = []

    src_for_target = base_mapping.get(target_base, target_base)
    if src_for_target not in df_roll.columns:
        df_roll[src_for_target] = np.nan

    for h in range(1, horizon_steps + 1):
        df_roll = add_or_update_lags(df_roll, base_mapping, lags)
        row = df_roll.iloc[-1]

        # vetor de features + imputação por mu
        X_row = np.array([row.get(c, np.nan) for c in feat_cols], dtype=float).reshape(1, -1)
        if X_row.shape[1] != len(mu):
            break  # incompatível com o DNA
        mask = np.isnan(X_row).ravel()
        if mask.any():
            X_row[0, mask] = mu[mask]

        X_std = standardize_apply(X_row, mu, sigma)
        y_hat_raw = float(predict_with_genome(X_std, genome))
        y_hat_cal = float(apply_calibration(y_hat_raw, dna))

        next_ts = last_ts + timedelta(minutes=step_minutes * h)
        future = df_roll.iloc[-1][keep_cols].copy()
        future[ts_col] = next_ts
        future[src_for_target] = y_hat_cal

        df_roll = pd.concat([df_roll[keep_cols], future.to_frame().T], ignore_index=True)
        preds.append({"timestamp_prev": next_ts, "y_hat": y_hat_cal})

    return preds

def interpolate_forecast(preds, freq_minutes=5):
    if not preds:
        return {}
    times = [p["timestamp_prev"] for p in preds]
    values = [p["y_hat"] for p in preds]
    result, t = {}, times[0]
    while t <= times[-1]:
        if t in times:
            result[t] = float(values[times.index(t)])
        else:
            prev_idx = max(i for i, tt in enumerate(times) if tt <= t)
            next_idx = min(i for i, tt in enumerate(times) if tt >= t)
            if prev_idx == next_idx:
                interp = float(values[prev_idx])
            else:
                dt = (times[next_idx] - times[prev_idx]).total_seconds()
                alpha = (t - times[prev_idx]).total_seconds() / dt if dt else 0.0
                interp = values[prev_idx] + alpha * (values[next_idx] - values[prev_idx])
            result[t] = float(interp)
        t += timedelta(minutes=freq_minutes)
    return result

def get_last_real_history(client, table_hour_fq, min_rows=12):
    sql = f"""
    SELECT *
    FROM `{table_hour_fq}`
    WHERE altura_real_getmare IS NOT NULL
    ORDER BY timestamp_br DESC
    LIMIT {min_rows * 3}
    """
    df = run_query(client, sql).to_dataframe(create_bqstorage_client=True)
    df = df[df["altura_real_getmare"].notna()].sort_values("timestamp_br").tail(min_rows).reset_index(drop=True)
    df = fix_timestamp(df)
    df = make_time_features(df, "timestamp_br")
    return df

# ---------- Upload helpers ----------
def load_df_csv(client: bigquery.Client, df: pd.DataFrame, table_fq: str, write="WRITE_APPEND"):
    """Upload robusto via CSV."""
    df = df.copy()
    if "timestamp_br" in df.columns:
        df["timestamp_br"] = pd.to_datetime(df["timestamp_br"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    temp_path = "/tmp/_bq_temp_prev_correnteza.csv"
    df.to_csv(temp_path, index=False, encoding="utf-8")
    with open(temp_path, "rb") as f:
        client.load_table_from_file(
            f, table_fq,
            job_config=bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                write_disposition=write,
            ),
        ).result()
    print(f"Upload CSV concluído em {table_fq} ({write}).")

def delete_prev_correnteza(client: bigquery.Client, df_ins: pd.DataFrame, table_fq: str):
    """Remove na tabela final (horária) os registros exatamente nos timestamps de df_ins['timestamp_br']."""
    if df_ins.empty:
        return
    ts_list = (
        pd.to_datetime(df_ins['timestamp_br'])
        .dt.strftime('%Y-%m-%d %H:%M:%S').tolist()
    )
    if not ts_list:
        return
    lista = ",".join(f"DATETIME('{ts}')" for ts in ts_list)
    sql = f"DELETE FROM `{table_fq}` WHERE timestamp_br IN ({lista})"
    run_query(client, sql)
    print(f"DELETE em {table_fq}: {len(ts_list)} timestamps removidos.")

def insert_prev_correnteza(client: bigquery.Client, df_ins: pd.DataFrame, table_fq: str):
    """INSERT batelado para a tabela horária (após DELETE dos mesmos timestamps)."""
    if df_ins.empty:
        return
    df = df_ins.copy()
    df["timestamp_br"] = pd.to_datetime(df["timestamp_br"]).dt.floor("H")
    value_cols = [c for c in df.columns if c.startswith("valor_previsto_")]
    cols = ["timestamp_br"] + value_cols

    def val_or_null(x):
        return "NULL" if pd.isna(x) else f"{float(x):.9f}"

    rows_sql = []
    for _, r in df.iterrows():
        ts = pd.to_datetime(r["timestamp_br"]).strftime("%Y-%m-%d %H:%M:%S")
        vals = [f"DATETIME('{ts}')"] + [val_or_null(r.get(c)) for c in value_cols]
        rows_sql.append("(" + ",".join(vals) + ")")

    if not rows_sql:
        return

    col_list = ",".join(cols)
    values_sql = ",".join(rows_sql)
    sql = f"INSERT INTO `{table_fq}` ({col_list}) VALUES {values_sql}"
    run_query(client, sql)
    print(f"INSERT em {table_fq}: {len(df)} linhas.")

def merge_5min_from_stage(client: bigquery.Client, table_hist: str, table_stage: str, value_cols: list):
    """
    Faz MERGE (upsert) de stage -> histórico em 5-min.
    Atualiza quando timestamp_br já existe; insere quando não existe.
    """
    set_clause = ",\n      ".join([f"T.{c} = S.{c}" for c in value_cols])
    col_list = ", ".join(["timestamp_br"] + value_cols)
    val_list = ", ".join([f"S.{c}" for c in ["timestamp_br"] + value_cols])

    sql = f"""
    MERGE `{table_hist}` T
    USING (
      SELECT {col_list}
      FROM `{table_stage}`
    ) S
    ON T.timestamp_br = S.timestamp_br
    WHEN MATCHED THEN UPDATE SET
      {set_clause}
    WHEN NOT MATCHED THEN
      INSERT ({col_list}) VALUES ({val_list})
    """
    run_query(client, sql)
    print(f"MERGE 5-min concluído: {table_stage} -> {table_hist}")

# ========== Main ==========
def main():
    client = bigquery.Client(project=PROJECT_ID)

    # Garante tabelas (horária + 5-min stage + 5-min histórica)
    ensure_table(client, TABLE_PREV_WIDE, schema_prev_correnteza())
    ensure_table(client, TABLE_PREV5_STAGE, schema_prev_correnteza())
    ensure_table(client, TABLE_PREV5_HIST,  schema_prev_correnteza())

    table_hour_fq = f"{PROJECT_ID}.{DATASET_MESTRE}.{TABLE_HOUR}"

    # Carrega DNAs válidos
    dnas = {depth: load_dna(depth) for depth in DEPTHS}
    dnas = {k: v for k, v in dnas.items()
            if v and all(key in v for key in ("feat_cols", "mu", "sigma", "lags", "genome"))}
    if not dnas:
        print("Nenhum DNA válido. Encerrando.")
        return

    # Histórico real
    df_real = get_last_real_history(client, table_hour_fq, min_rows=12)
    if df_real.empty:
        print("Histórico real está vazio. Encerrando.")
        return

    # Previsões
    preds_by_depth_hour, preds_by_depth_5min = {}, {}
    for depth, dna in dnas.items():
        target_base = f"intensidade_{depth}" if depth != "superficie" else "intensidade_superficie"
        base_mapping = build_base_mapping(dna["feat_cols"], df_real.columns)
        preds = recursive_forecast(
            df_hist=df_real,
            feat_cols=list(dna["feat_cols"]),
            dna=dna,
            base_mapping=base_mapping,
            target_base=target_base,
            horizon_steps=HORIZON_MINUTES // STEP_MINUTES,
            step_minutes=STEP_MINUTES,
            ts_col="timestamp_br"
        )
        if not preds:
            print(f"[WARN] Sem previsões para {depth} (preds vazio).")
            continue
        preds_by_depth_hour[depth] = preds
        preds_by_depth_5min[depth] = interpolate_forecast(preds, freq_minutes=STEP_MINUTES_5)

    if not preds_by_depth_hour:
        print("[WARN] Nenhuma profundidade gerou previsão. Encerrando sem upload.")
        return

    # Monta DataFrame hora/hora
    all_ts = sorted({p["timestamp_prev"] for plist in preds_by_depth_hour.values() for p in plist})
    df_prev_h = pd.DataFrame({"timestamp_br": all_ts})
    for depth, plist in preds_by_depth_hour.items():
        s = pd.Series({p["timestamp_prev"]: float(p["y_hat"]) for p in plist})
        df_prev_h = df_prev_h.merge(
            s.rename(f"valor_previsto_{depth}").rename_axis("timestamp_br").reset_index(),
            on="timestamp_br", how="left"
        )

    df_prev_h["timestamp_br"] = pd.to_datetime(df_prev_h["timestamp_br"]).dt.floor("H")
    df_prev_h = df_prev_h.drop_duplicates(subset=["timestamp_br"]).sort_values("timestamp_br").reset_index(drop=True)
    df_prev_h = drop_fully_nan_predictions(df_prev_h)

    # 5-min
    all_ts5 = sorted({ts for d in preds_by_depth_5min.values() for ts in d.keys()})
    df_prev_5 = pd.DataFrame({"timestamp_br": all_ts5}) if all_ts5 else pd.DataFrame(columns=["timestamp_br"])
    for depth, dmap in preds_by_depth_5min.items():
        if not dmap:
            continue
        s5 = pd.Series({ts: float(v) for ts, v in dmap.items()})
        df_prev_5 = df_prev_5.merge(
            s5.rename(f"valor_previsto_{depth}").rename_axis("timestamp_br").reset_index(),
            on="timestamp_br", how="left"
        )
    if not df_prev_5.empty:
        df_prev_5["timestamp_br"] = pd.to_datetime(df_prev_5["timestamp_br"]).dt.floor("5min")
        df_prev_5 = df_prev_5.drop_duplicates(subset=["timestamp_br"]).sort_values("timestamp_br").reset_index(drop=True)
        df_prev_5 = drop_fully_nan_predictions(df_prev_5)

    # Arredondamento de segurança
    for col in df_prev_5.columns:
        if col.startswith("valor_previsto_"):
            df_prev_5[col] = df_prev_5[col].round(9)


    # ===== Upsert estilo maré na tabela horária (evita duplicatas) =====
    if not df_prev_h.empty:
        delete_prev_correnteza(client, df_prev_h, TABLE_PREV_WIDE)
        insert_prev_correnteza(client, df_prev_h, TABLE_PREV_WIDE)
    else:
        print("[WARN] df_prev_h vazio após filtros; não houve upsert hora/hora.")

    # ===== 5-MIN COM HISTÓRICO: STAGE + MERGE =====
    if not df_prev_5.empty:
        # 1) joga no STAGE com TRUNCATE (apenas o horizonte atual)
        load_df_csv(client, df_prev_5, TABLE_PREV5_STAGE, write="WRITE_TRUNCATE")
        # 2) MERGE do STAGE para a tabela HISTÓRICA (acumula e atualiza)
        value_cols_5 = [c for c in df_prev_5.columns if c.startswith("valor_previsto_")]
        merge_5min_from_stage(client, TABLE_PREV5_HIST, TABLE_PREV5_STAGE, value_cols_5)
    else:
        print("[WARN] df_prev_5 vazio após filtros; não houve MERGE 5-min.")

    print(f"[{datetime.now()}] Previsão gerada e salva no BigQuery (hora + 5-min histórico)!")
    print(f"Tabelas: hora={TABLE_PREV_WIDE} | 5min_stage={TABLE_PREV5_STAGE} | 5min_hist={TABLE_PREV5_HIST}")

if __name__ == "__main__":
    main()
