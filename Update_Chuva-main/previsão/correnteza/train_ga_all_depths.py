#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
GA robusto: treino, calibração linear, métricas extra, feature importance e backtest recursivo calibrado.
"""

import os
import json
import random
import threading
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from google.cloud import bigquery

# ====== CONFIGS ======
PROJECT_ID       = "local-bliss-359814"
DATASET_ID       = "wherehouse_tratado"
TABLE_ID         = "mestre_hour_tratada"
BEST_DNA_DIR     = "./best_dna_correnteza_prev"
CSV_PATH         = "dados_para_treino.csv"
TRAIN_DAYS       = 90
TEST_LOOKBACK_DAYS = 14
INTERP_LIMIT_HOURS = 6
DEPTHS_CSV       = "1_5m,3m,6m,7_5m,9m,10_5m,12m,13_5m,superficie"
POP_SIZE         = 200
GENERATIONS      = 60
RANDOM_STATE     = 42
LAGS             = [1, 2, 3, 4, 5, 6]
MIN_FEATURE_DENSITY = 0.65
MIN_ROWS_TO_TRAIN   = 100
SLEEP_BETWEEN_TRAIN = 3600

EXOG_PHYS_FORCED = ["altura_prev_getmare", "altura_real_getmare", "enchente_vazante"]

EXOG_CANDIDATES = [
    "ow_wind_speed", "ow_wind_deg", "vento_vel_m_s_inmet", "vento_dir_deg_inmet",
    "fc_wind_speed", "fc_wind_deg", "ventointensidade", "ventonum",
    "temperatura", "sensacaotermica", "umidade", "pressao",
    "ow_rain_1h", "ow_rain_3h", "fc_rain_3h"
] + EXOG_PHYS_FORCED + ["tod_sin", "tod_cos", "dow_sin", "dow_cos"]

random.seed(RANDOM_STATE)
np.random.seed(RANDOM_STATE)
os.makedirs(BEST_DNA_DIR, exist_ok=True)
SENTINEL_VALUES = set([-9.223372036854776e+18, -9.223e+18, -9999, 9999])

# ==== Funções utilitárias ====
def to_float_or_nan(x):
    try:
        x = float(x)
        if x in SENTINEL_VALUES or np.isnan(x) or np.isinf(x):
            return np.nan
        return x
    except Exception:
        return np.nan

def make_time_features(df, ts_col="timestamp_br"):
    ts = pd.to_datetime(df[ts_col])
    hour = ts.dt.hour + ts.dt.minute / 60.0
    df["tod_sin"] = np.sin(2 * np.pi * hour / 24.0)
    df["tod_cos"] = np.cos(2 * np.pi * hour / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * ts.dt.dayofweek / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * ts.dt.dayofweek / 7.0)
    return df

def pick_existing_and_dense(df, cols, min_notna_frac=0.65):
    return [c for c in cols if c in df.columns and df[c].notna().mean() >= min_notna_frac]

def add_lags(df, cols, lags):
    lagged_cols = {}
    for c in cols:
        if c in df.columns:
            for L in lags:
                lagged_cols[f"{c}_L{L}"] = df[c].shift(L)
    if lagged_cols:
        df_out = pd.concat([df, pd.DataFrame(lagged_cols, index=df.index)], axis=1)
    else:
        df_out = df.copy()
    return df_out

def add_physical_deltas(df):
    # Força as físicas sempre presentes, calcula seus deltas
    for col in EXOG_PHYS_FORCED:
        if col in df.columns:
            delta_col = f"d_{col}"
            df[delta_col] = df[col].diff()
    return df

def load_data_bq(project_id, dataset_id, table_id, days_back):
    client = bigquery.Client(project=project_id)
    query = f"""
    SELECT * FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE timestamp_br >= DATETIME_SUB(CURRENT_DATETIME("America/Sao_Paulo"), INTERVAL {days_back} DAY)
    ORDER BY timestamp_br
    """
    df = client.query(query).result().to_dataframe(create_bqstorage_client=True)
    if df.empty:
        raise Exception("❌ Dados do BigQuery vieram vazios!")
    for c in df.columns:
        if df[c].dtype.kind in ("i", "f"):
            df[c] = df[c].apply(to_float_or_nan)
    df["timestamp_br"] = pd.to_datetime(df["timestamp_br"])
    df = make_time_features(df, "timestamp_br")
    return df

def get_or_download_data(csv_path, project_id, dataset_id, table_id, days_back):
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        if "timestamp_br" in df.columns:
            df["timestamp_br"] = pd.to_datetime(df["timestamp_br"])
        return df
    else:
        df = load_data_bq(project_id, dataset_id, table_id, days_back)
        df.to_csv(csv_path, index=False)
        return df

def sanitize_hourly_and_interpolate(df, cols_keep, ts_col="timestamp_br", interp_limit_hours=6):
    cols = [c for c in cols_keep if c in df.columns]
    out = df[[ts_col] + cols].copy().sort_values(ts_col).drop_duplicates(subset=[ts_col]).set_index(ts_col)
    full_idx = pd.date_range(out.index.min().floor("H"), out.index.max().ceil("H"), freq="H")
    out = out.reindex(full_idx)
    num_cols = out.select_dtypes(include=[np.number]).columns.tolist()
    if num_cols:
        out[num_cols] = out[num_cols].interpolate(method="time", limit=interp_limit_hours, limit_direction="both")
    out = out.reset_index().rename(columns={"index": ts_col})
    return out

# ==== GA e Métricas ====
def guess_target_col(df, depth):
    for c in [f"intensidade_{depth}_ajustada", f"intensidade_{depth}"]:
        if c in df.columns and df[c].notna().sum() > 10:
            return c
    return None

def build_matrix(df, target_col, exog_cols, lags):
    cols_to_lag = sorted(set(exog_cols + [target_col]))
    df2 = add_lags(df.copy(), cols_to_lag, lags)
    feat_cols = [f"{c}_L{L}" for c in cols_to_lag for L in lags if f"{c}_L{L}" in df2.columns]
    df2 = df2.dropna(subset=feat_cols + [target_col]).reset_index(drop=True)
    X = df2[feat_cols].values if feat_cols else np.empty((0, 0))
    y = df2[target_col].values if feat_cols else np.empty((0,))
    ts = df2["timestamp_br"] if "timestamp_br" in df2.columns else pd.Series(dtype="datetime64[ns]")
    return X, y, feat_cols, ts

def standardize_fit(X):
    mu = np.nanmean(X, axis=0)
    sigma = np.nanstd(X, axis=0)
    sigma[sigma == 0] = 1.0
    return (X - mu) / sigma, mu, sigma

def standardize_apply(X, mu, sigma):
    sigma = np.where(sigma == 0, 1.0, sigma)
    return (X - mu) / sigma

def predict_with_genome(X, genome):
    w = np.array(genome[:-1], dtype=float)
    b = float(genome[-1])
    return X @ w + b

def rmse(y_true, y_pred): return float(np.sqrt(np.nanmean((np.array(y_true) - np.array(y_pred))**2)))
def mae(y_true, y_pred): return float(np.nanmean(np.abs(np.array(y_true) - np.array(y_pred))))
def mape(y_true, y_pred, eps=1e-6):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    denom = np.maximum(np.abs(y_true), eps)
    return float(np.nanmean(np.abs(y_true - y_pred) / denom) * 100)
def smape(y_true, y_pred, eps=1e-6):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    denom = np.abs(y_true) + np.abs(y_pred) + eps
    return float(100 * np.nanmean(np.abs(y_true - y_pred) / denom))
def sign_accuracy(y_true, y_pred):
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    mask = ~np.isnan(y_true) & ~np.isnan(y_pred)
    return float((np.sign(y_true[mask]) == np.sign(y_pred[mask])).mean() * 100) if mask.any() else np.nan
def sign_penalty_rmse(y_true, y_pred):
    penalty = 2.0  # penalização inversão de sinal
    errors = np.array(y_true) - np.array(y_pred)
    signs = np.sign(y_true) != np.sign(y_pred)
    errors[signs] *= penalty
    return float(np.sqrt(np.nanmean(errors**2)))

def train_ga(X_train, y_train, pop_size=200, generations=60):
    n_feats = X_train.shape[1]
    pop = [np.random.uniform(-2, 2, n_feats+1) for _ in range(pop_size)]
    for _ in range(generations):
        fitness = np.array([sign_penalty_rmse(y_train, predict_with_genome(X_train, g)) for g in pop])
        idx = np.argsort(fitness)
        pop = [pop[i] for i in idx[:pop_size//4]]
        while len(pop) < pop_size:
            if np.random.rand() < 0.5:
                parent = pop[np.random.randint(max(1, len(pop)//2))]
                child  = parent + np.random.normal(0, 0.15, n_feats+1)
            else:
                p1, p2 = random.sample(pop[:max(2, len(pop))], 2)
                alpha  = np.random.rand()
                child  = alpha*p1 + (1-alpha)*p2
            pop.append(child)
    fitness = np.array([sign_penalty_rmse(y_train, predict_with_genome(X_train, g)) for g in pop])
    best_idx = int(np.argmin(fitness))
    return pop[best_idx], float(fitness[best_idx])

def linear_calibration(y_true, y_hat):
    mask = ~np.isnan(y_true) & ~np.isnan(y_hat)
    if mask.sum() < 10:
        return 1.0, 0.0  # sem ajuste
    a, b = np.polyfit(y_hat[mask], y_true[mask], 1)
    return float(a), float(b)

def feature_importance(genome, feat_cols):
    absw = np.abs(np.array(genome[:-1]))
    order = np.argsort(-absw)
    return [(feat_cols[i], float(absw[i])) for i in order[:min(10, len(feat_cols))]]

def recursive_backtest(df_hourly, target_col, feat_cols, lags, genome, mu, sigma, start_time, end_time, ts_col="timestamp_br", calib=(1.0,0.0)):
    df = df_hourly[[ts_col, target_col] + sorted(set([c.rsplit("_L",1)[0] for c in feat_cols]))].copy()
    df = df[(df[ts_col] >= start_time) & (df[ts_col] <= end_time)].copy()
    df = df.sort_values(ts_col).reset_index(drop=True)
    max_lag = max(lags) if lags else 1
    df_full = df_hourly[[ts_col, target_col] + [c for c in df.columns if c not in (ts_col, target_col)]].copy().sort_values(ts_col).reset_index(drop=True)
    try:
        start_idx = int(df_full.index[df_full[ts_col] == start_time][0])
    except IndexError:
        idxs = df_full.index[df_full[ts_col] >= start_time]
        if len(idxs) == 0:
            return pd.DataFrame(columns=[ts_col, "y_true", "y_hat_raw", "y_hat_cal"]), {}
        start_idx = int(idxs[0])
    work_start = max(0, start_idx - max_lag - 1)
    work_end = int(df_full.index[df_full[ts_col] == end_time][0]) if any(df_full[ts_col] == end_time) else df_full.index[-1]
    roll = df_full.iloc[work_start: start_idx].copy()
    preds_rows = []
    for i in range(start_idx, work_end + 1):
        cur_row = df_full.iloc[i].copy()
        cur_ts = pd.to_datetime(cur_row[ts_col])
        bases = sorted(set([fc.rsplit("_L",1)[0] for fc in feat_cols]))
        for b in bases:
            if b not in roll.columns:
                roll[b] = np.nan
        roll_lagged = add_lags(roll, bases, lags)
        last = roll_lagged.iloc[-1] if len(roll_lagged) else pd.Series(dtype=float)
        X_row = np.array([last.get(fc, np.nan) for fc in feat_cols], dtype=float).reshape(1, -1)
        if np.isnan(X_row).any():
            new_row = cur_row[bases].copy()
            new_row[target_col] = cur_row[target_col]
            new_row[ts_col] = cur_ts
            roll = pd.concat([roll, new_row.to_frame().T], ignore_index=True)
            continue
        X_std = standardize_apply(X_row, mu, sigma)
        y_hat_raw = float(predict_with_genome(X_std, genome))
        a, b = calib
        y_hat_cal = a * y_hat_raw + b
        y_true = cur_row[target_col] if pd.notna(cur_row[target_col]) else np.nan
        preds_rows.append({
            ts_col: cur_ts, "y_true": y_true, "y_hat_raw": y_hat_raw, "y_hat_cal": y_hat_cal,
            "sign_true": np.sign(y_true), "sign_hat_cal": np.sign(y_hat_cal),
            "erro_raw": y_true - y_hat_raw, "erro_cal": y_true - y_hat_cal
        })
        new_row = cur_row[bases].copy()
        new_row[target_col] = y_hat_raw
        new_row[ts_col] = cur_ts
        roll = pd.concat([roll, new_row.to_frame().T], ignore_index=True)
    df_pred = pd.DataFrame(preds_rows).dropna(subset=["y_true", "y_hat_raw"])
    if df_pred.empty:
        return df_pred, {}
    metrics = {
        "rmse": rmse(df_pred["y_true"], df_pred["y_hat_cal"]),
        "mae": mae(df_pred["y_true"], df_pred["y_hat_cal"]),
        "mape": mape(df_pred["y_true"], df_pred["y_hat_cal"]),
        "smape": smape(df_pred["y_true"], df_pred["y_hat_cal"]),
        "sign_acc": sign_accuracy(df_pred["y_true"], df_pred["y_hat_cal"]),
        "n": int(len(df_pred))
    }
    return df_pred, metrics

# ==== Loop por profundidade ====
def train_depth_forever(depth, df_raw):
    while True:
        try:
            print(f"\n[{datetime.now()}] [THREAD-{depth}] Iniciando treino...")
            tgt_col = guess_target_col(df_raw, depth)
            if not tgt_col:
                print(f"[THREAD-{depth}] Sem coluna alvo válida. Aguardando..."); time.sleep(SLEEP_BETWEEN_TRAIN); continue
            # Colunas obrigatórias físicas sempre presentes
            cols_needed = sorted(set([tgt_col] + EXOG_CANDIDATES + [f"d_{col}" for col in EXOG_PHYS_FORCED]))
            df_sub = sanitize_hourly_and_interpolate(df_raw, cols_keep=cols_needed, ts_col="timestamp_br", interp_limit_hours=INTERP_LIMIT_HOURS)
            df_sub = add_physical_deltas(df_sub)
            # Exógenas densas + forçadas
            exog_cols_this = sorted(set(pick_existing_and_dense(df_sub, EXOG_CANDIDATES, min_notna_frac=MIN_FEATURE_DENSITY) + [col for col in EXOG_PHYS_FORCED if col in df_sub.columns] + [f"d_{col}" for col in EXOG_PHYS_FORCED if f"d_{col}" in df_sub.columns]))
            X, y, feat_cols, ts_used = build_matrix(df_sub, tgt_col, exog_cols_this, LAGS)
            if len(X) < MIN_ROWS_TO_TRAIN or len(feat_cols) == 0:
                print(f"[THREAD-{depth}] Poucos dados ou sem features (linhas válidas={len(X)}). Aguardando..."); time.sleep(SLEEP_BETWEEN_TRAIN); continue
            n = len(X); split = int(n * 0.85)
            X_train, y_train, X_val, y_val = X[:split], y[:split], X[split:], y[split:]
            X_train_std, mu, sigma = standardize_fit(X_train)
            X_val_std = standardize_apply(X_val, mu, sigma)
            best_genome, train_rmse = train_ga(X_train_std, y_train, pop_size=POP_SIZE, generations=GENERATIONS)
            y_val_raw = predict_with_genome(X_val_std, best_genome)
            # Calibração linear pós treino
            a, b = linear_calibration(y_val, y_val_raw)
            y_val_cal = a * y_val_raw + b
            val_rmse = rmse(y_val, y_val_cal)
            val_smape = smape(y_val, y_val_cal)
            val_signacc = sign_accuracy(y_val, y_val_cal)
            # Backtest recursivo calibrado
            end_time = df_sub["timestamp_br"].max().floor("H")
            start_time = end_time - pd.Timedelta(days=TEST_LOOKBACK_DAYS) + pd.Timedelta(hours=1)
            start_time = max(start_time, df_sub["timestamp_br"].min())
            df_bt, bt_metrics = recursive_backtest(df_hourly=df_sub, target_col=tgt_col, feat_cols=feat_cols, lags=LAGS, genome=np.array(best_genome, dtype=float), mu=np.array(mu, dtype=float), sigma=np.array(sigma, dtype=float), start_time=start_time, end_time=end_time, ts_col="timestamp_br", calib=(a,b))
            # Top 10 features
            top_feats = feature_importance(best_genome, feat_cols)
            print(f"[THREAD-{depth}] RMSE(tr): {train_rmse:.4f} | RMSE(val): {val_rmse:.4f} | BT: rmse={bt_metrics.get('rmse',np.nan):.4f}, smape={bt_metrics.get('smape',np.nan):.2f}%, sign_acc={bt_metrics.get('sign_acc',np.nan):.2f}% n={bt_metrics.get('n',0)}")
            print(f"[THREAD-{depth}] Top10 features: {[f'{f[0]}={f[1]:.3g}' for f in top_feats]}")
            # Salva DNA se melhorou RMSE de validação
            os.makedirs(BEST_DNA_DIR, exist_ok=True)
            out_path = os.path.join(BEST_DNA_DIR, f"{depth}_intensity.json")
            can_save = True; old_rmse = None
            if os.path.exists(out_path):
                try:
                    with open(out_path, "r", encoding="utf-8") as f:
                        old = json.load(f)
                    old_rmse = float(old.get("rmse_val", 9999))
                    if val_rmse >= old_rmse: can_save = False
                except Exception: pass
            dna = {
                "genome": list(map(float, best_genome)), "lags": LAGS, "mu": list(map(float, mu)),
                "sigma": list(map(float, sigma)), "rmse_tr": float(train_rmse), "rmse_val": float(val_rmse),
                "smape_val": float(val_smape), "signacc_val": float(val_signacc), "calibration": (a, b),
                "feat_cols": feat_cols, "depth": depth, "target_col": tgt_col, "created_at": datetime.utcnow().isoformat(" "),
                "top10_feat_importance": top_feats,
                "backtest": {**bt_metrics, "start_time": str(start_time), "end_time": str(end_time)}
            }
            if can_save:
                with open(out_path, "w", encoding="utf-8") as f: json.dump(dna, f, ensure_ascii=False, indent=2)
                print(f"[THREAD-{depth}] ✅ Novo DNA salvo! (RMSE(val): {val_rmse:.4f} | antigo: {old_rmse})")
                bt_csv = os.path.join(BEST_DNA_DIR, f"{depth}_backtest.csv")
                if not df_bt.empty:
                    df_bt.to_csv(bt_csv, index=False)
                    print(f"[THREAD-{depth}] Backtest salvo em: {bt_csv}")
            else:
                print(f"[THREAD-{depth}] Mantido DNA anterior (melhor).")
        except Exception as ex:
            print(f"[THREAD-{depth}] ERRO: {ex}")
        time.sleep(SLEEP_BETWEEN_TRAIN)

def main():
    df_raw = get_or_download_data(
        csv_path=CSV_PATH,
        project_id=PROJECT_ID,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        days_back=TRAIN_DAYS
    )
    # <<<<<< FILTRO PARA IGNORAR LINHAS SEM DADO REAL >>>>>>
    if "altura_real_getmare" in df_raw.columns:
        df_raw = df_raw[df_raw["altura_real_getmare"].notna()]
    # <<<<<<----------------------------------------->>>>>>>
    depths = [x.strip() for x in DEPTHS_CSV.split(",") if x.strip()]
    threads = []
    for depth in depths:
        t = threading.Thread(target=train_depth_forever, args=(depth, df_raw), daemon=True)
        t.start(); threads.append(t)
    print(f"Iniciadas {len(threads)} threads para as profundidades: {depths}")
    try:
        while True: time.sleep(600)
    except KeyboardInterrupt: print("Encerrando...")


if __name__ == "__main__":
    main()
