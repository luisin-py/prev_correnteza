# %% [markdown]
# # Previsão de Correnteza V5 — Retroalimentação Recursiva até +6h
# Esta versão herda toda a estrutura de treinamento da V4 e modifica apenas a avaliação.
#
# **Arquitetura de Inferência Recursiva:**
# - Round 1: Dados reais → prevê t+1, t+2
# - Round 2: Lags de correnteza atualizados pelas previsões t+1/t+2 + meteo real t+3/t+4 → prevê t+3, t+4
# - Round 3: Lags atualizados com t+3/t+4 previstos + meteo real t+5/t+6 → prevê t+5, t+6
# - Métricas comparadas por horizonte: +1h até +6h (mede degradação do efeito "bola de neve")

# %%
import os, time, random, warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
from datetime import date
from google.oauth2 import service_account
from pandas_gbq import read_gbq
import joblib

from sklearn.ensemble import HistGradientBoostingRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler

import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, BatchNormalization, LSTM, Input, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping

warnings.filterwarnings('ignore')
random.seed(42)
np.random.seed(42)
tf.random.set_seed(42)

OUTPUT_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V5"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# %% [markdown]
# ## 1. Dados de Chuva Externos — Open-Meteo (Horário)

# %%
CIDADES = [
    ("PORTO_ALEGRE",    -30.0324999, -51.2303767),
    ("CANOAS",          -29.9216045, -51.1799525),
    ("SAO_LEOPOLDO",    -29.7544405, -51.1516497),
    ("NOVO_HAMBURGO",   -29.6905705, -51.1429035),
    ("GRAVATAI",        -29.9440222, -50.9930938),
    ("SANTA_MARIA",     -29.6860512, -53.8069214),
    ("CACHOEIRA_SUL",   -30.0482234, -52.8901686),
    ("SANTA_CRUZ_SUL",  -29.714209,  -52.4285807),
    ("RIO_GRANDE",      -32.035,     -52.0986),
]

def fetch_rain_hourly(lat, lon, start: date, end: date, pause=1.5) -> pd.DataFrame:
    dfs = []
    cur = start
    while cur < end:
        blk_end = min(date(cur.year, 12, 31), end)
        try:
            r = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={"latitude": lat, "longitude": lon,
                        "start_date": cur.isoformat(), "end_date": blk_end.isoformat(),
                        "hourly": "precipitation", "timezone": "America/Sao_Paulo"},
                timeout=60
            )
            r.raise_for_status()
            hourly = r.json().get("hourly", {})
            if hourly.get("time"):
                dfs.append(pd.DataFrame(hourly))
            time.sleep(pause)
        except Exception as e:
            print(f"  Erro em {cur}–{blk_end}: {e}")
        cur = date(cur.year + 1, 1, 1)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

START_RAIN, END_RAIN = date(2020, 1, 1), date.today()

print("=== Baixando dados de chuva horários da Open-Meteo ===")
rain_list = []
for nome, lat, lon in CIDADES:
    print(f"  > {nome}...")
    df_r = fetch_rain_hourly(lat, lon, START_RAIN, END_RAIN)
    if not df_r.empty:
        df_r["time"] = pd.to_datetime(df_r["time"])
        df_r = df_r.rename(columns={"precipitation": f"rain_{nome}"})
        rain_list.append(df_r.set_index("time")[[f"rain_{nome}"]])

if rain_list:
    df_rain = rain_list[0]
    for extra in rain_list[1:]:
        df_rain = df_rain.join(extra, how="outer")
    df_rain = df_rain.reset_index().rename(columns={"time": "datahora"})
    df_rain["datahora"] = pd.to_datetime(df_rain["datahora"])
    print(f"Chuva carregada. Shape: {df_rain.shape}")
else:
    df_rain = pd.DataFrame()
    print("Nenhum dado de chuva obtido.")

# %% [markdown]
# ## 2. Autenticação e Extração BigQuery

# %%
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID, QUERY_TABLE = "local-bliss-359814", "ML.xtrain_horario_t_2026"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
query = f"SELECT * FROM `{PROJECT_ID}.{QUERY_TABLE}`"

print("Extraindo dados do BigQuery...")
df_bq = read_gbq(query, project_id=PROJECT_ID, credentials=credentials, dialect="standard")
df_bq["datahora"] = pd.to_datetime(df_bq["datahora"])
print(f"BigQuery carregado. Shape: {df_bq.shape}")

# %% [markdown]
# ## 3. Merge BigQuery + Chuva Open-Meteo

# %%
if not df_rain.empty:
    df_bq["datahora_h"] = df_bq["datahora"].dt.floor("H")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("H")
    df_merged = df_bq.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_merged = df_merged.drop(columns=["datahora_h"])
else:
    df_merged = df_bq.copy()

df_merged = df_merged.sort_values("datahora").reset_index(drop=True)
cols_drop = ["temp_out"] + [c for c in df_merged.columns if "wind_dir" in c]
df_merged.drop(columns=[c for c in cols_drop if c in df_merged.columns], inplace=True)
cols_conv = df_merged.columns.drop("datahora", errors="ignore")
df_merged[cols_conv] = df_merged[cols_conv].apply(pd.to_numeric, errors="coerce").fillna(0).astype("float32")

# %% [markdown]
# ## 4. Transformação Direcional → Intensidade Sinalizada (+Enchente / -Vazante)

# %%
for int_col, dir_col in [("intensidade_6m_kt","direcao_6m_deg"),
                          ("intensidade_superficie_kt","direcao_superficie_deg"),
                          ("intensidade_3m_kt","direcao_3m_deg")]:
    if int_col in df_merged.columns and dir_col in df_merged.columns:
        mask = (df_merged[dir_col] > 90) & (df_merged[dir_col] < 270)
        df_merged.loc[mask, int_col] *= -1

TARGET_BASE = ["intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt"]
y_base_cols = [c for c in TARGET_BASE if c in df_merged.columns]
df_merged = df_merged[df_merged[y_base_cols].sum(axis=1) != 0].reset_index(drop=True)
print(f"Após filtro de zero-intensidade: {df_merged.shape}")

# %% [markdown]
# ## 5. Feature Engineering (Lags + MAs)

# %%
base_cols = [c for c in df_merged.columns if c != "datahora"]
rain_feat_cols = [c for c in base_cols if c.startswith("rain_")]

X_parts = []
for lag in range(1, 6):
    Xl = df_merged[base_cols].shift(lag)
    Xl.columns = [f"{c}_lag{lag}" for c in base_cols]
    X_parts.append(Xl)
for w in [3, 6]:
    Xm = df_merged[base_cols].rolling(w).mean().shift(1)
    Xm.columns = [f"{c}_ma{w}" for c in base_cols]
    X_parts.append(Xm)
for w in [12, 24, 48]:
    if rain_feat_cols:
        Xr = df_merged[rain_feat_cols].rolling(w).mean().shift(1)
        Xr.columns = [f"{c}_ma{w}" for c in rain_feat_cols]
        X_parts.append(Xr)

X_o = pd.concat(X_parts, axis=1)

# %% [markdown]
# ## 6. Targets Multi-Step (t+1 e t+2)

# %%
y_parts = {}
for col in y_base_cols:
    y_parts[f"{col}_h1"] = df_merged[col].shift(-1)
    y_parts[f"{col}_h2"] = df_merged[col].shift(-2)

y_multi = pd.DataFrame(y_parts)
y_cols = list(y_multi.columns)

data_final = pd.concat([X_o, y_multi], axis=1).dropna()
valid_idx = data_final.index
X_clean = data_final[X_o.columns]
y_clean = data_final[y_cols]

data_final.to_csv(os.path.join(OUTPUT_DIR, "base_treinamento_v5.csv"), index=False)
print(f"Dataset final: X={X_clean.shape}  y={y_clean.shape}")

# %% [markdown]
# ## 7. Divisão Temporal: 80% Treino | 10% Validação | 10% Teste

# %%
N = len(X_clean)
i_val, i_test = int(N * 0.80), int(N * 0.90)

trainX, valX, testX = X_clean.iloc[:i_val], X_clean.iloc[i_val:i_test], X_clean.iloc[i_test:]
trainY, valY, testY = y_clean.iloc[:i_val], y_clean.iloc[i_val:i_test], y_clean.iloc[i_test:]

test_dates = df_merged.loc[valid_idx[i_test:], "datahora"].values
val_dates  = df_merged.loc[valid_idx[i_val:i_test], "datahora"].values

scaler_X, scaler_y = MinMaxScaler(), MinMaxScaler()
trainX_n = scaler_X.fit_transform(trainX)
valX_n   = scaler_X.transform(valX)
testX_n  = scaler_X.transform(testX)
trainY_n = scaler_y.fit_transform(trainY)
valY_n   = scaler_y.transform(valY)
testY_n  = scaler_y.transform(testY)
testY_real, valY_real = testY.values, valY.values

print(f"Treino:{trainX.shape} Val:{valX.shape} Teste:{testX.shape}")

# %% [markdown]
# ## 8 a 10. Treinamento — Igual à V4 (Árvores, MLP, LSTM)

# %%
# --- Modelos de Árvore (otimizados para velocidade) ---
# HistGradientBoosting: usa histogramas + suporte nativo multi-output (elimina MultiOutputRegressor)
# XGBoost: tree_method='hist' para árvores baseadas em histograma
# LightGBM já é rápido por padrão
# CatBoost: threads explícitos para evitar overhead
tree_models = {
    "Gradient Boosting": HistGradientBoostingRegressor(random_state=42, max_iter=300,
                             learning_rate=0.05, max_leaf_nodes=31,
                             early_stopping=True, validation_fraction=0.1,
                             n_iter_no_change=20, verbose=0),
    "XGBoost":           MultiOutputRegressor(XGBRegressor(random_state=42, n_jobs=-1,
                             tree_method="hist", n_estimators=500,
                             learning_rate=0.05, max_depth=6,
                             early_stopping_rounds=20, eval_metric="rmse"), n_jobs=-1),
    "LightGBM":          MultiOutputRegressor(LGBMRegressor(random_state=42, n_jobs=-1,
                             verbose=-1, n_estimators=500, learning_rate=0.05,
                             num_leaves=31), n_jobs=-1),
    "CatBoost":          MultiOutputRegressor(CatBoostRegressor(random_state=42, verbose=0,
                             iterations=500, learning_rate=0.05, depth=6,
                             thread_count=-1), n_jobs=1),
}

for name, model in tree_models.items():
    print(f">> Treinando {name}...")
    t0 = time.time()
    if name == "Gradient Boosting":
        # HistGB suporta multi-output nativo
        from sklearn.multioutput import MultiOutputRegressor as _MOR
        model = _MOR(model, n_jobs=-1)
        model.fit(trainX_n, trainY_n)
    elif name == "XGBoost":
        # XGBoost com early stopping requer eval_set por estimator interno
        from sklearn.multioutput import MultiOutputRegressor as _MOR
        # Usa fit sem early stopping para compatibilidade com MultiOutputRegressor
        xgb_model = MultiOutputRegressor(XGBRegressor(random_state=42, n_jobs=-1,
                        tree_method="hist", n_estimators=500, learning_rate=0.05,
                        max_depth=6), n_jobs=-1)
        xgb_model.fit(trainX_n, trainY_n)
        model = xgb_model
    else:
        model.fit(trainX_n, trainY_n)
    tree_models[name] = model
    print(f"   Concluido em {time.time()-t0:.1f}s")
    joblib.dump(model, os.path.join(OUTPUT_DIR, f"modelo_{name.replace(' ','_')}.joblib"))

def build_mlp(in_dim, out_dim):
    m = Sequential([Input(shape=(in_dim,)),
        Dense(128, activation="relu"), BatchNormalization(),
        Dense(64,  activation="relu"), BatchNormalization(),
        Dense(32,  activation="relu"),
        Dense(out_dim, activation="linear")])
    m.compile(optimizer=Adam(1e-3), loss="mse", metrics=["mae"])
    return m

print("\n>> Treinando MLP...")
t0 = time.time()
mlp = build_mlp(trainX_n.shape[1], len(y_cols))
mlp.fit(trainX_n, trainY_n, validation_data=(valX_n, valY_n),
        epochs=10000, batch_size=256, verbose=0,
        callbacks=[EarlyStopping(monitor="val_loss", patience=50,
                                 restore_best_weights=True, verbose=1)])
print(f"   MLP concluido em {time.time()-t0:.1f}s")
mlp.save(os.path.join(OUTPUT_DIR, "modelo_MLP.keras"))

TIMESTEPS = 6
base_arr  = df_merged[base_cols].values.astype("float32")
scaler_base = MinMaxScaler()
scaler_base.fit(base_arr[:valid_idx[i_val - 1] + 1])
base_norm = scaler_base.transform(base_arr)

def make_3d(ids, arr, t): return np.array([arr[i - t : i] for i in ids])

trainX_3d = make_3d(valid_idx[:i_val],       base_norm, TIMESTEPS)
valX_3d   = make_3d(valid_idx[i_val:i_test], base_norm, TIMESTEPS)
testX_3d  = make_3d(valid_idx[i_test:],      base_norm, TIMESTEPS)

def build_lstm(t, feats, out):
    m = Sequential([Input(shape=(t, feats)),
        LSTM(64, activation="tanh"), Dropout(0.2),
        Dense(32, activation="relu"),
        Dense(out, activation="linear")])
    m.compile(optimizer=Adam(1e-3), loss="mse", metrics=["mae"])
    return m

print("\n>> Treinando LSTM_V5...")
t0 = time.time()
lstm = build_lstm(TIMESTEPS, len(base_cols), len(y_cols))
lstm.fit(trainX_3d, trainY_n, validation_data=(valX_3d, valY_n),
         epochs=10000, batch_size=256, verbose=0,
         callbacks=[EarlyStopping(monitor="val_loss", patience=50,
                                  restore_best_weights=True, verbose=1)])
print(f"   LSTM concluido em {time.time()-t0:.1f}s")
lstm.save(os.path.join(OUTPUT_DIR, "modelo_LSTM_V5.keras"))

# Salva scalers para reutilização sem retreinamento
joblib.dump(scaler_X,    os.path.join(OUTPUT_DIR, "scaler_X.joblib"))
joblib.dump(scaler_y,    os.path.join(OUTPUT_DIR, "scaler_y.joblib"))
joblib.dump(scaler_base, os.path.join(OUTPUT_DIR, "scaler_base.joblib"))
print("Scalers salvos em disco.")

# %% [markdown]
# ## 11. Walk-Forward Recursivo até +6h — Motor de Retroalimentação
#
# **Algoritmo:**
# 1. Com dados reais no instante t, prevê t+1, t+2
# 2. Atualiza lags de CORRENTEZA com os valores previstos (t+1, t+2 → lag1/lag2/ma3/ma6)
# 3. Usa meteo REAL de t+3 a t+6 (simula previsão meteorológica disponível)
# 4. Prevê t+3, t+4 com os dados híbridos (correnteza prevista + meteo real)
# 5. Repete para t+5, t+6
# 6. Calcula acurácia separada para cada horizonte (1h até 6h)

# %%
# Identifica colunas de correnteza (intensidade) vs colunas meteo (chuva, vento, etc.)
corr_base_cols = [c for c in y_base_cols]   # ex: ['intensidade_6m_kt', ...]
meteo_cols     = [c for c in base_cols if c not in corr_base_cols]

# Nomes das colunas de lag e MA criadas para as variáveis de correnteza no X_clean
corr_lag_cols = [f"{c}_lag{lag}" for c in corr_base_cols for lag in range(1, 6)]
corr_ma_cols  = [f"{c}_ma{w}" for c in corr_base_cols for w in [3, 6]]

import traceback as _tb

def predict_2h(model_name, model, X_row_norm):
    """Prevê os próximos 2 horizontes para uma linha de features normalizada."""
    X_2d = X_row_norm.reshape(1, -1)     # (1, n_features)
    if model_name == "LSTM_V5":
        # Reconstri janela 3D usando apenas as features base (sem lags)
        base_feats = scaler_X.inverse_transform(X_2d)[:, :len(base_cols)]  # (1, base_cols)
        base_feats_n = scaler_base.transform(base_feats)                    # (1, base_cols)
        X_3d = np.repeat(base_feats_n[:, np.newaxis, :], 1, axis=1)        # (1, 1, base_cols)
        pred_n = model.predict(X_3d, verbose=0)                              # (1, n_targets)
    else:
        raw = model.predict(X_2d)                    # sklearn retorna (1, n_targets) ou (n_targets,)
        pred_n = np.atleast_2d(raw)                  # garante (1, n_targets)
    return scaler_y.inverse_transform(pred_n)[0]     # (n_targets,)


def recursive_walk_forward(model_name, model, X_all, y_all, dates_all,
                            df_full, valid_ids_all, step=2):
    """
    Roda o motor recursivo de 3 rounds (6 horas totais) para cada janela de avaliação.
    - X_all, y_all: features e targets não normalizados
    - df_full: DataFrame original com os dados base (para buscar meteo real futuro)
    - valid_ids_all: índices no df_full para cada linha de X_all
    """
    records = []

    # Itera em blocos de 6 horas (3 chamadas ao modelo, cada uma retorna 2h)
    for start in range(0, len(X_all) - 6, 6):
        base_row_idx = valid_ids_all[start]   # índice no df_full para este ponto de partida

        # === ROUND 1: Dados Reais → Prevê t+1, t+2 ===
        X_r1 = X_all[start].copy()            # features reais no instante t
        X_r1_n = scaler_X.transform(X_r1.reshape(1, -1))
        pred_r1 = predict_2h(model_name, model, X_r1_n)  # (6,): [int6m_h1, intSup_h1, int3m_h1, int6m_h2, ...]
        
        y_real_block = y_all[start : start + 6]   # reais das 6h

        # Registra h+1 e h+2
        n_base = len(y_base_cols)
        for h_idx, h_label in enumerate(["+1h", "+2h"]):
            rec = {"datahora": dates_all[start + h_idx], "horizonte": h_label}
            # y_cols layout: [d0_h1, d1_h1, d2_h1, d0_h2, d1_h2, d2_h2]
            # pred layout:   [d0_h1, d1_h1, d2_h1, d0_h2, d1_h2, d2_h2]
            for i, base_col in enumerate(y_base_cols):
                h1_col = f"{base_col}_h1"
                h2_col = f"{base_col}_h2"
                # real: y_real_block rows = time steps; cols follow y_cols order
                j_h1 = y_cols.index(h1_col)
                j_h2 = y_cols.index(h2_col)
                rec[f"real_{h1_col}"] = y_real_block[h_idx, j_h1] if h_idx < len(y_real_block) else np.nan
                rec[f"real_{h2_col}"] = y_real_block[h_idx, j_h2] if h_idx < len(y_real_block) else np.nan
                rec[f"pred_{h1_col}"] = pred_r1[i]
                rec[f"pred_{h2_col}"] = pred_r1[i + n_base]
            records.append(rec)

        # === ROUND 2: Injeção recursiva → Prevê t+3, t+4 ===
        # Pegar features do step t+2 como base (meteo real t+3/t+4)
        if start + 2 < len(X_all) and base_row_idx + 2 < len(df_full):
            X_r2 = X_all[start + 2].copy()   # features reais do t+2

            # Atualizar lags das intensidades com valores PREVISTOS de t+1 e t+2
            # lag1 no t+2 = valor previsto para t+1 (o passado mais recente deve ser nossa previsão)
            for i, col in enumerate(y_base_cols):
                h1_pred = pred_r1[i]           # previsão t+1 para esta profundidade
                h2_pred = pred_r1[i + len(y_base_cols)]  # previsão t+2

                lag1_col = f"{col}_lag1"
                lag2_col = f"{col}_lag2"
                ma3_col  = f"{col}_ma3"
                ma6_col  = f"{col}_ma6"

                col_idx_lag1 = X_clean.columns.get_loc(lag1_col) if lag1_col in X_clean.columns else None
                col_idx_lag2 = X_clean.columns.get_loc(lag2_col) if lag2_col in X_clean.columns else None

                if col_idx_lag1 is not None:
                    X_r2[col_idx_lag1] = h1_pred   # lag1 = previsto t+1
                if col_idx_lag2 is not None:
                    X_r2[col_idx_lag2] = h2_pred   # lag2 = previsto t+2

                # Recalcula ma3 e ma6 usando a média entre os previstos + lags mais antigos do real
                for ma_w, ma_col in [(3, ma3_col), (6, ma6_col)]:
                    ma_idx = X_clean.columns.get_loc(ma_col) if ma_col in X_clean.columns else None
                    if ma_idx is not None:
                        # Aproximação: média simples dos 2 previstos + passado real
                        hist_lags = [X_r2[X_clean.columns.get_loc(f"{col}_lag{l}")]
                                     for l in range(3, min(ma_w + 1, 6))
                                     if f"{col}_lag{l}" in X_clean.columns]
                        window_vals = [h1_pred, h2_pred] + hist_lags
                        X_r2[ma_idx] = np.mean(window_vals[:ma_w])

            X_r2_n = scaler_X.transform(X_r2.reshape(1, -1))
            pred_r2 = predict_2h(model_name, model, X_r2_n)

            for h_idx, h_label in enumerate(["+3h", "+4h"]):
                real_global_idx = h_idx + 2
                rec = {"datahora": dates_all[start + real_global_idx] if start + real_global_idx < len(dates_all) else np.nan,
                       "horizonte": h_label}
                for i, base_col in enumerate(y_base_cols):
                    h1_col = f"{base_col}_h1"
                    h2_col = f"{base_col}_h2"
                    j_h1 = y_cols.index(h1_col)
                    j_h2 = y_cols.index(h2_col)
                    rec[f"real_{h1_col}"] = y_real_block[real_global_idx, j_h1] if real_global_idx < len(y_real_block) else np.nan
                    rec[f"real_{h2_col}"] = y_real_block[real_global_idx, j_h2] if real_global_idx < len(y_real_block) else np.nan
                    rec[f"pred_{h1_col}"] = pred_r2[i]
                    rec[f"pred_{h2_col}"] = pred_r2[i + n_base]
                records.append(rec)

            # === ROUND 3: t+3/t+4 previstos → Prevê t+5, t+6 ===
            if start + 4 < len(X_all):
                X_r3 = X_all[start + 4].copy()

                for i, col in enumerate(y_base_cols):
                    h3_pred = pred_r2[i]
                    h4_pred = pred_r2[i + len(y_base_cols)]

                    for lag_n, lag_val in [(1, h3_pred), (2, h4_pred)]:
                        lag_col = f"{col}_lag{lag_n}"
                        if lag_col in X_clean.columns:
                            X_r3[X_clean.columns.get_loc(lag_col)] = lag_val

                    for ma_w, ma_col in [(3, f"{col}_ma3"), (6, f"{col}_ma6")]:
                        if ma_col in X_clean.columns:
                            hist_lags = [X_r3[X_clean.columns.get_loc(f"{col}_lag{l}")]
                                         for l in range(3, min(ma_w + 1, 6))
                                         if f"{col}_lag{l}" in X_clean.columns]
                            window_vals = [h3_pred, h4_pred] + hist_lags
                            X_r3[X_clean.columns.get_loc(ma_col)] = np.mean(window_vals[:ma_w])

                X_r3_n = scaler_X.transform(X_r3.reshape(1, -1))
                pred_r3 = predict_2h(model_name, model, X_r3_n)

                for h_idx, h_label in enumerate(["+5h", "+6h"]):
                    real_global_idx = h_idx + 4
                    rec = {"datahora": dates_all[start + real_global_idx] if start + real_global_idx < len(dates_all) else np.nan,
                           "horizonte": h_label}
                    for i, base_col in enumerate(y_base_cols):
                        h1_col = f"{base_col}_h1"
                        h2_col = f"{base_col}_h2"
                        j_h1 = y_cols.index(h1_col)
                        j_h2 = y_cols.index(h2_col)
                        rec[f"real_{h1_col}"] = y_real_block[real_global_idx, j_h1] if real_global_idx < len(y_real_block) else np.nan
                        rec[f"real_{h2_col}"] = y_real_block[real_global_idx, j_h2] if real_global_idx < len(y_real_block) else np.nan
                        rec[f"pred_{h1_col}"] = pred_r3[i]
                        rec[f"pred_{h2_col}"] = pred_r3[i + n_base]
                    records.append(rec)

    df_out = pd.DataFrame(records)
    df_out["model"] = model_name
    return df_out


# %% [markdown]
# ## 12. Executando Walk-Forward Recursivo para Todos os Modelos

# %%
ALL_X   = np.vstack([valX.values, testX.values])
ALL_Y   = np.vstack([valY_real, testY_real])
ALL_DTS = np.concatenate([val_dates, test_dates])
ALL_IDS = np.array(list(valid_idx[i_val:]))

predictors = {
    **{name: m for name, m in tree_models.items()},
    "MLP":     mlp,
    "LSTM_V5": lstm,
}

print("=== Walk-Forward Recursivo +6h ===")
wf_all = []
for mname, mdl in predictors.items():
    print(f"  > {mname}...")
    try:
        df_wf = recursive_walk_forward(mname, mdl, ALL_X, ALL_Y, ALL_DTS, df_merged, ALL_IDS)
        if df_wf.empty:
            print(f"    AVISO: nenhum registro gerado para {mname}")
        else:
            wf_all.append(df_wf)
    except Exception as e:
        print(f"    ERRO em {mname}: {e}")
        _tb.print_exc()   # mostra traceback completo para diagnóstico

if not wf_all:
    raise RuntimeError("Nenhum modelo gerou previsões. Verifique os erros acima.")

df_wf_all = pd.concat(wf_all, ignore_index=True)
df_wf_all.to_csv(os.path.join(OUTPUT_DIR, "walk_forward_recursive_v5.csv"), index=False)
print(f"Previsões salvas ({len(df_wf_all)} linhas).")

# %% [markdown]
# ## 13. Métricas por Horizonte (+1h a +6h) — Mede Degradação por Retroalimentação

# %%
H_ORDER = ["+1h", "+2h", "+3h", "+4h", "+5h", "+6h"]
summary_rows = []

for mname in df_wf_all["model"].unique():
    for hz in H_ORDER:
        sub = df_wf_all[(df_wf_all["model"] == mname) & (df_wf_all["horizonte"] == hz)]
        if sub.empty: continue
        row = {"Model": mname, "Horizonte": hz}
        for col in y_cols:
            if f"real_{col}" not in sub.columns: continue
            rv = sub[f"real_{col}"].dropna().values
            pv = sub[f"pred_{col}"].dropna().values
            n  = min(len(rv), len(pv))
            if n == 0: continue
            label = col.replace("intensidade_", "Int_").replace("_kt", "")
            row[f"MAE_{label}"]    = round(mean_absolute_error(rv[:n], pv[:n]), 3)
            row[f"RMSE_{label}"]   = round(np.sqrt(mean_squared_error(rv[:n], pv[:n])), 3)
            row[f"R2_{label}"]     = round(r2_score(rv[:n], pv[:n]), 3)
            row[f"Acc05_{label}%"] = round(np.mean(np.abs(rv[:n] - pv[:n]) < 0.5) * 100, 2)
        summary_rows.append(row)

df_summary = pd.DataFrame(summary_rows)
df_summary.to_csv(os.path.join(OUTPUT_DIR, "metricas_recursivas_v5.csv"), index=False)

print("\n=== ACURÁCIA WALK-FORWARD RECURSIVO (Acc<0.5 kt por horizonte) ===")
acc_cols = ["Model", "Horizonte"] + [c for c in df_summary.columns if "Acc05" in c]
print(df_summary[acc_cols].to_string(index=False))

# %% [markdown]
# ## 14. Visualização — Degradação de Acurácia por Horizonte

# %%
# Plot 1: Curva de degradação do Acc<0.5 por horizonte para cada modelo
acc_col_list = [c for c in df_summary.columns if "Acc05" in c]

fig, axes = plt.subplots(len(acc_col_list), 1, figsize=(14, 5 * len(acc_col_list)), sharex=True)
if len(acc_col_list) == 1: axes = [axes]

for ax, acc_c in zip(axes, acc_col_list):
    for mname in df_summary["Model"].unique():
        sub = df_summary[df_summary["Model"] == mname].set_index("Horizonte")
        vals = [sub.loc[h, acc_c] if h in sub.index else np.nan for h in H_ORDER]
        ax.plot(H_ORDER, vals, marker="o", label=mname)
    ax.set_title(f"Degradação da Acurácia (Erro<0.5 kt) — {acc_c}", fontsize=13)
    ax.set_ylabel("Acurácia (%)")
    ax.legend(ncol=3, fontsize=8)
    ax.grid(True, alpha=0.4)
    ax.set_ylim(0, 105)

plt.xlabel("Horizonte de Previsão")
plt.suptitle("V5 — Efeito Bola de Neve: Degradação do Acerto por Horizonte", fontsize=14, y=1.01)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "degradacao_acuracia_v5.png"), dpi=180, bbox_inches="tight")
plt.show()

# Plot 2: Série temporal das previsões vs real para cada profundidade (horizonte +1h e +6h)
formatter = mdates.DateFormatter("%d/%m\n%H:%M")

for depth_col in y_base_cols:
    fig, axes = plt.subplots(2, 1, figsize=(18, 10), sharex=False)
    for ax, hz in zip(axes, ["+1h", "+6h"]):
        target_col = f"{depth_col}_h1" if hz == "+1h" else f"{depth_col}_h2"
        for mname in df_wf_all["model"].unique():
            sub = df_wf_all[(df_wf_all["model"] == mname) & (df_wf_all["horizonte"] == hz)].copy()
            sub = sub.dropna(subset=[f"real_{target_col}", f"pred_{target_col}"])
            sub["datahora"] = pd.to_datetime(sub["datahora"])
            sub = sub.sort_values("datahora").tail(300)
            if mname == df_wf_all["model"].unique()[0]:
                ax.plot(sub["datahora"], sub[f"real_{target_col}"],
                        color="black", linewidth=2.2, label="Real", zorder=10)
            ax.plot(sub["datahora"], sub[f"pred_{target_col}"],
                    linestyle="--", alpha=0.7, label=mname)
        ax.set_title(f"{depth_col} | Horizonte {hz}", fontsize=12)
        ax.legend(ncol=3, fontsize=8, loc="lower left")
        ax.grid(True, alpha=0.4)
        ax.xaxis.set_major_formatter(formatter)
    plt.suptitle(f"Comparativo +1h vs +6h — {depth_col}", fontsize=14)
    plt.tight_layout()
    fname = os.path.join(OUTPUT_DIR, f"comparativo_{depth_col}_v5.png")
    plt.savefig(fname, dpi=180, bbox_inches="tight")
    plt.show()

print("\n✅ Pipeline V5 concluído! Arquivos em:", OUTPUT_DIR)
