# %% [markdown]
# # Previsão de Correnteza V4 — Walk-Forward + Chuva Open-Meteo
# Esta versão incorpora:
# - **Chuva horária (Open-Meteo)** fundida à tabela do BigQuery
# - **Targets duplos (t+1 e t+2)** para horizonte de 2 horas
# - **Walk-Forward Validation** (2 horas de previsão cega, ingere dado real, avança)
# - **Intensidades sinalizadas** (+= Enchente, -= Vazante)
# - Todos os outputs salvos em `modelo_V4/`

# %%
import os, time, random, warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import requests
from datetime import date, timedelta
from google.oauth2 import service_account
from pandas_gbq import read_gbq
import joblib

from sklearn.ensemble import GradientBoostingRegressor
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

OUTPUT_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V4"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# %% [markdown]
# ## 1. Dados de Chuva Externos — Open-Meteo (Horário)
# Baixa `precipitation` horário para 9 cidades do RS e retorna DataFrame pivotado por cidade.

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

# Open-Meteo limita a ~1 ano por chamada; vamos cortar em blocos anuais
def fetch_rain_hourly(lat, lon, start: date, end: date, pause=1.5) -> pd.DataFrame:
    """Retorna DataFrame com colunas ['time', 'precipitation'] para uma localidade."""
    dfs = []
    cur = start
    while cur < end:
        blk_end = min(date(cur.year, 12, 31), end)
        try:
            r = requests.get(
                "https://archive-api.open-meteo.com/v1/archive",
                params={
                    "latitude": lat, "longitude": lon,
                    "start_date": cur.isoformat(), "end_date": blk_end.isoformat(),
                    "hourly": "precipitation",
                    "timezone": "America/Sao_Paulo"
                },
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

START_RAIN = date(2020, 1, 1)
END_RAIN   = date.today()

print("=== Baixando dados de chuva horários da Open-Meteo ===")
rain_list = []
for nome, lat, lon in CIDADES:
    print(f"  > {nome}...")
    df_r = fetch_rain_hourly(lat, lon, START_RAIN, END_RAIN)
    if not df_r.empty:
        df_r["time"] = pd.to_datetime(df_r["time"])
        # Renomeia apenas a coluna de chuva, mantém 'time' como coluna normal
        df_r = df_r.rename(columns={"precipitation": f"rain_{nome}"})
        rain_list.append(df_r.set_index("time")[[f"rain_{nome}"]])

# Pivot: merge sequencial para garantir que o índice 'time' vira coluna 'datahora'
if rain_list:
    df_rain = rain_list[0]
    for extra in rain_list[1:]:
        df_rain = df_rain.join(extra, how="outer")
    df_rain = df_rain.reset_index().rename(columns={"time": "datahora"})
    df_rain["datahora"] = pd.to_datetime(df_rain["datahora"])
    print(f"Chuva carregada. Colunas: {list(df_rain.columns[:5])}... Shape: {df_rain.shape}")
else:
    df_rain = pd.DataFrame()
    print("Nenhum dado de chuva obtido — colunas de chuva estarão ausentes.")

# %% [markdown]
# ## 2. Autenticação e Extração BigQuery

# %%
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID       = "local-bliss-359814"
QUERY_TABLE      = "ML.xtrain_horario_t_2026"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
query = f"SELECT * FROM `{PROJECT_ID}.{QUERY_TABLE}`"

print("Extraindo dados do BigQuery...")
try:
    df_bq = read_gbq(query, project_id=PROJECT_ID, credentials=credentials, dialect="standard")
    df_bq["datahora"] = pd.to_datetime(df_bq["datahora"])
    print(f"BigQuery carregado. Shape: {df_bq.shape}")
except Exception as e:
    print(f"Erro BigQuery: {e}")
    raise

# %% [markdown]
# ## 3. Merge BigQuery + Chuva Open-Meteo

# %%
if not df_rain.empty:
    # Arredonda datahora para hora cheia antes do merge para alinhar granularidades
    df_bq["datahora_h"] = df_bq["datahora"].dt.floor("H")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("H")
    df_merged = df_bq.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_merged = df_merged.drop(columns=["datahora_h"])
else:
    df_merged = df_bq.copy()

df_merged = df_merged.sort_values("datahora").reset_index(drop=True)

# Remover colunas irrelevantes
cols_drop = ["temp_out"] + [c for c in df_merged.columns if "wind_dir" in c]
df_merged.drop(columns=[c for c in cols_drop if c in df_merged.columns], inplace=True)

# Tipos numéricos
cols_conv = df_merged.columns.drop("datahora", errors="ignore")
df_merged[cols_conv] = df_merged[cols_conv].apply(pd.to_numeric, errors="coerce").fillna(0).astype("float32")

# %% [markdown]
# ## 4. Transformação Direcional → Intensidade Sinalizada (+Enchente / -Vazante)

# %%
depths_pairs = [
    ("intensidade_6m_kt",         "direcao_6m_deg"),
    ("intensidade_superficie_kt",  "direcao_superficie_deg"),
    ("intensidade_3m_kt",          "direcao_3m_deg"),
]

for int_col, dir_col in depths_pairs:
    if int_col in df_merged.columns and dir_col in df_merged.columns:
        mask_vazante = (df_merged[dir_col] > 90) & (df_merged[dir_col] < 270)
        df_merged.loc[mask_vazante, int_col] *= -1

# Remover registros onde todas as intensidades alvo são zero (ruído/ausência)
TARGET_BASE = ["intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt"]
y_base_cols = [c for c in TARGET_BASE if c in df_merged.columns]
valid_rows   = df_merged[y_base_cols].sum(axis=1) != 0
df_merged    = df_merged[valid_rows].reset_index(drop=True)
print(f"Após filtro de zero-intensidade: {df_merged.shape}")

# %% [markdown]
# ## 5. Feature Engineering
# Lags (1-5), MAs standard (3, 6) para todas as features;
# MAs longas (12, 24, 48) para as colunas de chuva.

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
# Para cada profundidade, criamos dois horizontes de previsão antecipados.
# O modelo prevê 6 valores simultaneamente: cada profundidade em t+1 e t+2.

# %%
y_parts = {}
for col in y_base_cols:
    y_parts[f"{col}_h1"] = df_merged[col].shift(-1)   # próxima hora
    y_parts[f"{col}_h2"] = df_merged[col].shift(-2)   # daqui a 2 horas

y_multi = pd.DataFrame(y_parts)
y_cols   = list(y_multi.columns)

# Concatena tudo e remove NaN
data_final  = pd.concat([X_o, y_multi], axis=1).dropna()
valid_idx   = data_final.index

X_clean = data_final[X_o.columns]
y_clean = data_final[y_cols]

data_final.to_csv(os.path.join(OUTPUT_DIR, "base_treinamento_v4.csv"), index=False)
print(f"Dataset final: X={X_clean.shape}  y={y_clean.shape}")

# %% [markdown]
# ## 7. Divisão Temporal: 80% Treino | 10% Validação (walk-forward) | 10% Teste

# %%
N         = len(X_clean)
i_val     = int(N * 0.80)
i_test    = int(N * 0.90)

trainX = X_clean.iloc[:i_val];   trainY = y_clean.iloc[:i_val]
valX   = X_clean.iloc[i_val:i_test]; valY   = y_clean.iloc[i_val:i_test]
testX  = X_clean.iloc[i_test:];  testY  = y_clean.iloc[i_test:]

test_dates = df_merged.loc[valid_idx[i_test:], "datahora"].values
val_dates  = df_merged.loc[valid_idx[i_val:i_test], "datahora"].values

scaler_X = MinMaxScaler()
scaler_y = MinMaxScaler()

trainX_n = scaler_X.fit_transform(trainX)
valX_n   = scaler_X.transform(valX)
testX_n  = scaler_X.transform(testX)
trainY_n = scaler_y.fit_transform(trainY)
valY_n   = scaler_y.transform(valY)
testY_n  = scaler_y.transform(testY)

testY_real = testY.values
valY_real  = valY.values

print(f"Treino:{trainX.shape} Val:{valX.shape} Teste:{testX.shape}")

# %% [markdown]
# ## 8. Fase 1 — Tree-Based Models

# %%
tree_models = {
    "Gradient Boosting": MultiOutputRegressor(GradientBoostingRegressor(random_state=42)),
    "XGBoost":           MultiOutputRegressor(XGBRegressor(random_state=42, n_jobs=-1)),
    "LightGBM":          MultiOutputRegressor(LGBMRegressor(random_state=42, n_jobs=-1, verbose=-1)),
    "CatBoost":          MultiOutputRegressor(CatBoostRegressor(random_state=42, verbose=0)),
}

test_preds_all  = {}    # preds estáticas no conjunto de teste
train_preds_all = {}    # guarda preds do treino para diagnóstico

for name, model in tree_models.items():
    print(f">> Treinando {name}...")
    model.fit(trainX_n, trainY_n)
    pred_n = model.predict(testX_n)
    pred_r = scaler_y.inverse_transform(pred_n)
    test_preds_all[name] = pred_r
    joblib.dump(model, os.path.join(OUTPUT_DIR, f"modelo_{name.replace(' ', '_')}.joblib"))

# %% [markdown]
# ## 9. Fase 2 — MLP

# %%
def build_mlp(in_dim, out_dim):
    m = Sequential([
        Input(shape=(in_dim,)),
        Dense(220, activation="relu"), BatchNormalization(),
        Dense(150, activation="relu"), BatchNormalization(),
        Dense(80,  activation="relu"), BatchNormalization(),
        Dense(30,  activation="relu"), BatchNormalization(),
        Dense(out_dim, activation="linear"),
    ])
    m.compile(optimizer=Adam(1e-3), loss="mse", metrics=["mae"])
    return m

print("\n>> Treinando MLP...")
mlp = build_mlp(trainX_n.shape[1], len(y_cols))
es  = EarlyStopping(monitor="val_loss", patience=300, restore_best_weights=True, verbose=0)
mlp.fit(trainX_n, trainY_n, validation_data=(valX_n, valY_n),
        epochs=10000, batch_size=64, verbose=0, callbacks=[es])

pred_mlp  = scaler_y.inverse_transform(mlp.predict(testX_n, verbose=0))
test_preds_all["MLP"] = pred_mlp
mlp.save(os.path.join(OUTPUT_DIR, "modelo_MLP.keras"))

# %% [markdown]
# ## 10. Fase 3 — LSTM (Sliding Windows, timesteps=6)

# %%
TIMESTEPS = 6
base_arr  = df_merged[base_cols].values.astype("float32")

scaler_base = MinMaxScaler()
scaler_base.fit(base_arr[:valid_idx[i_val - 1] + 1])
base_norm   = scaler_base.transform(base_arr)

def make_3d(ids, arr, t):
    return np.array([arr[i - t : i] for i in ids])

trainX_3d = make_3d(valid_idx[:i_val],        base_norm, TIMESTEPS)
valX_3d   = make_3d(valid_idx[i_val:i_test],  base_norm, TIMESTEPS)
testX_3d  = make_3d(valid_idx[i_test:],       base_norm, TIMESTEPS)

def build_lstm(t, feats, out):
    m = Sequential([
        Input(shape=(t, feats)),
        LSTM(128, activation="tanh"),
        Dropout(0.2),
        Dense(64, activation="relu"),
        Dropout(0.2),
        Dense(32, activation="relu"),
        Dense(out, activation="linear"),
    ])
    m.compile(optimizer=Adam(1e-3), loss="mse", metrics=["mae"])
    return m

print("\n>> Treinando LSTM_V4...")
lstm = build_lstm(TIMESTEPS, len(base_cols), len(y_cols))
es2  = EarlyStopping(monitor="val_loss", patience=300, restore_best_weights=True, verbose=0)
lstm.fit(trainX_3d, trainY_n, validation_data=(valX_3d, valY_n),
         epochs=10000, batch_size=64, verbose=0, callbacks=[es2])

pred_lstm = scaler_y.inverse_transform(lstm.predict(testX_3d, verbose=0))
test_preds_all["LSTM_V4"] = pred_lstm
lstm.save(os.path.join(OUTPUT_DIR, "modelo_LSTM_V4.keras"))

# %% [markdown]
# ## 11. Walk-Forward Evaluation (2h-horizon)
# Para cada modelo treinado, executamos um loop sobre o período de validação + teste.
# A cada passo, revelamos 2 horas de dado real, prevemos as 2 horas seguintes,
# acumulamos os erros separados por horizonte (h+1 vs h+2) e avançamos.

# %%
STEP = 2   # quantas horas revelamos / prevemos por iteração
ALL_DATA   = pd.concat([valX, testX], axis=0).reset_index(drop=True)
ALL_Y_REAL = np.vstack([valY_real, testY_real])
ALL_DATES  = np.concatenate([val_dates, test_dates])

def walk_forward_eval(model_name, predictor_fn, X_arr, y_real, dates, step=2):
    """
    Avança de 'step' em 'step', prevê cegamente as próximas 'step' linhas,
    ingere o real, e acumula erros por horizonte (h1 e h2).
    Retorna DataFrame com erro MAE por horizonte e por col alvo.
    """
    records_h1, records_h2 = [], []

    for start in range(0, len(X_arr) - step, step):
        X_window = X_arr[start : start + step]
        y_true   = y_real[start : start + step]
        ts       = dates[start : start + step]

        X_norm_w = scaler_X.transform(X_window)
        y_pred_n = predictor_fn(X_norm_w)   # (step, n_targets)
        y_pred   = scaler_y.inverse_transform(y_pred_n)

        # H+1 = primeira previsão do bloco
        rec_h1 = {"datahora": ts[0], "horizonte": "+1h"}
        for j, col in enumerate(y_cols):
            rec_h1[f"real_{col}"]  = y_true[0, j] if len(y_true) > 0 else np.nan
            rec_h1[f"pred_{col}"]  = y_pred[0, j]
        records_h1.append(rec_h1)

        # H+2 = segunda previsão do bloco (quando existir)
        if len(y_true) > 1:
            rec_h2 = {"datahora": ts[1], "horizonte": "+2h"}
            for j, col in enumerate(y_cols):
                rec_h2[f"real_{col}"] = y_true[1, j]
                rec_h2[f"pred_{col}"] = y_pred[1, j]
            records_h2.append(rec_h2)

    df_wf = pd.DataFrame(records_h1 + records_h2)
    df_wf["model"] = model_name
    return df_wf

# Gera predictor lambdas para cada modelo
def tree_predictor(model):
    return lambda X: model.predict(X)

def keras_predictor(model):
    return lambda X: model.predict(X, verbose=0)

def lstm_predictor(model, t_steps, arr_norm, all_valid_ids):
    """Para LSTM precisamos montar o 3D a partir dos índices reais no DataFrame."""
    combined_ids = np.array(list(valid_idx[i_val:]))   # numpy array para suportar slice
    state = {"call_count": 0}                           # closure mutável
    def fn(X):
        batch_size_local = len(X)
        idxs = combined_ids[state["call_count"] : state["call_count"] + batch_size_local]
        state["call_count"] += batch_size_local
        X3d = np.array([arr_norm[i - t_steps : i] for i in idxs])
        return model.predict(X3d, verbose=0)
    return fn

models_wf = {
    **{name: tree_predictor(mdl) for name, mdl in tree_models.items()},
    "MLP":     keras_predictor(mlp),
    "LSTM_V4": lstm_predictor(lstm, TIMESTEPS, base_norm, valid_idx[i_val:]),
}

ALL_DATA_np = ALL_DATA.values.astype("float32")

print("\n=== Walk-Forward Evaluation ===")
wf_results_all = []
for mname, pred_fn in models_wf.items():
    print(f"  > {mname}...")
    try:
        df_wf = walk_forward_eval(mname, pred_fn, ALL_DATA_np, ALL_Y_REAL, ALL_DATES, STEP)
        wf_results_all.append(df_wf)
    except Exception as e:
        print(f"    Erro em {mname}: {e}")

df_wf_all = pd.concat(wf_results_all, ignore_index=True)
df_wf_all.to_csv(os.path.join(OUTPUT_DIR, "walk_forward_predictions.csv"), index=False)
print(f"Walk-forward resultado salvo ({len(df_wf_all)} linhas).")

# %% [markdown]
# ## 12. Métricas Consolidadas por Horizonte

# %%
summary_rows = []
for mname in df_wf_all["model"].unique():
    for hz in ["+1h", "+2h"]:
        sub = df_wf_all[(df_wf_all["model"] == mname) & (df_wf_all["horizonte"] == hz)]
        if sub.empty:
            continue
        row = {"Model": mname, "Horizonte": hz}
        for col in y_cols:
            real_c = f"real_{col}"
            pred_c = f"pred_{col}"
            if real_c not in sub.columns:
                continue
            real_v = sub[real_c].dropna().values
            pred_v = sub[pred_c].dropna().values
            n = min(len(real_v), len(pred_v))
            if n == 0:
                continue
            label = col.replace("intensidade_","Int_").replace("_kt","")
            row[f"MAE_{label}"]    = round(mean_absolute_error(real_v[:n], pred_v[:n]), 3)
            row[f"RMSE_{label}"]   = round(np.sqrt(mean_squared_error(real_v[:n], pred_v[:n])), 3)
            row[f"R2_{label}"]     = round(r2_score(real_v[:n], pred_v[:n]), 3)
            row[f"Acc05_{label}%"] = round(np.mean(np.abs(real_v[:n] - pred_v[:n]) < 0.5) * 100, 2)
        summary_rows.append(row)

df_summary = pd.DataFrame(summary_rows)
df_summary.to_csv(os.path.join(OUTPUT_DIR, "metricas_walk_forward_v4.csv"), index=False)
print("\n=== MÉTRICAS WALK-FORWARD (extrato Acc<0.5) ===")
acc_cols = ["Model", "Horizonte"] + [c for c in df_summary.columns if "Acc05" in c]
print(df_summary[acc_cols].to_string(index=False))

# %% [markdown]
# ## 13. Visualização — Previsão vs Real por Horizonte
# Plotamos no eixo X as datas reais do período de validação + teste.

# %%
n_depths = len(y_base_cols)   # 3 profundidades

for hz in ["+1h", "+2h"]:
    fig, axes = plt.subplots(n_depths, 1, figsize=(18, 5 * n_depths), sharex=True)
    if n_depths == 1:
        axes = [axes]

    for i, base_col in enumerate(y_base_cols):
        ax = axes[i]
        col_h = f"{base_col}_h{hz[-2]}"   # '_h1' ou '_h2'
        real_col = f"real_{col_h}"
        pred_col = f"pred_{col_h}"

        for mname in df_wf_all["model"].unique():
            sub = df_wf_all[(df_wf_all["model"] == mname) & (df_wf_all["horizonte"] == hz)].copy()
            if pred_col not in sub.columns:
                continue
            sub = sub.dropna(subset=[real_col, pred_col])
            sub["datahora"] = pd.to_datetime(sub["datahora"])
            sub = sub.sort_values("datahora")

            if mname == list(df_wf_all["model"].unique())[0]:
                ax.plot(sub["datahora"], sub[real_col],
                        label="Real", color="black", linewidth=2.2, zorder=10)
            ax.plot(sub["datahora"], sub[pred_col],
                    label=mname, alpha=0.7, linestyle="--")

        ax.set_title(f"{base_col} | Horizonte {hz}", fontsize=13)
        ax.legend(ncol=3, loc="lower left", fontsize=8)
        ax.grid(True, alpha=0.4)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%d/%m\n%H:%M"))

    plt.suptitle(f"Walk-Forward V4 — Horizonte {hz}", fontsize=15, y=1.01)
    plt.tight_layout()
    fname = os.path.join(OUTPUT_DIR, f"walkforward_{hz.replace('+','h')}.png")
    plt.savefig(fname, dpi=180, bbox_inches="tight")
    plt.show()
    print(f"Plot salvo: {fname}")

print("\n✅ Pipeline V4 concluído! Arquivos em:", OUTPUT_DIR)
