# %% [markdown]
# # Previsão de Correnteza V6 — Inferência em Produção (Local)
#
# Transforma o pipeline de treino V5 em inferência pura:
# - Carrega modelo LightGBM pré-treinado + scalers do V5
# - Busca dados ao vivo do BigQuery (últimas 48h + hora atual)
# - Chuva via Open-Meteo Forecast (não mais archive)
# - OpenWeather forecast via BQ, interpolado de 3h→1h
# - Motor recursivo simplificado: 3 rounds → previsões +1h a +6h
# - Escrita no BigQuery com MERGE (regra do primeiro_calculo a 2h)
# - Foco: intensidade_superficie_kt

# %%
import sys
import os

# Fix Windows encoding (cp1252 → utf-8)
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

import time
import warnings
import traceback as _tb
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
import requests
import joblib
from google.oauth2 import service_account
from google.cloud import bigquery

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURAÇÃO
# ══════════════════════════════════════════════════════════════════════════════

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"

# Diretório dos modelos pré-treinados (na mesma pasta V6 agora)
MODEL_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V6"
OUTPUT_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V6"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Tabelas BigQuery
TABLE_HISTORICO   = f"{PROJECT_ID}.wherehouse_tratado.mestre_hour_tratada"
TABLE_5MIN        = f"{PROJECT_ID}.wherehouse_tratado.mestre_5min_linear"
TABLE_OW_FORECAST = f"{PROJECT_ID}.wherehouse.dados_openweather_forecast"
TABLE_PREVISOES   = f"{PROJECT_ID}.wherehouse_tratado.previsoes_oficiais"
TABLE_TEMP        = f"{PROJECT_ID}.wherehouse_tratado.temp_previsoes_run"

# Cidades para chuva (Open-Meteo)
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

# ═══════════════════════════════════════════════════════════════════
# Colunas-base que o modelo V5 espera (da tabela ML.xtrain_horario_t_2026)
# ═══════════════════════════════════════════════════════════════════
# Mapeamento: mestre_hour_tratada → nomes do V5
# mestre_hour_tratada.timestamp_br        → datahora
# mestre_hour_tratada.altura_prev_getmare → previsao
# mestre_hour_tratada.sensacaotermica     → hi_temp  (high temp / sensação ~ hi_temp)
# mestre_hour_tratada.umidade             → out_hum
# mestre_hour_tratada.ventointensidade    → wind_speed
# mestre_hour_tratada.ventonum            → vento_num
# mestre_hour_tratada.pressao             → bar
# mestre_hour_tratada.tipo                → fase_lua (tipo = fase da lua numérica)
# mestre_hour_tratada.direcao_6m_deg      → direcao_6m_deg
# mestre_hour_tratada.direcao_superficie_deg → direcao_superficie_deg
# mestre_hour_tratada.direcao_3m_deg      → direcao_3m_deg
# mestre_hour_tratada.intensidade_6m      → intensidade_6m_kt
# mestre_hour_tratada.intensidade_superficie → intensidade_superficie_kt
# mestre_hour_tratada.intensidade_3m      → intensidade_3m_kt
# mestre_hour_tratada.altura_real_getmare → altura_mare

BASE_COLS = [
    "previsao", "hi_temp", "out_hum", "wind_speed", "vento_num", "bar",
    "fase_lua", "direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg",
    "intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt",
    "altura_mare",
    "rain_PORTO_ALEGRE", "rain_CANOAS", "rain_SAO_LEOPOLDO",
    "rain_NOVO_HAMBURGO", "rain_GRAVATAI", "rain_SANTA_MARIA",
    "rain_CACHOEIRA_SUL", "rain_SANTA_CRUZ_SUL", "rain_RIO_GRANDE",
]

# Variáveis de correnteza (targets do V5)
TARGET_BASE = ["intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt"]

# Foco desta V6: apenas superfície
FOCO_COL = "intensidade_superficie_kt"

# Targets multi-step do modelo (ordem do scaler_y do V5)
Y_COLS = [
    "intensidade_6m_kt_h1", "intensidade_6m_kt_h2",
    "intensidade_superficie_kt_h1", "intensidade_superficie_kt_h2",
    "intensidade_3m_kt_h1", "intensidade_3m_kt_h2",
]

# ══════════════════════════════════════════════════════════════════════════════
# 1. CARREGAMENTO DO MODELO E SCALERS (Local)
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("═" * 70)
print("  V6 — INFERÊNCIA DE CORRENTEZA (SUPERFÍCIE)")
print("═" * 70)

print("\n[1/7] Carregando modelo e scalers do V5...")
scaler_X = joblib.load(os.path.join(MODEL_DIR, "scaler_X.joblib"))
scaler_y = joblib.load(os.path.join(MODEL_DIR, "scaler_y.joblib"))
model    = joblib.load(os.path.join(MODEL_DIR, "modelo_LightGBM.joblib"))
print(f"  ✓ Modelo LightGBM carregado ({scaler_X.n_features_in_} features → {scaler_y.n_features_in_} targets)")

# ══════════════════════════════════════════════════════════════════════════════
# 2. AUTENTICAÇÃO GCP
# ══════════════════════════════════════════════════════════════════════════════

# %%
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
print("[2/7] ✓ Autenticação GCP configurada")

# ══════════════════════════════════════════════════════════════════════════════
# 3. INGESTÃO DE DADOS AO VIVO
# ══════════════════════════════════════════════════════════════════════════════

# %% [markdown]
# ### 3A. Histórico Recente — Últimas 48h do BigQuery
# Mapeamos as colunas de mestre_hour_tratada para os nomes que o modelo V5 espera.

# %%
print("\n[3/7] Buscando dados ao vivo...")

# SQL com mapeamento de colunas para o formato V5
query_historico = f"""
SELECT
    CAST(timestamp_br AS DATETIME) AS datahora,
    CAST(altura_prev_getmare AS FLOAT64) AS previsao,
    CAST(sensacaotermica AS FLOAT64) AS hi_temp,
    CAST(umidade AS FLOAT64) AS out_hum,
    CAST(ventointensidade AS FLOAT64) AS wind_speed,
    CAST(ventonum AS FLOAT64) AS vento_num,
    CAST(pressao AS FLOAT64) AS bar,
    CAST(tipo AS FLOAT64) AS fase_lua,
    CAST(direcao_6m AS FLOAT64) AS direcao_6m_deg,
    CAST(direcao_superficie AS FLOAT64) AS direcao_superficie_deg,
    CAST(direcao_3m AS FLOAT64) AS direcao_3m_deg,
    CAST(intensidade_6m AS FLOAT64) AS intensidade_6m_kt,
    CAST(intensidade_superficie AS FLOAT64) AS intensidade_superficie_kt,
    CAST(intensidade_3m AS FLOAT64) AS intensidade_3m_kt,
    CAST(altura_real_getmare AS FLOAT64) AS altura_mare
FROM `{TABLE_HISTORICO}`
ORDER BY timestamp_br DESC
LIMIT 48
"""
print("  3A. Histórico recente (últimas 48h)...")
df_historico = bq_client.query(query_historico).to_dataframe()
df_historico["datahora"] = pd.to_datetime(df_historico["datahora"])
df_historico = df_historico.sort_values("datahora").reset_index(drop=True)
print(f"      ✓ {len(df_historico)} registros carregados")
print(f"      Período: {df_historico['datahora'].iloc[0]} → {df_historico['datahora'].iloc[-1]}")

# %% [markdown]
# ### 3B. Hora Atual (estimada via 5 minutos interpolados)

# %%
query_hora_atual = f"""
SELECT
    DATETIME_TRUNC(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo'), HOUR) AS datahora,
    AVG(CAST(intensidade_superficie AS FLOAT64)) AS intensidade_superficie_kt,
    AVG(CAST(intensidade_6m AS FLOAT64)) AS intensidade_6m_kt,
    AVG(CAST(intensidade_3m AS FLOAT64)) AS intensidade_3m_kt,
    AVG(CAST(direcao_superficie AS FLOAT64)) AS direcao_superficie_deg,
    AVG(CAST(direcao_6m AS FLOAT64)) AS direcao_6m_deg,
    AVG(CAST(direcao_3m AS FLOAT64)) AS direcao_3m_deg
FROM `{TABLE_5MIN}`
WHERE CAST(timestamp_br AS DATETIME) >= DATETIME_TRUNC(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo'), HOUR)
"""
print("  3B. Estimativa da hora atual (via 5 min)...")
try:
    df_hora_atual = bq_client.query(query_hora_atual).to_dataframe()
    df_hora_atual["datahora"] = pd.to_datetime(df_hora_atual["datahora"])

    if not df_hora_atual.empty and not df_hora_atual["datahora"].isna().all():
        # Mescla com a última linha do histórico para preencher colunas meteorológicas
        last_row = df_historico.iloc[-1:].copy()
        for col in df_hora_atual.columns:
            if col != "datahora" and col in last_row.columns and pd.notna(df_hora_atual[col].values[0]):
                last_row[col] = df_hora_atual[col].values[0]
        last_row["datahora"] = df_hora_atual["datahora"].values[0]
        df_historico = pd.concat([df_historico, last_row], ignore_index=True)
        print(f"      ✓ Hora atual estimada: {df_hora_atual['datahora'].values[0]}")
    else:
        print("      ⚠ Sem dados 5min para hora atual, usando último registro")
except Exception as e:
    print(f"      ⚠ Erro ao buscar hora atual: {e}")

# %% [markdown]
# ### 3C. Chuva — Open-Meteo Forecast (últimas 48h + próximas 6h)

# %%
print("  3C. Chuva Open-Meteo (forecast)...")
rain_list = []
for nome, lat, lon in CIDADES:
    try:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat, "longitude": lon,
                "hourly": "precipitation",
                "past_hours": 48,       # precisa de 48h para MAs
                "forecast_hours": 6,
                "timezone": "America/Sao_Paulo",
            },
            timeout=30,
        )
        r.raise_for_status()
        hourly = r.json().get("hourly", {})
        if hourly.get("time"):
            df_r = pd.DataFrame(hourly)
            df_r["time"] = pd.to_datetime(df_r["time"])
            df_r = df_r.rename(columns={"precipitation": f"rain_{nome}"})
            rain_list.append(df_r.set_index("time")[[f"rain_{nome}"]])
    except Exception as e:
        print(f"      ⚠ Erro chuva {nome}: {e}")
    time.sleep(0.3)  # rate limit suave

if rain_list:
    df_rain = rain_list[0]
    for extra in rain_list[1:]:
        df_rain = df_rain.join(extra, how="outer")
    df_rain = df_rain.fillna(0).reset_index().rename(columns={"time": "datahora"})
    df_rain["datahora"] = pd.to_datetime(df_rain["datahora"])
    print(f"      ✓ Chuva carregada: {len(df_rain)} horas, {len(CIDADES)} cidades")
else:
    df_rain = pd.DataFrame()
    print("      ⚠ Nenhum dado de chuva obtido")

# %% [markdown]
# ### 3D. OpenWeather Forecast via BigQuery (vento, pressão — próximas 9h)

# %%
print("  3D. OpenWeather forecast (BQ)...")

# O timestamp_execucao na tabela de forecast nos permite pegar a rodagem mais recente
query_ow = f"""
SELECT * FROM (
    SELECT
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_txt) AS datahora,
        wind_speed,
        wind_deg,
        wind_gust,
        pressure,
        humidity,
        temp,
        feels_like,
        ROW_NUMBER() OVER(PARTITION BY dt_txt ORDER BY timestamp_execucao DESC) as rn
    FROM `{TABLE_OW_FORECAST}`
    WHERE PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_txt) >= CURRENT_TIMESTAMP()
      AND PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', dt_txt) <= TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 9 HOUR)
)
WHERE rn = 1
ORDER BY datahora
"""
try:
    df_ow = bq_client.query(query_ow).to_dataframe()
    df_ow["datahora"] = pd.to_datetime(df_ow["datahora"])
    print(f"      ✓ {len(df_ow)} registros de forecast OpenWeather")
except Exception as e:
    df_ow = pd.DataFrame()
    print(f"      ⚠ Erro OpenWeather: {e}")

# Interpolação 3h→1h
if not df_ow.empty and len(df_ow) >= 2:
    # Garante que não há duplicatas de datahora (o MERGE/ROW_NUMBER já deve ter resolvido)
    df_ow = df_ow.drop_duplicates(subset=["datahora"]).set_index("datahora")
    num_cols = df_ow.select_dtypes(include=[np.number]).columns.tolist()
    df_ow_num = df_ow[num_cols]

    novo_index = pd.date_range(
        start=df_ow_num.index.min(),
        end=df_ow_num.index.max(),
        freq="1h",
    )
    df_ow_interp = df_ow_num.reindex(novo_index).interpolate(method="linear")
    df_ow_interp = df_ow_interp.reset_index().rename(columns={"index": "datahora"})
    print(f"      ✓ Interpolado para {len(df_ow_interp)} horas")
else:
    df_ow_interp = pd.DataFrame()
    print("      ⚠ OpenWeather insuficiente para interpolação")

# ══════════════════════════════════════════════════════════════════════════════
# 4. PRÉ-PROCESSAMENTO — Monta DataFrame Unificado
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("\n[4/7] Pré-processando dados...")

# 4.1 Merge histórico com chuva
df_base = df_historico.copy()
if not df_rain.empty:
    df_base["datahora_h"] = df_base["datahora"].dt.floor("h")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("h")
    df_base = df_base.merge(
        df_rain.drop(columns=["datahora"]), on="datahora_h", how="left"
    )
    df_base.drop(columns=["datahora_h"], inplace=True)

# 4.2 Garantir colunas de chuva existem (mesmo se a API falhou)
for nome, _, _ in CIDADES:
    rain_col = f"rain_{nome}"
    if rain_col not in df_base.columns:
        df_base[rain_col] = 0.0

# 4.3 Converter tudo para numérico
df_base = df_base.sort_values("datahora").reset_index(drop=True)
cols_conv = df_base.columns.drop("datahora", errors="ignore")
df_base[cols_conv] = df_base[cols_conv].apply(
    pd.to_numeric, errors="coerce"
).fillna(0).astype("float32")

# 4.4 Transformação direcional (+ enchente / - vazante)
for int_col, dir_col in [
    ("intensidade_6m_kt", "direcao_6m_deg"),
    ("intensidade_superficie_kt", "direcao_superficie_deg"),
    ("intensidade_3m_kt", "direcao_3m_deg"),
]:
    if int_col in df_base.columns and dir_col in df_base.columns:
        mask = (df_base[dir_col] > 90) & (df_base[dir_col] < 270)
        df_base.loc[mask, int_col] *= -1

print(f"  ✓ Base preparada: {df_base.shape}")
print(f"  ✓ Última linha: {df_base['datahora'].iloc[-1]}")
print(f"  ✓ Colunas: {list(df_base.columns)}")

# ══════════════════════════════════════════════════════════════════════════════
# 5. FEATURE ENGINEERING — Lags + MAs (mesmo esquema do V5)
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("\n[5/7] Engenharia de features...")

# Usar exatamente as mesmas colunas base que o V5 usou (exceto datahora)
v5_base_cols = [c for c in BASE_COLS]
rain_feat_cols = [c for c in v5_base_cols if c.startswith("rain_")]

X_parts = []
for lag in range(1, 6):
    Xl = df_base[v5_base_cols].shift(lag)
    Xl.columns = [f"{c}_lag{lag}" for c in v5_base_cols]
    X_parts.append(Xl)
for w in [3, 6]:
    Xm = df_base[v5_base_cols].rolling(w).mean().shift(1)
    Xm.columns = [f"{c}_ma{w}" for c in v5_base_cols]
    X_parts.append(Xm)
for w in [12, 24, 48]:
    if rain_feat_cols:
        Xr = df_base[rain_feat_cols].rolling(w).mean().shift(1)
        Xr.columns = [f"{c}_ma{w}" for c in rain_feat_cols]
        X_parts.append(Xr)

X_o = pd.concat(X_parts, axis=1)

# Pegar a ÚLTIMA LINHA (t0) que tem todas as features calculadas
last_valid_idx = X_o.dropna().index[-1] if not X_o.dropna().empty else X_o.index[-1]
X_t0 = X_o.loc[last_valid_idx].fillna(0).values.astype("float32")

# Verificar dimensões
expected_features = scaler_X.n_features_in_
actual_features = len(X_t0)
print(f"  Features esperadas: {expected_features}, geradas: {actual_features}")

if actual_features != expected_features:
    print(f"  ⚠ AJUSTANDO dimensão: {actual_features} → {expected_features}")
    if actual_features < expected_features:
        X_t0 = np.pad(X_t0, (0, expected_features - actual_features), constant_values=0)
    else:
        X_t0 = X_t0[:expected_features]

print(f"  ✓ Features t₀ prontas: shape=({len(X_t0)},)")

# Guardar nomes das colunas de features para manipulação de lags
feature_names = list(X_o.columns)
while len(feature_names) < expected_features:
    feature_names.append(f"_pad_{len(feature_names)}")
feature_names = feature_names[:expected_features]

# ══════════════════════════════════════════════════════════════════════════════
# 6. MOTOR RECURSIVO — 3 Rounds → Previsões +1h a +6h
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("\n[6/7] Executando inferência recursiva...")

corr_base_cols = TARGET_BASE
n_base = len(corr_base_cols)  # 3 profundidades

# Índice da coluna de superfície nos Y_COLS
idx_sup_h1 = Y_COLS.index("intensidade_superficie_kt_h1")
idx_sup_h2 = Y_COLS.index("intensidade_superficie_kt_h2")


def predict_2h(X_row):
    """Prevê os próximos 2 horizontes para uma linha de features."""
    X_2d = X_row.reshape(1, -1)
    X_n = scaler_X.transform(X_2d)
    raw = model.predict(X_n)
    pred_n = np.atleast_2d(raw)
    return scaler_y.inverse_transform(pred_n)[0]  # (6,)


def update_lags(X_row, pred_prev, feature_names):
    """Atualiza lags e MAs de correnteza com valores previstos."""
    X_new = X_row.copy()
    for i, col in enumerate(corr_base_cols):
        h1_pred = pred_prev[i * 2]             # posição no Y_COLS: h1
        h2_pred = pred_prev[i * 2 + 1]         # posição no Y_COLS: h2

        # Atualiza lag1 e lag2
        for lag_n, lag_val in [(1, h2_pred), (2, h1_pred)]:
            # lag1 = passado mais recente = previsão h2 (mais perto do t+3)
            # lag2 = anterior = previsão h1
            lag_col = f"{col}_lag{lag_n}"
            if lag_col in feature_names:
                X_new[feature_names.index(lag_col)] = lag_val

        # Recalcula MAs
        for ma_w in [3, 6]:
            ma_col = f"{col}_ma{ma_w}"
            if ma_col in feature_names:
                hist_lags = []
                for l in range(3, min(ma_w + 1, 6)):
                    lc = f"{col}_lag{l}"
                    if lc in feature_names:
                        hist_lags.append(X_new[feature_names.index(lc)])
                window_vals = [h2_pred, h1_pred] + hist_lags
                X_new[feature_names.index(ma_col)] = np.mean(window_vals[:ma_w])
    return X_new


def inject_meteo_forecast(X_row, hour_offset, feature_names, df_ow_interp_local):
    """Injeta dados meteorológicos do forecast OpenWeather para horas futuras."""
    if df_ow_interp_local is None or df_ow_interp_local.empty:
        return X_row

    X_new = X_row.copy()
    now = pd.Timestamp.now().floor("h")
    target_time = now + pd.Timedelta(hours=hour_offset)

    # Mapeia colunas do OpenWeather → colunas do modelo V5
    meteo_mapping = {
        "wind_speed": "wind_speed",
        "pressure": "bar",
        "humidity": "out_hum",
        "feels_like": "hi_temp",
    }

    for ow_col, model_col in meteo_mapping.items():
        if ow_col in df_ow_interp_local.columns:
            # Encontra a linha mais próxima no forecast
            time_diffs = (df_ow_interp_local["datahora"].dt.tz_localize(None) - target_time).abs()
            closest_idx = time_diffs.idxmin()
            val = df_ow_interp_local.loc[closest_idx, ow_col]

            # Atualiza lag1 desta variável meteo
            lag1_col = f"{model_col}_lag1"
            if lag1_col in feature_names:
                X_new[feature_names.index(lag1_col)] = val

    return X_new


# Momento atual
t0_datetime = df_base["datahora"].iloc[last_valid_idx]
print(f"  t₀ = {t0_datetime}")

# === ROUND 1: Dados reais → Prevê t+1, t+2 ===
pred_r1 = predict_2h(X_t0)
print(f"  Round 1: t+1={pred_r1[idx_sup_h1]:.3f} kt, t+2={pred_r1[idx_sup_h2]:.3f} kt")

# === ROUND 2: Atualiza lags com t+1/t+2 previstos → Prevê t+3, t+4 ===
X_r2 = update_lags(X_t0.copy(), pred_r1, feature_names)
ow_data = df_ow_interp if not df_ow_interp.empty else None
X_r2 = inject_meteo_forecast(X_r2, 3, feature_names, ow_data)
pred_r2 = predict_2h(X_r2)
print(f"  Round 2: t+3={pred_r2[idx_sup_h1]:.3f} kt, t+4={pred_r2[idx_sup_h2]:.3f} kt")

# === ROUND 3: Atualiza lags com t+3/t+4 previstos → Prevê t+5, t+6 ===
X_r3 = update_lags(X_r2.copy(), pred_r2, feature_names)
X_r3 = inject_meteo_forecast(X_r3, 5, feature_names, ow_data)
pred_r3 = predict_2h(X_r3)
print(f"  Round 3: t+5={pred_r3[idx_sup_h1]:.3f} kt, t+6={pred_r3[idx_sup_h2]:.3f} kt")

# Monta DataFrame de resultados
previsoes = []
for h_offset, pred, h1h2 in [
    (1, pred_r1, "h1"), (2, pred_r1, "h2"),
    (3, pred_r2, "h1"), (4, pred_r2, "h2"),
    (5, pred_r3, "h1"), (6, pred_r3, "h2"),
]:
    datahora_alvo = t0_datetime + pd.Timedelta(hours=h_offset)
    if h1h2 == "h1":
        val = pred[idx_sup_h1]
    else:
        val = pred[idx_sup_h2]
    previsoes.append({
        "datahora_alvo": datahora_alvo,
        "horizonte": f"+{h_offset}h",
        "previsao_correnteza_superficie": round(float(val), 4),
        "gerado_em": pd.Timestamp.now(),
    })

df_previsoes = pd.DataFrame(previsoes)

print("\n  ┌─────────────────────┬───────────┬────────────────────────────┐")
print("  │   Hora Alvo         │ Horizonte │ Previsão Superfície (kt)   │")
print("  ├─────────────────────┼───────────┼────────────────────────────┤")
for _, row in df_previsoes.iterrows():
    dt_str = row['datahora_alvo'].strftime('%Y-%m-%d %H:%M')
    hz_str = row['horizonte']
    val_str = f"{row['previsao_correnteza_superficie']:>8.4f}"
    print(f"  │ {dt_str}    │   {hz_str:>5s}   │          {val_str}            │")
print("  └─────────────────────┴───────────┴────────────────────────────┘")

# Salva localmente
df_previsoes.to_csv(os.path.join(OUTPUT_DIR, "previsoes_v6_last_run.csv"), index=False)

# ══════════════════════════════════════════════════════════════════════════════
# 7. ESCRITA NO BIGQUERY — MERGE com Regra do Primeiro Cálculo
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("\n[7/7] Escrevendo previsões no BigQuery...")

# 7.1 Escreve na tabela temporária
df_temp = df_previsoes[["datahora_alvo", "previsao_correnteza_superficie"]].copy()
# Garante que o timestamp é timezone-naive para o BigQuery
df_temp["datahora_alvo"] = pd.to_datetime(df_temp["datahora_alvo"]).dt.tz_localize(None)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("datahora_alvo", "TIMESTAMP"),
        bigquery.SchemaField("previsao_correnteza_superficie", "FLOAT64"),
    ],
)

try:
    load_job = bq_client.load_table_from_dataframe(
        df_temp, TABLE_TEMP, job_config=job_config
    )
    load_job.result()
    print(f"  ✓ Tabela temp escrita: {TABLE_TEMP} ({len(df_temp)} linhas)")
except Exception as e:
    print(f"  ⚠ Erro ao escrever tabela temp: {e}")
    _tb.print_exc()

# 7.2 Executa MERGE com a tabela oficial (regra do primeiro_calculo a 2h)
merge_sql = f"""
MERGE `{TABLE_PREVISOES}` T
USING `{TABLE_TEMP}` S
ON T.datahora_alvo = S.datahora_alvo

-- Se a hora já existe, atualiza a previsão dinâmica
WHEN MATCHED THEN
  UPDATE SET
    T.correnteza_superficie_prevista_dinamica = S.previsao_correnteza_superficie,
    T.atualizado_em = CURRENT_TIMESTAMP(),
    -- Compara a hora alvo com o horário local real do Brasil (-3h de diferença com UTC)
    T.correnteza_superficie_prevista_primeiro_calculo = IF(
        T.correnteza_superficie_prevista_primeiro_calculo IS NULL
        AND TIMESTAMP_DIFF(S.datahora_alvo, CAST(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS TIMESTAMP), MINUTE) BETWEEN 105 AND 135,
        S.previsao_correnteza_superficie,
        T.correnteza_superficie_prevista_primeiro_calculo
    )

-- Se a hora não existe (previsões distantes), insere linha nova
WHEN NOT MATCHED THEN
  INSERT (datahora_alvo, correnteza_superficie_prevista_dinamica, correnteza_superficie_prevista_primeiro_calculo, atualizado_em)
  VALUES (
    S.datahora_alvo, 
    S.previsao_correnteza_superficie, 
    -- Se já estiver na janela de 2h na inserção, grava no primeiro_calculo
    IF(TIMESTAMP_DIFF(S.datahora_alvo, CAST(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS TIMESTAMP), MINUTE) BETWEEN 105 AND 135, S.previsao_correnteza_superficie, NULL),
    CURRENT_TIMESTAMP()
  )
"""

try:
    merge_job = bq_client.query(merge_sql)
    merge_job.result()
    print(f"  ✓ MERGE executado com sucesso na tabela {TABLE_PREVISOES}")
    print(f"    Regra 2a hora: primeiro_calculo preenchido se NULL e Δt ∈ [105, 135] min")
except Exception as e:
    print(f"  ⚠ Erro no MERGE (tabela pode não existir ainda): {e}")
    print(f"  Para criar a tabela oficial pela primeira vez, execute:")
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_PREVISOES}` (
      datahora_alvo TIMESTAMP,
      correnteza_superficie_prevista_dinamica FLOAT64,
      correnteza_superficie_prevista_primeiro_calculo FLOAT64,
      atualizado_em TIMESTAMP
    )
    """
    print(f"  Tentando criar tabela automaticamente...")
    try:
        bq_client.query(create_sql).result()
        print(f"  ✓ Tabela criada! Re-executando MERGE...")
        merge_job = bq_client.query(merge_sql)
        merge_job.result()
        print(f"  ✓ MERGE executado com sucesso!")
    except Exception as e2:
        print(f"  ✗ Falha ao criar tabela: {e2}")

# ══════════════════════════════════════════════════════════════════════════════
# RESULTADO FINAL
# ══════════════════════════════════════════════════════════════════════════════

# %%
print("\n" + "═" * 70)
print("  ✅ V6 CONCLUÍDA — Previsões de superfície geradas com sucesso!")
print(f"  📁 CSV local: {os.path.join(OUTPUT_DIR, 'previsoes_v6_last_run.csv')}")
print(f"  🕐 Próxima execução: a cada 15 minutos via Cloud Scheduler")
print("═" * 70)
