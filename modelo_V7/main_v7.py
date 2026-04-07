# %% [markdown]
# # Previsão de Correnteza V7 — Inferência em Produção (Foco no Futuro)
# 
# Diferente da V6 que atualizava lags recursivamente "no escuro" para os climas futuros,
# a V7 traz o "Amanhã" direto do OpenWeather e Open-Meteo como features puras ("Leads"). 
# A rede agora enxerga 5 horas a frente num relance e entrega Correnteza +1h e +2h absolutas.

import os, sys, time, requests, joblib, warnings
import pandas as pd
import numpy as np
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

warnings.filterwarnings('ignore')
if sys.stdout.encoding != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

print("==========================================================")
print("  V7 — INFERÊNCIA DE CORRENTEZA (LEADS CLIMÁTICOS)")
print("==========================================================")

# ====================================================================================
# 1. CONFIGURAÇÕES GERAIS E CARREGAMENTO
# ====================================================================================
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"
MODEL_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V7"
OUTPUT_DIR = MODEL_DIR

TABLE_HISTORICO = f"{PROJECT_ID}.wherehouse_tratado.mestre_hour_tratada"
TABLE_PREVISOES = f"{PROJECT_ID}.wherehouse_tratado.previsoes_oficiais"
TABLE_TEMP      = f"{PROJECT_ID}.wherehouse_tratado.temp_previsoes_run_v7"

CIDADES = [
    ("PORTO_ALEGRE", -30.0325, -51.2304), ("CANOAS", -29.9216, -51.1800),
    ("SAO_LEOPOLDO", -29.7544, -51.1516), ("NOVO_HAMBURGO", -29.6906, -51.1429),
    ("GRAVATAI", -29.9440, -50.9931), ("SANTA_MARIA", -29.6861, -53.8069),
    ("CACHOEIRA_SUL", -30.0482, -52.8902), ("SANTA_CRUZ_SUL", -29.7142, -52.4286),
    ("RIO_GRANDE", -32.035, -52.0986),
]

print("\n[1/5] Carregando Modelo V7 e Scalers...")
scaler_X   = joblib.load(os.path.join(MODEL_DIR, "scaler_X.joblib"))
scaler_y   = joblib.load(os.path.join(MODEL_DIR, "scaler_y.joblib"))
model_lgbm = joblib.load(os.path.join(MODEL_DIR, "modelo_LightGBM.joblib"))
print(f"  ✓ Modelos ativados: Matriz Exige {scaler_X.n_features_in_} features")

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)


# ====================================================================================
# 2. CAPTURA DOS DADOS (O PASSADO DA BOIA + O FUTURO DA ATMOSFERA)
# ====================================================================================
print("\n[2/5] Buscando Dados de Alta Frequência...")

# 2A: O Passado da Boia
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
ORDER BY timestamp_br DESC LIMIT 48
"""
df_hist = bq_client.query(query_historico).to_dataframe()
df_hist["datahora"] = pd.to_datetime(df_hist["datahora"])
df_hist = df_hist.sort_values("datahora").reset_index(drop=True)
print(f"  ✓ Boia carregada: Último timestamp real foi {df_hist['datahora'].iloc[-1]}")

# Identificador do T0 
agora_hora = pd.Timestamp.now().floor('H')
if df_hist['datahora'].iloc[-1] < agora_hora:
    # Se a boia tá levemente atrasada, nós replicamos a última leitura na HORA ATUAL (inércia base t0)
    ultima = df_hist.iloc[-1:].copy()
    ultima['datahora'] = agora_hora
    df_hist = pd.concat([df_hist, ultima], ignore_index=True)
    t0_hora = agora_hora
else:
    t0_hora = df_hist['datahora'].iloc[-1]
print(f"  ✓ Instante Base de Lançamento (T0): {t0_hora}")


# 2B: O Futuro da Atmosfera (OpenWeather)
OW_API_KEY = "10fe60f23364376f39951ae7c07d0007"
lat_rg, lon_rg = -32.035, -52.0986 
try:
    url_ow = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat_rg}&lon={lon_rg}&exclude=current,minutely,daily,alerts&units=metric&appid={OW_API_KEY}"
    r_ow = requests.get(url_ow, timeout=15)
    r_ow.raise_for_status()
    hourly_ow = r_ow.json().get("hourly", [])
    
    rows_ow = []
    for h in hourly_ow:
        rows_ow.append({
            "datahora": pd.to_datetime(h["dt"], unit='s', utc=True).tz_convert("America/Sao_Paulo").tz_localize(None),
            "wind_speed": h.get("wind_speed"),
            "vento_num": h.get("wind_deg"),
            "bar": h.get("pressure"),
            "out_hum": h.get("humidity"),
            "hi_temp": h.get("feels_like")
        })
    df_ow = pd.DataFrame(rows_ow)
    # Pegamos pro futuro (T0 + 1 até T0 + 5)
    df_ow_futuro = df_ow[(df_ow["datahora"] > t0_hora) & (df_ow["datahora"] <= t0_hora + pd.Timedelta(hours=5))].copy()
except Exception as e:
    print(f"  ⚠ Erro OpenWeather: {e}")
    df_ow_futuro = pd.DataFrame()

# 2C: O Futuro da Chuva (Open-Meteo)
rain_frames = []
try:
    for nome, lat, lon in CIDADES:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "hourly": "precipitation", 
                    "past_hours": 48, "forecast_hours": 6, "timezone": "America/Sao_Paulo"},
            timeout=15
        )
        r.raise_for_status()
        h_data = r.json().get("hourly", {})
        if h_data.get("time"):
            dfr = pd.DataFrame(h_data)
            dfr["time"] = pd.to_datetime(dfr["time"])
            dfr = dfr.rename(columns={"precipitation": f"rain_{nome}"})
            rain_frames.append(dfr.set_index("time")[[f"rain_{nome}"]])
except Exception as e:
    print(f"  ⚠ Erro OpenMeteo: {e}")

df_rain = pd.DataFrame()
if rain_frames:
    df_rain = rain_frames[0]
    for extra in rain_frames[1:]:
        df_rain = df_rain.join(extra, how="outer")
    df_rain = df_rain.reset_index().rename(columns={"time": "datahora"})

# Cruza chuva com histórico
if not df_rain.empty:
    df_hist["datahora_h"] = df_hist["datahora"].dt.floor("H")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("H")
    df_hist = df_hist.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_hist = df_hist.drop(columns=["datahora_h"])
    # Mesma coisa pra chuva do futuro
    df_ow_futuro["datahora_h"] = df_ow_futuro["datahora"].dt.floor("H")
    df_ow_futuro = df_ow_futuro.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_ow_futuro = df_ow_futuro.drop(columns=["datahora_h"])

# Se falhar algo da chuva, forçamos zeros 
col_chuvas = [f"rain_{c[0]}" for c in CIDADES]
for col in col_chuvas:
    if col not in df_hist.columns:
        df_hist[col] = 0.0
    if not df_ow_futuro.empty and col not in df_ow_futuro.columns:
        df_ow_futuro[col] = 0.0

# ====================================================================================
# 3. GERAÇÃO FANTASMA DE HORIZONTES PARA GERAR LEADS
# A sacada máxima: Vamos criar um painel temporal único "df_live".
# Com as 5 linhas do "futuro" coladas em baixo da nossa linha atual "T0", 
# usaremos Numpy `.shift(-N)` na raiz de T0 e ele buscará a atmosfera sozinha.
# ====================================================================================
print("\n[3/5] Compilando Grade de Tensão Temporal (T-48... T0... T+5)...")

# Assegura existência de todas as colunas no futuro com NaN
for col in df_hist.columns:
    if col not in df_ow_futuro.columns:
        df_ow_futuro[col] = np.nan

# Empilha tudo (Passado Real + Futuro Fictício)
df_live = pd.concat([df_hist, df_ow_futuro], ignore_index=True)

# Aplica Bússola Setorial (0-360 -> 0-15) nos ângulos
for col in ["direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg", "vento_num"]:
    if col in df_live.columns:
        df_live[col] = pd.to_numeric(df_live[col], errors='coerce')
        df_live[col] = (df_live[col] / 22.5).round() % 16

# Trata vetores nulos
cols_conv = df_live.columns.drop("datahora", errors="ignore")
df_live[cols_conv] = df_live[cols_conv].apply(pd.to_numeric, errors="coerce").astype("float32")
# Prenche campos vazios fantasma (do painel inferior) com ultimo valor (Inércia provisória se algo falhar)
df_live.fillna(method="ffill", inplace=True)
df_live.fillna(0.0, inplace=True)


# ====================================================================================
# 4. FEATURE ENGINEERING PREDITIVO (CONSTRUÇÃO DE T0)
# Vamos replicar a engenharia Exata que construiu ML.xtrain_horario_t_2026_V7
# ====================================================================================
base_cols = [c for c in df_live.columns if c != "datahora"]
rain_feat_cols = [c for c in base_cols if c.startswith("rain_")]
meteo_cols = ["hi_temp", "out_hum", "wind_speed", "vento_num", "bar"] + rain_feat_cols

X_parts = []
# 4.1 Lags
for lag in range(1, 6):
    X_parts.append(df_live[base_cols].shift(lag).add_suffix(f"_lag{lag}"))
# 4.2 MAs
for w in [3, 6]:
    X_parts.append(df_live[base_cols].rolling(w).mean().shift(1).add_suffix(f"_ma{w}"))
for w in [12, 24, 48]:
    X_parts.append(df_live[rain_feat_cols].rolling(w).mean().shift(1).add_suffix(f"_ma{w}"))
# 4.3 LEADS
for lead in range(1, 6):
    X_parts.append(df_live[meteo_cols].shift(-lead).add_suffix(f"_lead{lead}"))

X_live_panel = pd.concat(X_parts, axis=1)

# Onde está o T0? 
# Como adicionamos as 5 linhas fantasmas no fundo de df_live, nosso T0 verdadeiro
# está ancorado a exatos 5 índices da base! (.iloc[-6])
idx_t0 = len(df_live) - 6

# Validando
linha_t0 = df_live.loc[idx_t0, "datahora"]
print(f"  ✓ Extraindo Vetorial de {linha_t0} (O T0 Absoluto)")

X_t0_bruto = X_live_panel.iloc[idx_t0].fillna(0).values.astype("float32")

# Dimension validation against Scaler
if len(X_t0_bruto) < scaler_X.n_features_in_:
    X_t0_bruto = np.pad(X_t0_bruto, (0, scaler_X.n_features_in_ - len(X_t0_bruto)), constant_values=0)
elif len(X_t0_bruto) > scaler_X.n_features_in_:
    X_t0_bruto = X_t0_bruto[:scaler_X.n_features_in_]

# ====================================================================================
# 5. INFERÊNCIA CEGA EM ALTA PERFOMANCE
# ====================================================================================
print("\n[4/5] Executando Matemática de Previsão...")

# Scala e Injeta
X_n = scaler_X.transform(X_t0_bruto.reshape(1, -1))
pred_n = model_lgbm.predict(X_n)
pred_raw = scaler_y.inverse_transform(np.atleast_2d(pred_n))[0]

# O Lightgbm _V7 devolveu um array "Flat" na mesma ordem dos Ys que criamos:
# y_cols = ['int6_h1', 'intSup_h1', 'int3_h1', 'int6_h2', 'intSup_h2', 'int3_h2']
# Precisamos das Posições de Superfície (Indice 1 = H1, Indice 4 = H2)
intensidade_sup_h1 = pred_raw[1]
intensidade_sup_h2 = pred_raw[4]

datahora_h1 = t0_hora + pd.Timedelta(hours=1)
datahora_h2 = t0_hora + pd.Timedelta(hours=2)

df_previsoes_finais = pd.DataFrame([
    {"datahora_alvo": datahora_h1, "previsao_correnteza_superficie": round(float(intensidade_sup_h1), 4), "horizonte": "+1h"},
    {"datahora_alvo": datahora_h2, "previsao_correnteza_superficie": round(float(intensidade_sup_h2), 4), "horizonte": "+2h"},
])

print("\n  ┌─────────────────────┬───────────┬────────────────────────────┐")
print("  │   Hora Alvo         │ Horizonte │ Previsão V7 Result   (kt)  │")
print("  ├─────────────────────┼───────────┼────────────────────────────┤")
for _, r in df_previsoes_finais.iterrows():
    print(f"  │ {r['datahora_alvo']}    │   {r['horizonte']}   │          {r['previsao_correnteza_superficie']:>8.4f}            │")
print("  └─────────────────────┴───────────┴────────────────────────────┘")


# ====================================================================================
# 6. ESCRITA NO BIGQUERY BIGQUERY — MERGE
# ====================================================================================
print("\n[5/5] Subindo Inferências com Checksum Primeiro_Calculo para BQ...")

df_temp = df_previsoes_finais[["datahora_alvo", "previsao_correnteza_superficie"]].copy()
df_temp["datahora_alvo"] = pd.to_datetime(df_temp["datahora_alvo"]).dt.tz_localize(None)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("datahora_alvo", "TIMESTAMP"),
        bigquery.SchemaField("previsao_correnteza_superficie", "FLOAT64"),
    ],
)
try:
    bq_client.load_table_from_dataframe(df_temp, TABLE_TEMP, job_config=job_config).result()
    print(f"  ✓ Subida para BQ temporário: {len(df_temp)} registros.")

    merge_sql = f"""
    MERGE `{TABLE_PREVISOES}` T
    USING `{TABLE_TEMP}` S
    ON T.datahora_alvo = S.datahora_alvo
    WHEN MATCHED THEN
    UPDATE SET
        T.correnteza_superficie_prevista_dinamica = S.previsao_correnteza_superficie,
        T.atualizado_em = CURRENT_TIMESTAMP(),
        T.correnteza_superficie_prevista_primeiro_calculo = IF(
            T.correnteza_superficie_prevista_primeiro_calculo IS NULL
            AND TIMESTAMP_DIFF(S.datahora_alvo, CAST(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS TIMESTAMP), MINUTE) BETWEEN 90 AND 150,
            S.previsao_correnteza_superficie,
            T.correnteza_superficie_prevista_primeiro_calculo
        )
    WHEN NOT MATCHED THEN
    INSERT (datahora_alvo, correnteza_superficie_prevista_dinamica, correnteza_superficie_prevista_primeiro_calculo, atualizado_em)
    VALUES (
        S.datahora_alvo, 
        S.previsao_correnteza_superficie, 
        IF(TIMESTAMP_DIFF(S.datahora_alvo, CAST(DATETIME(CURRENT_TIMESTAMP(), 'America/Sao_Paulo') AS TIMESTAMP), MINUTE) BETWEEN 90 AND 150, S.previsao_correnteza_superficie, NULL),
        CURRENT_TIMESTAMP()
    )
    """
    bq_client.query(merge_sql).result()
    print("  ✓ MERGE ATIVO: Gravação Inteligente concluída.")
except Exception as e:
    print(f"  ⚠ Erro Crítico no BQ Merge: {e}")

print("\n" + "="*58)
print(" ✅ V7 PRODUZIDA E DEPLOYADA AO VIVO ")
print("="*58)
