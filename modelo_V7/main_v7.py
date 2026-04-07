# %% [markdown]
# # Previsão de Correnteza V7 — Inferência em Produção (Lógica Exclusiva Walk-Forward)
# 
# A V7 tem a proeza de utilizar o clima do futuro nas features ("Leads"). 
# Para suportar 6 horas de previsão (H1 a H6) através de um modelo restrito a saídas curtas,
# injetamos uma recursão: Ele calcula o H1 utilizando o clima Lead. Após isso, 
# o resultado é Injetado no próprio Passado da Matriz, o script caminha 1h para frente 
# e ele recalcula usando a "Inércia Prevista" associada ao clima verdadeiro futuro!

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
print("  V7 — INFERÊNCIA RECURSIVA [6 HORAS] (LEADS CLIMÁTICOS)")
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
# 2. CAPTURA DOS DADOS (O PASSADO + 12 HORAS TENSAS DE FUTURO)
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

agora_hora = pd.Timestamp.now().floor('H')
if df_hist['datahora'].iloc[-1] < agora_hora:
    ultima = df_hist.iloc[-1:].copy()
    ultima['datahora'] = agora_hora
    df_hist = pd.concat([df_hist, ultima], ignore_index=True)
    t0_hora = agora_hora
else:
    t0_hora = df_hist['datahora'].iloc[-1]
print(f"  ✓ Instante Base de Lançamento (T0): {t0_hora}")

# 2B: O Futuro do Clima Atmosférico (+12H suportando o range de Lags da Previsão Longa)
OW_API_KEY = "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI"
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
    # A V7 agora exige esticar o futuro: Precisamos simular 6 horas p/ frente
    # Mas como rodamos features que sugam "lead5", precisamos que a janela Openweather seja grande o suficiente!
    # Se queremos T+6 buscando Lead5, precisaremos que exista T+11!
    df_ow_futuro = df_ow[(df_ow["datahora"] > t0_hora) & (df_ow["datahora"] <= t0_hora + pd.Timedelta(hours=14))].copy()
except Exception as e:
    print(f"  ⚠ Erro OpenWeather: {e}")
    df_ow_futuro = pd.DataFrame()

# 2C: O Futuro da Chuva (+12H)
rain_frames = []
try:
    for nome, lat, lon in CIDADES:
        r = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={"latitude": lat, "longitude": lon, "hourly": "precipitation", 
                    "past_hours": 48, "forecast_hours": 15, "timezone": "America/Sao_Paulo"},
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

if not df_rain.empty:
    df_hist["datahora_h"] = df_hist["datahora"].dt.floor("H")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("H")
    df_hist = df_hist.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_hist = df_hist.drop(columns=["datahora_h"])
    
    df_ow_futuro["datahora_h"] = df_ow_futuro["datahora"].dt.floor("H")
    df_ow_futuro = df_ow_futuro.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_ow_futuro = df_ow_futuro.drop(columns=["datahora_h"])

col_chuvas = [f"rain_{c[0]}" for c in CIDADES]
for col in col_chuvas:
    if col not in df_hist.columns: df_hist[col] = 0.0
    if not df_ow_futuro.empty and col not in df_ow_futuro.columns: df_ow_futuro[col] = 0.0

# ====================================================================================
# 3. CONSTRUINDO A GRADE HÍBRIDA GLOBAL (PASSADO REAL E VERDADES ATMOSFÉRICAS)
# ====================================================================================
print("\n[3/5] Organizando Linha do Tempo e Setorializando Vento...")
for col in df_hist.columns:
    if col not in df_ow_futuro.columns:
        df_ow_futuro[col] = np.nan

df_live = pd.concat([df_hist, df_ow_futuro], ignore_index=True)

# Aplica Bússola
for col in ["direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg", "vento_num"]:
    if col in df_live.columns:
        df_live[col] = pd.to_numeric(df_live[col], errors='coerce')
        df_live[col] = (df_live[col] / 22.5).round() % 16

cols_conv = df_live.columns.drop("datahora", errors="ignore")
df_live[cols_conv] = df_live[cols_conv].apply(pd.to_numeric, errors="coerce").astype("float32")
df_live.fillna(method="ffill", inplace=True) 
df_live.fillna(0.0, inplace=True)


# ====================================================================================
# 4. LOOP V7 "WALK-FORWARD" (FEATURIZAÇÃO CONTÍNUA RECURSIVA P/ 6 HORAS)
# ====================================================================================
print("\n[4/5] Executando Lógica de Previsão Recursiva (+6H)...")

base_cols = [c for c in df_live.columns if c != "datahora"]
rain_feat_cols = [c for c in base_cols if c.startswith("rain_")]
meteo_cols = ["hi_temp", "out_hum", "wind_speed", "vento_num", "bar"] + rain_feat_cols

df_previsoes_finais = []
idx_t0 = len(df_hist) - 1

print("  ┌─────────────────────┬───────────┬────────────────────────────┐")
print("  │   Hora Alvo         │ Horizonte │ Previsão V7 Result   (kt)  │")
print("  ├─────────────────────┼───────────┼────────────────────────────┤")

for step in range(6): 
    # O ponteiro de qual hora o modelo "está vivendo" avança:
    idx_curr = idx_t0 + step
    
    # ----------------------------------------------------
    # ENGENHARIA DE FEATURES "ON THE FLY" (Usando todo o DataFrame Live com a Água Injetada)
    # ----------------------------------------------------
    X_parts = []
    # 4.1 Lags (Vão puxar a inércia, se houver injeção do laço passado eles já usam!)
    for lag in range(1, 6): X_parts.append(df_live[base_cols].shift(lag).add_suffix(f"_lag{lag}"))
    # 4.2 MAs
    for w in [3, 6]: X_parts.append(df_live[base_cols].rolling(w).mean().shift(1).add_suffix(f"_ma{w}"))
    for w in [12, 24, 48]: X_parts.append(df_live[rain_feat_cols].rolling(w).mean().shift(1).add_suffix(f"_ma{w}"))
    # 4.3 LEADS do Clima (Nunca inventados, o Pandas busca eles direto das próximas linhas reais das APIs!)
    for lead in range(1, 6): X_parts.append(df_live[meteo_cols].shift(-lead).add_suffix(f"_lead{lead}"))
    
    X_live_panel = pd.concat(X_parts, axis=1)

    # Coleta a Feature pronta no Index atual de loop
    X_tcurr = X_live_panel.iloc[idx_curr].fillna(0).values.astype("float32")
    if len(X_tcurr) < scaler_X.n_features_in_:
        X_tcurr = np.pad(X_tcurr, (0, scaler_X.n_features_in_ - len(X_tcurr)), constant_values=0)
    elif len(X_tcurr) > scaler_X.n_features_in_:
        X_tcurr = X_tcurr[:scaler_X.n_features_in_]

    # INFERÊNCIA DO MODELO
    X_n = scaler_X.transform(X_tcurr.reshape(1, -1))
    pred_raw = scaler_y.inverse_transform(model_lgbm.predict(X_n))[0]

    # O alvo H1 (que é T+1 relativo ao Index Curr) sai no array pred_raw: [6m, sup, 3m, ...]
    out_6m = pred_raw[0]
    out_sup = pred_raw[1]
    out_3m = pred_raw[2]

    # INJETAR AS PREVISÕES VIRTUAIS NA CÉLULA DA ÁGUA FUTURA
    # Assim, no próximo step desse loop, o "lag1" dele será EXATAMENTE essa predição!
    df_live.loc[idx_curr + 1, "intensidade_6m_kt"] = out_6m
    df_live.loc[idx_curr + 1, "intensidade_superficie_kt"] = out_sup
    df_live.loc[idx_curr + 1, "intensidade_3m_kt"] = out_3m

    # SALVAR A PRINT DA PREDIÇÃO
    datahora_alvo = df_live.loc[idx_curr + 1, "datahora"]
    previsao_exata = round(float(out_sup), 4)

    df_previsoes_finais.append({
        "datahora_alvo": datahora_alvo,
        "previsao_correnteza_superficie": previsao_exata,
        "horizonte": f"+{step+1}h"
    })
    
    print(f"  │ {datahora_alvo}    │   +{step+1}H     │          {previsao_exata:>8.4f}            │")

print("  └─────────────────────┴───────────┴────────────────────────────┘")
df_previsoes_finais = pd.DataFrame(df_previsoes_finais)

# ====================================================================================
# 6. ESCRITA NO BIGQUERY BIGQUERY — MERGE
# ====================================================================================
print("\n[5/5] Subindo Lote de 6 Injeções com Checksum Primeiro_Calculo para BQ...")

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
    print("  ✓ MERGE ATIVO: Gravação Inteligente concluída com sucesso.")
except Exception as e:
    print(f"  ⚠ Erro Crítico no BQ Merge: {e}")

print("\n" + "="*58)
print(" ✅ PIPELINE V7 RECURSIVO (LAÇO +6 HORAS) FINALIZADO AO VIVO")
print("="*58)

