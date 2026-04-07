import os, sys, warnings, joblib
import pandas as pd
import numpy as np
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery

warnings.filterwarnings("ignore")

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"
MODEL_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V6"
TABLE_HISTORICO = f"{PROJECT_ID}.wherehouse_tratado.mestre_hour_tratada"
TABLE_PREVISOES = f"{PROJECT_ID}.wherehouse_tratado.previsoes_oficiais"

bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(CREDENTIALS_PATH), project=PROJECT_ID)
scaler_X = joblib.load(os.path.join(MODEL_DIR, "scaler_X.joblib"))
scaler_y = joblib.load(os.path.join(MODEL_DIR, "scaler_y.joblib"))
model    = joblib.load(os.path.join(MODEL_DIR, "modelo_LightGBM.joblib"))

BASE_COLS = [
    "previsao", "hi_temp", "out_hum", "wind_speed", "vento_num", "bar",
    "fase_lua", "direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg",
    "intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt",
    "altura_mare",
    "rain_PORTO_ALEGRE", "rain_CANOAS", "rain_SAO_LEOPOLDO",
    "rain_NOVO_HAMBURGO", "rain_GRAVATAI", "rain_SANTA_MARIA",
    "rain_CACHOEIRA_SUL", "rain_SANTA_CRUZ_SUL", "rain_RIO_GRANDE",
]
TARGET_BASE = ["intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt"]
Y_COLS = [
    "intensidade_6m_kt_h1", "intensidade_6m_kt_h2",
    "intensidade_superficie_kt_h1", "intensidade_superficie_kt_h2",
    "intensidade_3m_kt_h1", "intensidade_3m_kt_h2",
]
idx_sup_h1 = Y_COLS.index("intensidade_superficie_kt_h1")
idx_sup_h2 = Y_COLS.index("intensidade_superficie_kt_h2")


print(">> Baixando histórico total...")
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
LIMIT 120
"""
df_full = bq_client.query(query_historico).to_dataframe()
df_full["datahora"] = pd.to_datetime(df_full["datahora"])
df_full = df_full.sort_values("datahora").reset_index(drop=True)

# Chuva (só mocks de zeros pra simplificar no test limit/backfill)
for c in BASE_COLS:
    if c.startswith("rain_"):
        df_full[c] = 0.0

# Preprocess Degrees -> 16 regions
for col in ["direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg", "vento_num"]:
    if col in df_full.columns:
        df_full[col] = (df_full[col] / 22.5).round() % 16

today_start = pd.Timestamp.now().floor('d')
today_end = pd.Timestamp.now().floor('h')

results = []

def predict_2h(X_row):
    X_2d = X_row.reshape(1, -1)
    X_n = scaler_X.transform(X_2d)
    raw = model.predict(X_n)
    return scaler_y.inverse_transform(np.atleast_2d(raw))[0]

def update_lags(X_row, pred_prev, feature_names):
    X_new = X_row.copy()
    for i, col in enumerate(TARGET_BASE):
        h1_pred = pred_prev[i * 2]
        h2_pred = pred_prev[i * 2 + 1]
        for lag_n, lag_val in [(1, h2_pred), (2, h1_pred)]:
            lag_col = f"{col}_lag{lag_n}"
            if lag_col in feature_names:
                X_new[feature_names.index(lag_col)] = lag_val
        for ma_w in [3, 6]:
            ma_col = f"{col}_ma{ma_w}"
            if ma_col in feature_names:
                hist_lags = [X_new[feature_names.index(f"{col}_lag{l}")] for l in range(3, min(ma_w + 1, 6)) if f"{col}_lag{l}" in feature_names]
                window_vals = [h2_pred, h1_pred] + hist_lags
                if len(window_vals) > 0:
                    X_new[feature_names.index(ma_col)] = np.mean(window_vals[:ma_w])
    return X_new

def inject_meteo(X_row, hour_offset, feature_names, df_ow_interp_local, t_current):
    if df_ow_interp_local is None or df_ow_interp_local.empty:
        return X_row
    X_new = X_row.copy()
    target_time = t_current + pd.Timedelta(hours=hour_offset)
    meteo_mapping = {"wind_speed": "wind_speed", "pressure": "bar", "humidity": "out_hum", "feels_like": "hi_temp"}
    for ow_col, model_col in meteo_mapping.items():
        if ow_col in df_ow_interp_local.columns:
            time_diffs = (df_ow_interp_local["datahora"] - target_time).abs()
            if time_diffs.min() <= pd.Timedelta(hours=1):
                closest_idx = time_diffs.idxmin()
                val = df_ow_interp_local.loc[closest_idx, ow_col]
                lag1_col = f"{model_col}_lag1"
                if lag1_col in feature_names:
                    X_new[feature_names.index(lag1_col)] = val
    return X_new

current_simulated_times = pd.date_range(start=today_start, end=today_end, freq='1h')

for t_current in current_simulated_times:
    print(f"Simulando t0 = {t_current}")
    # 1. df base
    df_base = df_full[df_full["datahora"] <= t_current].copy().reset_index(drop=True)
    if len(df_base) < 48:
        continue
    
    # 2. df_ow simulado do futuro real da propria boia
    df_ow_interp = df_full[(df_full["datahora"] >= t_current) & (df_full["datahora"] <= t_current + pd.Timedelta(hours=9))].copy()
    df_ow_interp = df_ow_interp.rename(columns={"bar": "pressure", "out_hum": "humidity", "hi_temp": "feels_like"})
    
    # 3. Engenharia de features
    X_parts = []
    v5_base_cols = [c for c in BASE_COLS]
    rain_feat_cols = [c for c in v5_base_cols if c.startswith("rain_")]
    
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
    
    last_valid_idx = df_base.index[-1]
    X_t0 = X_o.loc[last_valid_idx].fillna(0).values.astype("float32")
    expected_features = scaler_X.n_features_in_
    if len(X_t0) < expected_features:
        X_t0 = np.pad(X_t0, (0, expected_features - len(X_t0)), constant_values=0)
    else:
        X_t0 = X_t0[:expected_features]
    
    feature_names = list(X_o.columns)
    while len(feature_names) < expected_features: feature_names.append(f"_pad_{len(feature_names)}")
    feature_names = feature_names[:expected_features]
    
    # RUN RECURSION
    pred_r1 = predict_2h(X_t0)
    
    X_r2 = update_lags(X_t0.copy(), pred_r1, feature_names)
    X_r2 = inject_meteo(X_r2, 3, feature_names, df_ow_interp, t_current)
    pred_r2 = predict_2h(X_r2)
    
    X_r3 = update_lags(X_r2.copy(), pred_r2, feature_names)
    X_r3 = inject_meteo(X_r3, 5, feature_names, df_ow_interp, t_current)
    pred_r3 = predict_2h(X_r3)
    
    # Save results
    for h_offset, pred, h1h2 in [
        (1, pred_r1, "h1"), (2, pred_r1, "h2"),
        (3, pred_r2, "h1"), (4, pred_r2, "h2"),
        (5, pred_r3, "h1"), (6, pred_r3, "h2"),
    ]:
        val = float(pred[idx_sup_h1]) if h1h2 == "h1" else float(pred[idx_sup_h2])
        # Primeiro calculo entra aqui se h_offset for 2 (pois eh 2 horas a frente)
        primeiro_calc = val if h_offset == 2 else np.nan
        results.append({
            "datahora_alvo": t_current + pd.Timedelta(hours=h_offset),
            "correnteza_superficie_prevista_dinamica": val,
            "correnteza_superficie_prevista_primeiro_calculo": primeiro_calc,
            "atualizado_em": datetime.utcnow()
        })

df_res = pd.DataFrame(results)

# Clean duplicates by taking the latest created dynamic pred, and if there's any not-nan primeiro_calc, we keep it
df_final_list = []
for alvo, group in df_res.groupby("datahora_alvo"):
    # Dinamica: pega o de menor lead_time (o que foi gerado mais recentemente, logo no fim do group pois iteramos cronologicamente)
    last_row = group.iloc[-1]
    dinamica = last_row["correnteza_superficie_prevista_dinamica"]
    # Primeiro_calculo: pega o primeiro que nao eh nulo no grupo
    primeiro = group["correnteza_superficie_prevista_primeiro_calculo"].dropna().first_valid_index()
    if primeiro is not None:
        val_primeiro = group.loc[primeiro, "correnteza_superficie_prevista_primeiro_calculo"]
    else:
        val_primeiro = dinamica # fallback
    
    df_final_list.append({
        "datahora_alvo": alvo,
        "correnteza_superficie_prevista_dinamica": round(dinamica, 4),
        "correnteza_superficie_prevista_primeiro_calculo": round(val_primeiro, 4),
        "atualizado_em": pd.Timestamp.now()
    })

df_final = pd.DataFrame(df_final_list)
print(f"Subindo {len(df_final)} linhas sumarizadas do Backfill de hoje para o BQ...")

# Usa o client pra jogar na tabela_oficial!
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    schema=[
        bigquery.SchemaField("datahora_alvo", "TIMESTAMP"),
        bigquery.SchemaField("correnteza_superficie_prevista_dinamica", "FLOAT64"),
        bigquery.SchemaField("correnteza_superficie_prevista_primeiro_calculo", "FLOAT64"),
        bigquery.SchemaField("atualizado_em", "TIMESTAMP"),
    ],
)
df_final["datahora_alvo"] = pd.to_datetime(df_final["datahora_alvo"]).dt.tz_localize(None)
df_final["atualizado_em"] = pd.to_datetime(df_final["atualizado_em"]).dt.tz_localize(None)

try:
    # Insere as previsoes. MERGE seria melhor mas APPEND eh mais robusto como backfill de emergência.
    # Mas como pediu pra atualizar, a gente faz um MERGE pra garantir.
    TABLE_TEMP = f"{PROJECT_ID}.wherehouse_tratado.temp_previsoes_backfill"
    bq_client.load_table_from_dataframe(df_final, TABLE_TEMP, job_config=job_config).result()
    
    merge_sql = f"""
    MERGE `{TABLE_PREVISOES}` T
    USING `{TABLE_TEMP}` S
    ON T.datahora_alvo = S.datahora_alvo
    WHEN MATCHED THEN
      UPDATE SET
        T.correnteza_superficie_prevista_dinamica = S.correnteza_superficie_prevista_dinamica,
        T.correnteza_superficie_prevista_primeiro_calculo = COALESCE(T.correnteza_superficie_prevista_primeiro_calculo, S.correnteza_superficie_prevista_primeiro_calculo),
        T.atualizado_em = S.atualizado_em
    WHEN NOT MATCHED THEN
      INSERT (datahora_alvo, correnteza_superficie_prevista_dinamica, correnteza_superficie_prevista_primeiro_calculo, atualizado_em)
      VALUES (S.datahora_alvo, S.correnteza_superficie_prevista_dinamica, S.correnteza_superficie_prevista_primeiro_calculo, S.atualizado_em)
    """
    bq_client.query(merge_sql).result()
    print("✓ BACKFILL CONCLUÍDO COM SUCESSO!")
except Exception as e:
    print("ERRO:", e)
