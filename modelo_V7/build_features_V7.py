# %% [markdown]
# # V7 - Etapa 1: Feature Engineering e Criação de Leads Mágicos no BigQuery
# 
# **Arquitetura V7:**
# A grande novidade desta versão é a inclusão do "Futuro Climático".
# Em vez do modelo depender exclusivamente dos lags (histórico passado) da inércia da água,
# passamos explicitamente as previsões atmosféricas futuras de vento, temperatura, pressão e chuva
# como features "Lead" (+1h a +5h). O modelo LightGBM enxergará o futuro meteorológico para 
# decidir sozinho as dinâmicas da bacia.

import os, time, pandas as pd, requests
from datetime import date
from google.oauth2 import service_account
from pandas_gbq import read_gbq, to_gbq
import warnings

warnings.filterwarnings('ignore')

print("==========================================================")
print("  Iniciando Construção de Matriz V7 (Com Leads Climáticos) ")
print("==========================================================")

# ====================================================================================
# 1. CREDENCIAIS E CONFIGURAÇÕES
# ====================================================================================
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"
RAW_BQ_TABLE = "ML.xtrain_horario_t_2026"  # Base Original de Onde partimos
TARGET_BQ_TABLE = "ML.xtrain_horario_t_2026_V7" # Para Onde Enviaremos a matriz pronta
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

CIDADES = [
    ("PORTO_ALEGRE", -30.0325, -51.2304), ("CANOAS", -29.9216, -51.1800),
    ("SAO_LEOPOLDO", -29.7544, -51.1516), ("NOVO_HAMBURGO", -29.6906, -51.1429),
    ("GRAVATAI", -29.9440, -50.9931), ("SANTA_MARIA", -29.6861, -53.8069),
    ("CACHOEIRA_SUL", -30.0482, -52.8902), ("SANTA_CRUZ_SUL", -29.7142, -52.4286),
    ("RIO_GRANDE", -32.035, -52.0986),
]

# ====================================================================================
# 2. BUSCA DO ARQUIVO HISTÓRICO DE CHUVA (OPEN-METEO)
# ====================================================================================
def fetch_rain_hourly(lat, lon, start: date, end: date, pause=0.5) -> pd.DataFrame:
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
            print(f"Erro Rain {cur}: {e}")
        cur = date(cur.year + 1, 1, 1)
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

print("\n(1/5) Baixando Chuva da OpenMeteo (Archive)...")
START_RAIN, END_RAIN = date(2020, 1, 1), date.today()
rain_list = []
for nome, lat, lon in CIDADES:
    print(f"      Puxando {nome}...")
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
else:
    df_rain = pd.DataFrame()

# ====================================================================================
# 3. LEITURA DA BASE CRUA NO BIGQUERY
# ====================================================================================
print(f"\n(2/5) Extraindo dados de Correnteza e Boia ({RAW_BQ_TABLE})...")
query = f"SELECT * FROM `{PROJECT_ID}.{RAW_BQ_TABLE}`"
df_bq = read_gbq(query, project_id=PROJECT_ID, credentials=credentials, dialect="standard")
df_bq["datahora"] = pd.to_datetime(df_bq["datahora"])

# ====================================================================================
# 4. PREPARAÇÃO E MERGE
# ====================================================================================
print("\n(3/5) Formatando Tipos e Consolidando (Merge)...")
if not df_rain.empty:
    df_bq["datahora_h"] = df_bq["datahora"].dt.floor("H")
    df_rain["datahora_h"] = df_rain["datahora"].dt.floor("H")
    df_merged = df_bq.merge(df_rain.drop(columns=["datahora"]), on="datahora_h", how="left")
    df_merged = df_merged.drop(columns=["datahora_h"])
else:
    df_merged = df_bq.copy()

df_merged = df_merged.sort_values("datahora").reset_index(drop=True)

# Limpeza de colunas inúteis
cols_drop = ["temp_out"] + [c for c in df_merged.columns if "wind_dir" in c]
df_merged.drop(columns=[c for c in cols_drop if c in df_merged.columns], inplace=True)

# Tipagem garantida
cols_conv = df_merged.columns.drop("datahora", errors="ignore")
df_merged[cols_conv] = df_merged[cols_conv].apply(pd.to_numeric, errors="coerce").fillna(0).astype("float32")

# Correção fundamental do V6: Discretizar Coordenadas de Vento (0-360) para Quadrantes (0-15)!
print("      Convertendo Graus(0-360) para Categorias Setoriais(0-15)...")
for col in ["direcao_6m_deg", "direcao_superficie_deg", "direcao_3m_deg", "vento_num"]:
    if col in df_merged.columns:
        df_merged[col] = (df_merged[col] / 22.5).round() % 16

# Remoção de Vazante/Enchente (A boia só manda grandezas absolutas)
print("      Ajustando Vazante / Enchente com Vetor...")
for int_col, dir_col in [("intensidade_6m_kt","direcao_6m_deg"),
                          ("intensidade_superficie_kt","direcao_superficie_deg"),
                          ("intensidade_3m_kt","direcao_3m_deg")]:
    if int_col in df_merged.columns and dir_col in df_merged.columns:
        mask = (df_merged[dir_col] > 90) & (df_merged[dir_col] < 270)
        df_merged.loc[mask, int_col] *= -1

TARGET_BASE = ["intensidade_6m_kt", "intensidade_superficie_kt", "intensidade_3m_kt"]
y_base_cols = [c for c in TARGET_BASE if c in df_merged.columns]

# Remove momentos mortos do sensor
df_merged = df_merged[df_merged[y_base_cols].sum(axis=1) != 0].reset_index(drop=True)

# ====================================================================================
# 5. FEATURE ENGINEERING (MAGIA DO V7: LEADS E LAGS)
# ====================================================================================
print("\n(4/5) Feature Engineering (Lags e *LEADS FUTUROS*)...")
base_cols = [c for c in df_merged.columns if c != "datahora"]
rain_feat_cols = [c for c in base_cols if c.startswith("rain_")]

# As métricas meteorológicas puras ganharão LEADS (olhar 5h no futuro)
meteo_cols = ["hi_temp", "out_hum", "wind_speed", "vento_num", "bar"] + rain_feat_cols

X_parts = []
# 5.1. LAGS Padrões (Passado de 1 a 5h atrás: garante Inércia d'água)
for lag in range(1, 6):
    Xl = df_merged[base_cols].shift(lag)
    Xl.columns = [f"{c}_lag{lag}" for c in base_cols]
    X_parts.append(Xl)

# 5.2. Médias Móveis (Inércia mais suavizada de vento e correnteza)
for w in [3, 6]:
    Xm = df_merged[base_cols].rolling(w).mean().shift(1)
    Xm.columns = [f"{c}_ma{w}" for c in base_cols]
    X_parts.append(Xm)

for w in [12, 24, 48]:
    if rain_feat_cols:
        Xr = df_merged[rain_feat_cols].rolling(w).mean().shift(1)
        Xr.columns = [f"{c}_ma{w}" for c in rain_feat_cols]
        X_parts.append(Xr)

# 5.3 A ARMA SECRETA V7: *LEADS CLIMÁTICOS*
# Vamos puxar do amanhã (1 a 5h pra frente) todos os ventos, umidades, pressões e chuvas
# Isso fará o LightGBM cruzar exatamente a atmosfera futura com o momento do alvo!
print("      Adicionando matrizes Futuras de Clima e Chuva (1 a 5 h)...")
for lead in range(1, 6):
    X_lead = df_merged[meteo_cols].shift(-lead)
    X_lead.columns = [f"{c}_lead{lead}" for c in meteo_cols]
    X_parts.append(X_lead)

X_o = pd.concat(X_parts, axis=1)

# 5.4 Múltiplos Horizontes de Label (+1H a +6H) para o V7
y_parts = {}
for col in y_base_cols:
    y_parts[f"{col}_h1"] = df_merged[col].shift(-1)
    y_parts[f"{col}_h2"] = df_merged[col].shift(-2)
    # y_parts[f"{col}_h3"] = df_merged[col].shift(-3)  -- manteremos h1 e h2 pro ML nao ficar gigante (inferencias dinâmicas)
    
y_multi = pd.DataFrame(y_parts)
y_cols = list(y_multi.columns)

data_final = pd.concat([df_merged[["datahora"]], X_o, y_multi], axis=1)

# Descarta linhas onde as Lags ou LEADS estouraram NaN
# Se pegamos shift(-5) as ultimas 5 linhas sempre terão nulo, dropamos!
data_final = data_final.dropna()

print(f"      Matriz de Treino Definitiva V7 Construída: {data_final.shape}")


# ====================================================================================
# 6. EXPORTAÇÃO (UPLOAD) PARA O BIGQUERY
# ====================================================================================
print(f"\n(5/5) Salvando Matriz Treinamento Oficial V7 no BigQuery...")
print(f"      Destino: {TARGET_BQ_TABLE}")

# to_gbq converte a matriz magicamente sobrepondo se já houver
try:
    to_gbq(
        dataframe=data_final,
        destination_table=TARGET_BQ_TABLE,
        project_id=PROJECT_ID,
        if_exists='replace',
        credentials=credentials
    )
    print("==========================================================")
    print("✓ SUCESSO ABSOLUTO! BASE SALVA NA NUVEM! ")
    print("Agora tudo está pronto para o modelo rodar do Storage!")
    print("==========================================================")
except Exception as e:
    print(f"⚠ ERRO NO UPLOAD BIGQUERY: {e}")
