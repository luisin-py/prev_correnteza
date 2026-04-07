# %% [markdown]
# # V7 - Etapa 2: Treinamento do Modelo LightGBM Puxando Nuvem (BigQuery)
# 
# **Arquitetura V7:**
# Este script tem a função EXCLUSIVA de treinamento!
# Ele irá baixar o DataSet limpo e já construído ("ML.xtrain_horario_t_2026_V7") do Cloud,
# fazer o Scalling e treinar a matemática do SciKit-Learn/LightGBM nele.
# 
# Com os "Leads Mágicos" do clima na base, o MultiOutputRegressor não precisa mais testar 
# tanta aleatoriedade no futuro distante.

import os, time, joblib
import numpy as np
import pandas as pd
from pandas_gbq import read_gbq
from google.oauth2 import service_account
from sklearn.preprocessing import MinMaxScaler
from sklearn.multioutput import MultiOutputRegressor
from lightgbm import LGBMRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import warnings

warnings.filterwarnings('ignore')

print("==========================================================")
print("  ML V7: Download BQ & Train Model (LightGBM)")
print("==========================================================")

# ====================================================================================
# 1. CREDENCIAIS E CONFIGURAÇÕES
# ====================================================================================
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"
BQ_ML_TABLE = "ML.xtrain_horario_t_2026_V7"
OUTPUT_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V7"

os.makedirs(OUTPUT_DIR, exist_ok=True)
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

# ====================================================================================
# 2. BAIXANDO DATASET DO BIGQUERY
# ====================================================================================
print(f"\n(1/4) Lendo DataSet de Treinamento da Nuvem (`{BQ_ML_TABLE}`)...")
query = f"SELECT * FROM `{PROJECT_ID}.{BQ_ML_TABLE}`"
df = read_gbq(query, project_id=PROJECT_ID, credentials=credentials, dialect="standard")
df["datahora"] = pd.to_datetime(df["datahora"])
df = df.sort_values("datahora").reset_index(drop=True)

print(f"      > Sucesso! Matriz recuperada com {df.shape[0]} linhas e {df.shape[1]} features.")

# ====================================================================================
# 3. SPLIT DE FEATURES E TARGETS
# ====================================================================================
print("\n(2/4) Separando Lags, Leads e o Y (Alvo)...")
# Como definimos targets_h1 e targets_h2 terminando com "_h1" ou "_h2" e "datahora":
y_cols = [c for c in df.columns if c.endswith("_h1") or c.endswith("_h2")]
base_drop = ["datahora"] + y_cols

X_clean = df.drop(columns=base_drop)
y_clean = df[y_cols]

print(f"      Features X (Dinâmica + Leads): {X_clean.shape[1]}")
print(f"      Targets  y (Correntezas h1,h2): {y_clean.shape[1]}")

# Divisão Temporal: 80% Treino | 10% Validação | 10% Teste
N = len(X_clean)
i_val, i_test = int(N * 0.80), int(N * 0.90)

trainX, valX, testX = X_clean.iloc[:i_val], X_clean.iloc[i_val:i_test], X_clean.iloc[i_test:]
trainY, valY, testY = y_clean.iloc[:i_val], y_clean.iloc[i_val:i_test], y_clean.iloc[i_test:]

print(f"      Matrizes de Corte:\n      Treino: {trainX.shape[0]}\n      Validação: {valX.shape[0]}\n      Teste: {testX.shape[0]}")

# Normalização de Scaler (Crucial para Gradientes e Redes Neurais!)
scaler_X, scaler_y = MinMaxScaler(), MinMaxScaler()
trainX_n = scaler_X.fit_transform(trainX)
valX_n   = scaler_X.transform(valX)
testX_n  = scaler_X.transform(testX)

trainY_n = scaler_y.fit_transform(trainY)
valY_n   = scaler_y.transform(valY)
testY_n  = scaler_y.transform(testY)

# ====================================================================================
# 4. TREINAMENTO LIGHTGBM INTELIGENTE
# ====================================================================================
print("\n(3/4) Iniciando Treinamento Árvores Histograma (LightGBM_V7)...")
t0 = time.time()
# Diferente do XGBoost comum, o LGBM cresce por folha e entende perfeitamente grandes arrays!
# O MultiOutput garante que os 6 caminhos de profundidade e horizonte (h1,h2 * 3 prof) sejam resolvidos.
lgbm_model = MultiOutputRegressor(
    LGBMRegressor(
        random_state=42, 
        n_jobs=-1,
        verbose=-1, 
        n_estimators=500, 
        learning_rate=0.05,
        num_leaves=31
    ), 
    n_jobs=-1
)

lgbm_model.fit(trainX_n, trainY_n)
print(f"      > Concluído em {time.time()-t0:.1f}s")


# ====================================================================================
# 5. SALVANDO MODELOS E SCALERS
# ====================================================================================
print("\n(4/4) Gerando Modelos Binários e Evaluando...")

joblib.dump(lgbm_model, os.path.join(OUTPUT_DIR, "modelo_LightGBM.joblib"))
joblib.dump(scaler_X, os.path.join(OUTPUT_DIR, "scaler_X.joblib"))
joblib.dump(scaler_y, os.path.join(OUTPUT_DIR, "scaler_y.joblib"))

# Métrica rápida de prova
pred_test_n = lgbm_model.predict(testX_n)
pred_test = scaler_y.inverse_transform(pred_test_n)

# Como y_clean são os targets (e sabemos a ordem), podemos ver a métrica do `intensidade_superficie_kt_h1`
try:
    idx_sup_h1 = y_cols.index("intensidade_superficie_kt_h1")
    r2_sup_h1 = r2_score(testY.iloc[:, idx_sup_h1], pred_test[:, idx_sup_h1])
    mae_sup_h1 = mean_absolute_error(testY.iloc[:, idx_sup_h1], pred_test[:, idx_sup_h1])
    
    print("==========================================================")
    print("  RESULTADOS NO TESTE CEGO (SUPERFÍCIE +1H):")
    print(f"  R² Score: {r2_sup_h1:.4f}  |  MAE Erro Abs: {mae_sup_h1:.3f} kt")
    print("==========================================================")
except ValueError:
    print("Coluna de superfície h1 não encontrada para métricas específicas.")

print(f"\n Tudo pronto. A Versão 7 pode ser iniciada agora baseada em: {OUTPUT_DIR}")
