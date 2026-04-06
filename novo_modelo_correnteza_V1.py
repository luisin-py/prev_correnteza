# %% [markdown]
# # Previsão de Correnteza (Intensidade e Direção) a partir do BigQuery
# Neste script extraímos os dados, aplicamos feature engineering (lags e médias móveis)
# e testamos múltiplos modelos progressivamente: Árvores de Decisão, MLP e LSTM.

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from google.oauth2 import service_account
from pandas_gbq import read_gbq
import joblib
import random

# Modelos Phase 1
from sklearn.ensemble import GradientBoostingRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from catboost import CatBoostRegressor
from sklearn.multioutput import MultiOutputRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import MinMaxScaler

# Modelos Phase 2 e 3
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, BatchNormalization, LSTM, Input
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping, ModelCheckpoint

import warnings
warnings.filterwarnings('ignore')

# Fixando sementes para reprodutibilidade
random.seed(42)
np.random.seed(42)
tf.random.set_seed(42)

# %% [markdown]
# ## 1. Autenticação e Extração de Dados
# Lendo diretamente do BigQuery usando a Service Account local (Sem Google Colab Auth).

# %%
# Configuração
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = 'local-bliss-359814'
QUERY_TABLE = 'ML.df_data_20260328' 

# Instanciar a chave local
credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)

query = f"""
SELECT *
FROM `{PROJECT_ID}.{QUERY_TABLE}`
"""

print("Aguarde, extraindo dados diretamente do BigQuery via Service Account...")
try:
    df = read_gbq(query, project_id=PROJECT_ID, credentials=credentials, dialect='standard')
    print("Dados carregados com sucesso. Shape:", df.shape)
except Exception as e:
    print("Erro ao tentar baixar dados do BigQuery:", e)
    print("Para testes offline, substitua esta célula pela importação de um CSV.")

# %% [markdown]
# ## 2. Tratamento de Dados e Engenharia de Features
# Replica os filtros iniciais, preenche NaN e adiciona "lags" e "moving averages". As targets são Intensidade e Direção.

# %%
if 'df' in locals():
    df_modelo = df.copy()
    
    # 2.1 Remover colunas espúrias (Wind Dir, Temp Out, etc)
    cols_to_drop = ['temp_out'] + [c for c in df_modelo.columns if 'wind_dir' in c]
    df_modelo.drop(columns=[c for c in cols_to_drop if c in df_modelo.columns], inplace=True)
    
    # 2.2 Tratamento de datas e tipos numéricos
    if 'datahora' in df_modelo.columns:
        df_modelo['datahora'] = pd.to_datetime(df_modelo['datahora'], errors='coerce')
        
    df_modelo = df_modelo.sort_values("datahora").reset_index(drop=True)
    
    # Transformar tudo o que restou (exceto datahora) para tipos numéricos e preencher NA
    cols_to_convert = df_modelo.columns.drop('datahora', errors='ignore')
    df_modelo[cols_to_convert] = df_modelo[cols_to_convert].apply(pd.to_numeric, errors='coerce')
    df_modelo[cols_to_convert] = df_modelo[cols_to_convert].fillna(0).astype('float32')
    
    # Identificar as colunas Target (Intensidade e Direção)
    col_intensidade = [c for c in df_modelo.columns if 'intensidade' in c][0]
    col_direcao = [c for c in df_modelo.columns if 'direcao' in c][0]
    y_cols = [col_intensidade, col_direcao]
    
    # Filtro essencial do notebook original: remover pontos onde a intensidade é nula
    df_modelo = df_modelo[df_modelo[col_intensidade] != 0].reset_index(drop=True)
    
    # 2.3 Feature Engineering: Lags e Médias Móveis
    base_cols = [c for c in df_modelo.columns if c != 'datahora']
    X_parts = []
    
    # A. Adicionando defasagens temporais (lags) - passados até 5 tempos
    for lag in range(1, 6):
        X_lag = df_modelo[base_cols].shift(lag)
        X_lag.columns = [f"{c}_lag{lag}" for c in base_cols]
        X_parts.append(X_lag)
    
    # B. Adicionando Médias Móveis (moving averages) em janelas de 3 e 6 tempos
    # Aplicamos um '.shift(1)' após o '.rolling()' para garantir que a média seja sobre o passado exclusivo (sem vazamento do futuro).
    for window in [3, 6]:
        X_ma = df_modelo[base_cols].rolling(window=window).mean().shift(1)
        X_ma.columns = [f"{c}_ma{window}" for c in base_cols]
        X_parts.append(X_ma)
    
    # Consolida os dados temporais num DataFrame
    X_o = pd.concat(X_parts, axis=1)
    
    # Define Targets
    y = df_modelo[y_cols]
    
    # Remove instâncias que conterão NaN nos primeiros momentos por causa das janelas de lag/MA
    data_final = pd.concat([X_o, y], axis=1).dropna()
    X_clean = data_final[X_o.columns]
    y_clean = data_final[y_cols]

# %% [markdown]
# ### Split Data com Validação 
# Utilizamos divisão temporal 80% (Treino), 10% (Validação) e 10% (Teste). A validação será útil para o Early Stopping das redes.

# %%
if 'data_final' in locals():
    total_len = len(X_clean)
    split_val = int(total_len * 0.8)
    split_test = int(total_len * 0.9)
    
    trainX = X_clean.iloc[:split_val]
    trainY = y_clean.iloc[:split_val]
    
    valX = X_clean.iloc[split_val:split_test]
    valY = y_clean.iloc[split_val:split_test]
    
    testX = X_clean.iloc[split_test:]
    testY = y_clean.iloc[split_test:]
    
    print(f"SPLIT DOS DADOS --- Treino: {trainX.shape}, Validação: {valX.shape}, Teste: {testX.shape}")
    
    # Normalização MinMax (Ajuste das métricas apenas no dataset de treinamento)
    scaler_X = MinMaxScaler()
    scaler_y = MinMaxScaler()
    
    trainX_norm = scaler_X.fit_transform(trainX)
    valX_norm   = scaler_X.transform(valX)
    testX_norm  = scaler_X.transform(testX)
    
    trainY_norm = scaler_y.fit_transform(trainY)
    valY_norm   = scaler_y.transform(valY)
    testY_norm  = scaler_y.transform(testY)

# %% [markdown]
# ### Métricas e Armazenamento
# Definição das métricas incluindo o Módulo de Erro Percentual Absoluto (Erro < 0.5)

# %%
metrics_records = []
preds_all_models = {}
testY_real = testY.values

def evaluate_model(name, model_preds, real_v):
    """
    Computa MAE, RMSE, R² e a % de previsões com erro < 0.5 por variável alvo.
    Salvando os mesmos dentro de um cache dictionary global (`metrics_records`)
    """
    mae_i = mean_absolute_error(real_v[:, 0], model_preds[:, 0])
    mae_d = mean_absolute_error(real_v[:, 1], model_preds[:, 1])
    
    rmse_i = np.sqrt(mean_squared_error(real_v[:, 0], model_preds[:, 0]))
    rmse_d = np.sqrt(mean_squared_error(real_v[:, 1], model_preds[:, 1]))
    
    r2_i = r2_score(real_v[:, 0], model_preds[:, 0])
    r2_d = r2_score(real_v[:, 1], model_preds[:, 1])
    
    # Calculo % erro Absoluto < 0.5
    erro = np.abs(real_v - model_preds)
    acc_i = np.mean(erro[:, 0] < 0.5) * 100
    acc_d = np.mean(erro[:, 1] < 0.5) * 100
    
    metrics_records.append({
        'Model': name,
        'MAE (Int)': mae_i, 'RMSE (Int)': rmse_i, 'R2 (Int)': r2_i, 'Erro <0.5 (Int)%': acc_i,
        'MAE (Dir)': mae_d, 'RMSE (Dir)': rmse_d, 'R2 (Dir)': r2_d, 'Erro <0.5 (Dir)%': acc_d
    })

# %% [markdown]
# ## 3. Fase 1: Árvores de Decisão & Boosting (Scikit, XGBoost, LightGBM, Catboost)

# %%
tree_models = {
    'Gradient Boosting': MultiOutputRegressor(GradientBoostingRegressor(random_state=42)),
    'XGBoost': MultiOutputRegressor(XGBRegressor(random_state=42, n_jobs=-1)),
    'LightGBM': MultiOutputRegressor(LGBMRegressor(random_state=42, n_jobs=-1, verbose=-1)),
    'CatBoost': MultiOutputRegressor(CatBoostRegressor(random_state=42, verbose=0))
}

for name, model in tree_models.items():
    print(f">> Treinando {name}...")
    model.fit(trainX_norm, trainY_norm)
    
    pred_norm = model.predict(testX_norm)
    pred_real = scaler_y.inverse_transform(pred_norm)
    
    preds_all_models[name] = pred_real
    evaluate_model(name, pred_real, testY_real)
    joblib.dump(model, f'modelo_{name.replace(" ", "_")}.joblib')

# %% [markdown]
# ## 4. Fase 2: Multilayer Perceptron (MLP)
# Construção com camadas Densas e BatchNormalization, treinado com ADAM. 10.000 épocas.

# %%
def build_mlp(input_dim):
    model = Sequential([
        Input(shape=(input_dim,)),
        Dense(220, activation='relu'), BatchNormalization(),
        Dense(200, activation='relu'), BatchNormalization(),
        Dense(150, activation='relu'), BatchNormalization(),
        Dense(100, activation='relu'), BatchNormalization(),
        Dense(80, activation='relu'),  BatchNormalization(),
        Dense(30, activation='relu'),  BatchNormalization(),
        Dense(10, activation='relu'),  BatchNormalization(),
        Dense(5, activation='relu'),   BatchNormalization(),
        Dense(2, activation='linear')
    ])
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
    return model

print("\n>> Treinando Multilayer Perceptron...")
mlp = build_mlp(trainX_norm.shape[1])
# Early Stopping com Validação para prevenir longo overfitting nas 10000 épocas demandadas.
es_mlp = EarlyStopping(monitor='val_loss', patience=300, restore_best_weights=True, verbose=0)

mlp.fit(
    trainX_norm, trainY_norm,
    validation_data=(valX_norm, valY_norm),
    epochs=10000, batch_size=64, verbose=0,
    callbacks=[es_mlp]
)

pred_mlp_norm = mlp.predict(testX_norm, verbose=0)
pred_mlp_real = scaler_y.inverse_transform(pred_mlp_norm)

preds_all_models['MLP'] = pred_mlp_real
evaluate_model('MLP', pred_mlp_real, testY_real)
mlp.save('modelo_MLP.keras')

# %% [markdown]
# ## 5. Fase 3: Long Short-Term Memory (LSTM)
# Reformatação 3D `[samples, timesteps, features]`. Mesma topologia densa da MLP.

# %%
# Transformar dados tubulares em 3D para leitura da Recorrência da LSTM
trainX_3d = trainX_norm.reshape((trainX_norm.shape[0], 1, trainX_norm.shape[1]))
valX_3d   = valX_norm.reshape((valX_norm.shape[0], 1, valX_norm.shape[1]))
testX_3d  = testX_norm.reshape((testX_norm.shape[0], 1, testX_norm.shape[1]))

def build_lstm(input_dim):
    model = Sequential([
        Input(shape=(1, input_dim)),
        LSTM(220, return_sequences=False, activation='tanh'),
        BatchNormalization(),
        Dense(200, activation='relu'), BatchNormalization(),
        Dense(150, activation='relu'), BatchNormalization(),
        Dense(100, activation='relu'), BatchNormalization(),
        Dense(80, activation='relu'),  BatchNormalization(),
        Dense(30, activation='relu'),  BatchNormalization(),
        Dense(10, activation='relu'),  BatchNormalization(),
        Dense(5, activation='relu'),   BatchNormalization(),
        Dense(2, activation='linear')
    ])
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
    return model

print("\n>> Treinando LSTM...")
lstm = build_lstm(trainX_norm.shape[1])
es_lstm = EarlyStopping(monitor='val_loss', patience=300, restore_best_weights=True, verbose=0)

lstm.fit(
    trainX_3d, trainY_norm,
    validation_data=(valX_3d, valY_norm),
    epochs=10000, batch_size=64, verbose=0,
    callbacks=[es_lstm]
)

pred_lstm_norm = lstm.predict(testX_3d, verbose=0)
pred_lstm_real = scaler_y.inverse_transform(pred_lstm_norm)

preds_all_models['LSTM'] = pred_lstm_real
evaluate_model('LSTM', pred_lstm_real, testY_real)
lstm.save('modelo_LSTM.keras')

# %% [markdown]
# ## 6. Avaliação e Comparação de Resultados
# Tabela Consolidada & Plotagem.

# %%
# Tabela Comparativa via Pandas
df_metrics = pd.DataFrame(metrics_records).round(3)
print("\n" + "="*80)
print("=== TABELA CONSOLIDADA DE RESULTADOS DOS MODELOS ===")
print("="*80)
print(df_metrics.to_string(index=False))

df_metrics.to_csv("metricas_modelos.csv", index=False)
print("\n(Resultados armazenados no arquivo local 'metricas_modelos.csv')")

# Plots Gráficos da Previsão de TODOS os modelos na massa de teste
models_plotted = preds_all_models.keys()

# Limitação gráfica para não congestionar a plotagem com milhares de linhas. Vamos mostrar os últimos 300 dados previstos de Teste
subset_points = min(300, len(testY_real)) 
time_axis = range(subset_points)

fig, axes = plt.subplots(2, 1, figsize=(16, 12))

# Subplot 1: Intensidade
axes[0].plot(time_axis, testY_real[-subset_points:, 0], label='Dados Reais', color='black', linewidth=2.5, zorder=10)
for m_name in models_plotted:
    axes[0].plot(time_axis, preds_all_models[m_name][-subset_points:, 0], label=f'{m_name}', alpha=0.7, linestyle='--')
axes[0].set_title('Modelo de Intensidade: Previsão x Real (Conjunto Teste)', fontsize=14)
axes[0].legend(loc='lower left')
axes[0].grid(True)

# Subplot 2: Direção
axes[1].plot(time_axis, testY_real[-subset_points:, 1], label='Dados Reais', color='black', linewidth=2.5, zorder=10)
for m_name in models_plotted:
    axes[1].plot(time_axis, preds_all_models[m_name][-subset_points:, 1], label=f'{m_name}', alpha=0.7, linestyle='--')
axes[1].set_title('Modelo de Direção: Previsão x Real (Conjunto Teste)', fontsize=14)
axes[1].legend(loc='lower left')
axes[1].grid(True)

plt.tight_layout()
plt.show()

