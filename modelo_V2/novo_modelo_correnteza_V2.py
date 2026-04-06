# %% [markdown]
# # Previsão de Correnteza V2 (Todas as Profundidades) a partir do BigQuery
# Evolução arquitetônica: Múltiplas profundidades, MAs longas para Chuva, 
# e rede LSTM utilizando Sliding Windows (Timesteps 3D).

# %%
import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
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
from tensorflow.keras.layers import Dense, BatchNormalization, LSTM, Input, Dropout
from tensorflow.keras.optimizers import Adam
from tensorflow.keras.callbacks import EarlyStopping

import warnings
warnings.filterwarnings('ignore')

# Fixando sementes para reprodutibilidade
random.seed(42)
np.random.seed(42)
tf.random.set_seed(42)

# ==========================================
# PASTA DE SAÍDA V2
# ==========================================
OUTPUT_DIR = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\modelo_V2"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# %% [markdown]
# ## 1. Autenticação e Extração de Dados
# Consulta a nova tabela `ML.xtrain_horario_t_2026`.

# %%
CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = 'local-bliss-359814'
QUERY_TABLE = 'ML.xtrain_horario_t_2026' 

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

# %% [markdown]
# ## 2. Tratamento de Dados e Engenharia de Features
# Novidades: MAs de chuva (12, 24, 48h) e 6 Targets simultaneamente.

# %%
if 'df' in locals():
    df_modelo = df.copy()
    
    # 2.1 Limpeza Inicial
    cols_to_drop = ['temp_out'] + [c for c in df_modelo.columns if 'wind_dir' in c]
    df_modelo.drop(columns=[c for c in cols_to_drop if c in df_modelo.columns], inplace=True)
    
    if 'datahora' in df_modelo.columns:
        df_modelo['datahora'] = pd.to_datetime(df_modelo['datahora'], errors='coerce')
        
    df_modelo = df_modelo.sort_values("datahora").reset_index(drop=True)
    
    # Numérico e preencher NA
    cols_to_convert = df_modelo.columns.drop('datahora', errors='ignore')
    df_modelo[cols_to_convert] = df_modelo[cols_to_convert].apply(pd.to_numeric, errors='coerce')
    df_modelo[cols_to_convert] = df_modelo[cols_to_convert].fillna(0).astype('float32')
    
    # 2.2 V2 Targets (Todas Profundidades)
    desired_targets = [
        'intensidade_6m_kt', 'intensidade_superficie_kt', 'intensidade_3m_kt',
        'direcao_6m_deg', 'direcao_superficie_deg', 'direcao_3m_deg'
    ]
    y_cols = [c for c in desired_targets if c in df_modelo.columns]
    print(f"Targets Disponíveis ({len(y_cols)}):", y_cols)
    
    # Remover registros onde TODAS as intensidades alvo estão zeradas (filtros de ruído/erro)
    # ou podemos ser mais rígidos dependendo do critério de corte original.
    if len(y_cols) > 0:
        filter_mask = df_modelo[[c for c in y_cols if 'intensidade' in c]].sum(axis=1) != 0
        df_modelo = df_modelo[filter_mask].reset_index(drop=True)
    
    # 2.3 Feature Engineering V2
    base_cols = [c for c in df_modelo.columns if c != 'datahora']
    
    # Identificar colunas de Chuva/Precipitação dinamicamente (para MAs mais longas)
    rain_cols = [c for c in base_cols if 'chuva' in c.lower() or 'rain' in c.lower() or 'precip' in c.lower()]
    # Se não houver nome evidente, 'previsao' poderia ser o termo
    if not rain_cols and 'previsao' in base_cols:
        rain_cols = ['previsao']
        
    X_parts = []
    
    # A. Lags (1 a 5) para todas as Base Features
    for lag in range(1, 6):
        X_lag = df_modelo[base_cols].shift(lag)
        X_lag.columns = [f"{c}_lag{lag}" for c in base_cols]
        X_parts.append(X_lag)
    
    # B. MAs padrão (3 e 6) para base_cols
    for window in [3, 6]:
        X_ma = df_modelo[base_cols].rolling(window=window).mean().shift(1)
        X_ma.columns = [f"{c}_ma{window}" for c in base_cols]
        X_parts.append(X_ma)

    # C. MAs mais longas (12, 24, 48) SOMENTE para variáveis de Chuva (V2 Request)
    for window in [12, 24, 48]:
        if rain_cols:
            X_rain_ma = df_modelo[rain_cols].rolling(window=window).mean().shift(1)
            X_rain_ma.columns = [f"{c}_ma{window}" for c in rain_cols]
            X_parts.append(X_rain_ma)
    
    # Consolidar Dataset Tabular Resultante
    X_o = pd.concat(X_parts, axis=1)
    y = df_modelo[y_cols]
    
    # Concatenação e Limpeza dos NaNs criados pelos Shifts 
    data_final = pd.concat([X_o, y], axis=1).dropna()
    valid_indices = data_final.index  # Os index reais em df_modelo que sobraram (fundamentais pra LSTM)
    
    X_clean = data_final[X_o.columns]
    y_clean = data_final[y_cols]

# Salvar Arquivo de Treinamento
data_final.to_csv(os.path.join(OUTPUT_DIR, 'base_treinamento.csv'), index=False)
print("Base de treinamento salva.")

# %% [markdown]
# ### Split Data com Validação 
# Utilizamos divisão temporal 80% (Treino), 10% (Validação) e 10% (Teste).

# %%
if 'data_final' in locals():
    total_len = len(X_clean)
    split_val = int(total_len * 0.8)
    split_test = int(total_len * 0.9)
    
    # Separação (Slices para dataframes 2D - Árvores e MLP)
    trainX = X_clean.iloc[:split_val]
    valX   = X_clean.iloc[split_val:split_test]
    testX  = X_clean.iloc[split_test:]
    
    trainY = y_clean.iloc[:split_val]
    valY   = y_clean.iloc[split_val:split_test]
    testY  = y_clean.iloc[split_test:]
    
    # Extrair datas do teste para o plot visual no final
    test_dates = df_modelo.loc[valid_indices[split_test:], 'datahora'].values
    
    # Normalização MinMax usando propriedades somente do Treino
    scaler_X_2d = MinMaxScaler()
    scaler_y = MinMaxScaler()
    
    trainX_norm = scaler_X_2d.fit_transform(trainX)
    valX_norm   = scaler_X_2d.transform(valX)
    testX_norm  = scaler_X_2d.transform(testX)
    
    trainY_norm = scaler_y.fit_transform(trainY)
    valY_norm   = scaler_y.transform(valY)
    testY_norm  = scaler_y.transform(testY)
    
    testY_real = testY.values
    
# %% [markdown]
# ### Métricas Universais de Avaliação Multivariada

# %%
metrics_records = []
preds_all_models = {}

def evaluate_model(name, model_preds, real_v):
    rec = {'Model': name}
    for i, col in enumerate(y_cols):
        # Para cada target (Intensidade ou Direção, 6m, Sup, 3m)
        mae = mean_absolute_error(real_v[:, i], model_preds[:, i])
        rmse = np.sqrt(mean_squared_error(real_v[:, i], model_preds[:, i]))
        r2 = r2_score(real_v[:, i], model_preds[:, i])
        
        # Acc. custom erro absoluto < 0.5
        acc_05 = np.mean(np.abs(real_v[:, i] - model_preds[:, i]) < 0.5) * 100
        
        p = col.replace("intensidade_", "Int_").replace("direcao_", "Dir_")
        rec[f'MAE {p}'] = mae
        rec[f'RMSE {p}'] = rmse
        rec[f'R2 {p}'] = r2
        rec[f'Erro<0.5 {p}%'] = acc_05
        
    metrics_records.append(rec)

# %% [markdown]
# ## 3. Fase 1: Árvores de Decisão & Boosting (Scikit, XGBoost, LightGBM, Catboost)
# Usam os dados tubulares chatos `[amostras, features(lags+mas)]`.

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
    joblib.dump(model, os.path.join(OUTPUT_DIR, f'modelo_{name.replace(" ", "_")}.joblib'))

# %% [markdown]
# ## 4. Fase 2: Multilayer Perceptron (MLP)
# Mantida a versão original profunda, também consumindo a base 2D Tabular (lags).
# output dimension: 6.

# %%
def build_mlp(input_dim, output_dim):
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
        Dense(output_dim, activation='linear')
    ])
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
    return model

print("\n>> Treinando Multilayer Perceptron (2D Tabular)...")
mlp = build_mlp(trainX_norm.shape[1], len(y_cols))
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
mlp.save(os.path.join(OUTPUT_DIR, 'modelo_MLP.keras'))

# %% [markdown]
# ## 5. Fase 3: Long Short-Term Memory (LSTM) - ARQUITETURA V2 (SLIDING WINDOWS)
# Formato Certo `[amostras, timesteps, features_base_originais]`.
# A arquitetura sofreu poda nas camadas Densas, recebendo inserção de Dropout e adequação pura às recorrências.

# %%
# Preparo especial Exclusivo para LSTM V2
timesteps = 6
df_base = df_modelo[base_cols]

# Treinamos um scaler unicamente para as Features Base para os limites do conjunto de Treinamento
# O idx mais alto presente no Treinamento é:
max_train_idx = valid_indices[split_val - 1]
scaler_base_X = MinMaxScaler()
scaler_base_X.fit(df_base.loc[:max_train_idx])

# Base global normalizada
df_base_norm_array = scaler_base_X.transform(df_base)

# Função Sliding Windows que retroage em "timesteps" os índices válidos da base de saída.
# Isso garante que a LSTM encare o mesmo passado real que a MLP e as Árvores sem distorção indexial.
def create_lstm_sequences(valid_ids, base_array_norm, t_steps):
    X_3d = []
    for idx in valid_ids:
        # Pega a janela rigorosamente antes do momento t correspondentes aos timesteps: [idx-timesteps, idx-1]
        window = base_array_norm[idx - t_steps : idx]
        X_3d.append(window)
    return np.array(X_3d)

trainX_3d = create_lstm_sequences(valid_indices[:split_val], df_base_norm_array, timesteps)
valX_3d   = create_lstm_sequences(valid_indices[split_val:split_test], df_base_norm_array, timesteps)
testX_3d  = create_lstm_sequences(valid_indices[split_test:], df_base_norm_array, timesteps)

print(f"SHAPE LSTM Treino: {trainX_3d.shape} | Val {valX_3d.shape} | Teste {testX_3d.shape}")

# Nova Edificação (Simplificada e com Regularizadores de Dropout)
def build_lstm_v2(t_steps, num_features, output_dim):
    model = Sequential([
        Input(shape=(t_steps, num_features)),
        LSTM(128, return_sequences=False, activation='tanh'),
        Dropout(0.2),
        Dense(64, activation='relu'),
        Dropout(0.2),
        Dense(32, activation='relu'),
        Dense(output_dim, activation='linear')
    ])
    model.compile(optimizer=Adam(learning_rate=0.001), loss='mse', metrics=['mae'])
    return model

print("\n>> Treinando LSTM V2 (Sliding Windows)...")
lstm = build_lstm_v2(timesteps, df_base.shape[1], len(y_cols))
es_lstm = EarlyStopping(monitor='val_loss', patience=300, restore_best_weights=True, verbose=0)

lstm.fit(
    trainX_3d, trainY_norm, # mantemos os mesmos y do alvo anterior
    validation_data=(valX_3d, valY_norm),
    epochs=10000, batch_size=64, verbose=0,
    callbacks=[es_lstm]
)

pred_lstm_norm_3d = lstm.predict(testX_3d, verbose=0)
pred_lstm_real_3d = scaler_y.inverse_transform(pred_lstm_norm_3d)

preds_all_models['LSTM_V2'] = pred_lstm_real_3d
evaluate_model('LSTM_V2', pred_lstm_real_3d, testY_real)
lstm.save(os.path.join(OUTPUT_DIR, 'modelo_LSTM_V2.keras'))

# %% [markdown]
# ## 6. Avaliação e Comparação de Resultados
# Tabela Consolidada & Plotagem Rica (Tempo no Eixo X).

# %%
df_metrics = pd.DataFrame(metrics_records).round(3)

print("\n" + "="*80)
print("=== TABELA CONSOLIDADA DE MÉTRICAS (TODAS PROFUNDIDADES) ===")
print("="*80)
# Mostrando apenas um resumo das accurações pro prompt n ficar poluído:
acc_cols = [c for c in df_metrics.columns if 'Erro<0.5' in c] + ['Model']
print(df_metrics[acc_cols].to_string(index=False))

df_metrics.to_csv(os.path.join(OUTPUT_DIR, "metricas_modelos_v2.csv"), index=False)
print(f"\nResultados arquivados em '{OUTPUT_DIR}'")

# Plots Gráficos Multivariados com Datas (Plotando os últimos N registros de Teste)
subset_points = min(300, len(testY_real))
time_axis_plt = test_dates[-subset_points:]

n_targets = len(y_cols)
fig, axes = plt.subplots(n_targets, 1, figsize=(16, 5 * n_targets), sharex=True)
if n_targets == 1: axes = [axes]

models_plotted = list(preds_all_models.keys())
# Define style and formatting for time
formatter = mdates.DateFormatter('%d/%m \n%H:%M')

for i, col_name in enumerate(y_cols):
    ax = axes[i]
    ax.plot(time_axis_plt, testY_real[-subset_points:, i], label='Real', color='black', linewidth=2.5, zorder=10)
    
    for m_name in models_plotted:
        ax.plot(time_axis_plt, preds_all_models[m_name][-subset_points:, i], label=f'{m_name}', alpha=0.7, linestyle='--')
        
    ax.set_title(f'Predição p/ a Target: {col_name} (Série de Teste)', fontsize=14)
    ax.legend(loc='lower left', ncol=min(4, len(models_plotted)+1))
    ax.grid(True)
    ax.xaxis.set_major_formatter(formatter)

plt.xlabel("Data / Hora")
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "comparativo_previsoes.png"), dpi=200)
plt.show()

