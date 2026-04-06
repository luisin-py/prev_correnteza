from pandas_gbq import to_gbq
from datetime import datetime

# nome da tabela com data de hoje
today = datetime.now().strftime('%Y%m%d')
table_id = f"ML.df_data_{today}"

# upload para BigQuery
to_gbq(
    df,
    destination_table=table_id,
    project_id=project_id,
    if_exists='replace',  # ou 'append'
    progress_bar=True
)
pd.DataFrame(df).to_csv('dados_treino.csv', index=False)
 !pip install keras==2.4.1
!pip install tensorflow==1.13.1
!pip list
!pip install tensorflow-gpu # =='1.13.1'
import tensorflow as tf
print(tf.__version__)
import pandas as pd
import numpy as np
import tensorflow as tf

print("GPUs disponÝveis:", tf.config.list_physical_devices('GPU'))
tf.debugging.set_log_device_placement(True)

# operaþÒo simples
a = tf.constant([[1.0, 2.0]])
b = tf.constant([[3.0], [4.0]])
c = tf.matmul(a, b)
print(c)
from google.colab import auth
auth.authenticate_user()
print('Authenticated')

# ler df ja pronto:

from google.colab import syntax
from pandas_gbq import read_gbq

project_id = 'local-bliss-359814'
query = syntax.sql('''
SELECT
  *
FROM
  `ML.df_data_20260328`
''')

# Use pandas_gbq em vez de pd.io.gbq
df = read_gbq(query, project_id=project_id, dialect='standard', progress_bar_type='tqdm')

df.head()
df
from google.colab import syntax
from pandas_gbq import read_gbq

project_id = 'local-bliss-359814'
query = syntax.sql('''
SELECT
  *
FROM
  `ML.xtrain_horario_t_2026`
''')

# Use pandas_gbq em vez de pd.io.gbq
df = read_gbq(query, project_id=project_id, dialect='standard', progress_bar_type='tqdm')

df.head()

from google.colab import drive
drive.mount('/content/drive')
num_colunas = df.shape[1]
print(num_colunas)
for col in df.columns:
    print(col)
    print(df[col].head())
df_safe = df.copy()
#usar s¾ se ta baixanro open meteo e n o pronto
import pandas as pd

# garantir datetime
df['datahora'] = pd.to_datetime(df['datahora'])
df_final['time'] = pd.to_datetime(df_final['time'])

# criar coluna de data (sem hora)
df['date'] = df['datahora'].dt.date
df_final['date'] = df_final['time'].dt.date

df_pivot = df_final.pivot(
    index='date',
    columns='cidade',
    values='precipitation_sum'
).reset_index()

df_merged = df.merge(df_pivot, on='date', how='left')

df_merged = df_merged.drop(columns='date')
df = df_merged
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# =========================
# PREPROCESSAMENTO EM df
# =========================

df = df.copy()

# drop coluna
df = df.drop(columns=['temp_out'], errors='ignore')

df = df.drop(columns=['wind_dir'], errors='ignore')
# =========================
# converter tudo para numÚrico
# =========================

df = df.apply(pd.to_numeric, errors='coerce')

# opcional: limpar NaN gerado pelo map
df = df.fillna(0)

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# =========================
# PREPROCESSAMENTO EM df
# =========================

df = df.copy()

# drop coluna
df = df.drop(columns=['temp_out'], errors='ignore')


# =========================
# converter tudo para numÚrico
# =========================

df = df.apply(pd.to_numeric, errors='coerce')

# opcional: limpar NaN gerado pelo map
df = df.fillna(0)

# =========================
# datahora -> datetime
# =========================

if 'datahora' in df.columns:
    df['datahora'] = pd.to_datetime(df['datahora'], errors='coerce')

# se datahora for index
if df.index.name == 'datahora':
    df.index = pd.to_datetime(df.index, errors='coerce')

# =========================
# tudo -> numÚrico (exceto datahora)
# =========================

cols_to_convert = df.columns.drop('datahora', errors='ignore')

df[cols_to_convert] = df[cols_to_convert].apply(
    pd.to_numeric, errors='coerce'
)

# =========================
# substituir erros por 0
# =========================

df[cols_to_convert] = df[cols_to_convert].fillna(0)

# opcional: garantir tipo float32 (mais leve)
df[cols_to_convert] = df[cols_to_convert].astype(np.float32)

df = df.sort_values("datahora").reset_index(drop=True)

# =========================
# TARGET (y) com lead +2
# =========================
y = df[["intensidade_3m_kt"]].shift(-2) # , "direcao_3m_deg"

# =========================
# FEATURES (X_o)
# =========================

# colunas
target_cols = ["intensidade_3m_kt"] # , "direcao_3m_deg"

# varißveis de correnteza (nÒo usar no futuro)
corrente_cols = [c for c in df.columns if "intensidade" in c or "direcao" in c]

# todas exceto datahora
base_cols = [c for c in df.columns if c != "datahora"]

X_parts = []

# ---------
# LAGS (t, t-1, t-2)
# tudo incluso
# ---------
for lag in range(0, 6):
    X_lag = df[base_cols].shift(lag)
    X_lag.columns = [f"{c}_lag{lag}" for c in base_cols]
    X_parts.append(X_lag)

# ---------
# LEADS (t+1 ... t+6)
# exclui correnteza
# ---------
lead_cols = [c for c in base_cols if c not in corrente_cols]

for lead in range(1, 7):
    X_lead = df[lead_cols].shift(-lead)
    X_lead.columns = [f"{c}_lead{lead}" for c in lead_cols]
    X_parts.append(X_lead)

# concatena tudo
X_o = pd.concat(X_parts, axis=1)

# =========================
# LIMPEZA FINAL
# =========================
data = pd.concat([X_o, y], axis=1).dropna()


# remove linhas onde intensidade == 0
data = data[data["intensidade_3m_kt"] != 0]

X_o = data[X_o.columns]
y = data[y.columns]
print(df.dtypes)
print(df.isna().sum().sum())  # deve ser 0 (exceto datetime invßlido)
X_o.shape

y.shape
df = df.set_index("datahora")

cols_to_drop = [c for c in X_o.columns if "wind_dir" in c]
X_o = X_o.drop(columns=cols_to_drop)

base_vars = sorted(set(c.split("_lag")[0].split("_lead")[0] for c in X_o.columns))

n_cols = 3
n_rows = int(np.ceil(len(base_vars) / n_cols))

fig, axes = plt.subplots(n_rows, n_cols, figsize=(15, 4 * n_rows))
axes = axes.flatten()

for i, var in enumerate(base_vars):
    related_cols = [c for c in X_o.columns if c.startswith(var)]

    for col in related_cols:
        axes[i].plot(X_o.index, X_o[col], alpha=0.6)

    axes[i].set_title(var)
    axes[i].grid(True)

# remover vazios
for j in range(i + 1, len(axes)):
    fig.delaxes(axes[j])

plt.tight_layout()
plt.show()

df.fillna(0, inplace=True)
percent_zeros = ((X_o == 0).mean() * 100).sort_values(ascending=False)

print(percent_zeros)

# =========================
# SPLIT (80/20 temporal)
# =========================
def normalize_minmax(X):
    X = np.array(X, dtype=np.float32)
    min_vals = X.min(axis=0)
    max_vals = X.max(axis=0)

    denom = max_vals - min_vals
    denom[denom == 0] = 1.0

    X_norm = (X - min_vals) / denom
    return X_norm, min_vals, max_vals




split = int(len(X_o) * 0.8)
trainX, testX = X_o.iloc[:split], X_o.iloc[split:]
trainY, testY = y.iloc[:split], y.iloc[split:]

print(trainX.shape)
print(testX.shape)

# =========================
# VISUALIZAÃ├O
# =========================

plttam = 300
plt.ylim([0, 5])
plt.plot(range(1000,1000+plttam), trainY.iloc[1000:1000+plttam])
plt.suptitle('Number: ' + str(plttam))
plt.show()

# =========================
# CONFIG
# =========================

USE_NORMALIZATION = True  # <<< MUDE AQUI

# =========================
# SPLIT (80/20 temporal)
# =========================

split = int(len(X_o) * 0.92)
trainX, testX = X_o.iloc[:split], X_o.iloc[split:]
trainY, testY = y.iloc[:split], y.iloc[split:]

# =========================
# LIMPEZA NUM╔RICA FINAL
# =========================

trainX = np.nan_to_num(trainX.to_numpy(), nan=0.0, posinf=0.0, neginf=0.0)
testX  = np.nan_to_num(testX.to_numpy(),  nan=0.0, posinf=0.0, neginf=0.0)

trainY = np.nan_to_num(trainY.to_numpy(), nan=0.0, posinf=0.0, neginf=0.0)
testY  = np.nan_to_num(testY.to_numpy(),  nan=0.0, posinf=0.0, neginf=0.0)

# =========================
# NORMALIZAÃ├O (CONDICIONAL)
# =========================

if USE_NORMALIZATION:
    # X
    trainX, min_vals, max_vals = normalize_minmax(trainX)
    testX = (testX - min_vals) / (max_vals - min_vals + 1e-8)

    # Y
    trainY, minY, maxY = normalize_minmax(trainY)
    testY = (testY - minY) / (maxY - minY + 1e-8)

else:
    # mantÚm dados originais
    min_vals, max_vals = None, None
    minY, maxY = None, None
#Build the Graph
#Clear any existing model in memory
tam = trainX.shape[1]
print('model imputs tam:', tam )
tf.keras.backend.clear_session()
#Initialize Sequential model
model = tf.keras.models.Sequential()
#imputlayer
model.add(tf.keras.layers.Dense(tam, input_shape=(tam,)))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(220, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(200, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(150, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(100, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(80, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(30, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(10, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(5, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
model.add(tf.keras.layers.Dense(2, activation='relu'))
model.add(tf.keras.layers.BatchNormalization())
# output
model.add(tf.keras.layers.Dense(2, activation='linear'))

#Compile the model
sgd_optimizer = tf.keras.optimizers.SGD(learning_rate=0.005, momentum=0.90)

model.compile(optimizer=sgd_optimizer,
              loss='mean_absolute_error',
              metrics=['mean_squared_error']) #  accuracy hinge mean_squared_error
#Review model
model.summary()
#Train the model
mckpt = tf.keras.callbacks.ModelCheckpoint('mnist_sgd_mom.keras',
                                           monitor='val_mean_squared_error', save_best_only=True, verbose=1)
model.fit(trainX,trainY,
          validation_data=(testX,testY),
          epochs=4,
          batch_size=32,
          callbacks = [mckpt])

model.compile(optimizer='sgd', loss='mean_squared_error', metrics=['mean_squared_error'])
model.save('mnist_sgd.keras')

#Save the model in current directory
model.save('mnist_dnn_v1.keras')

print("Colunas usadas em X:")
print(X.columns.tolist())

def treinar_com_acuracia(
    model,
    trainX, trainY,
    testX, testY,
    minY, maxY,
    epochs=50,
    threshold=0.5,
    idx_intensidade=0
):
    import numpy as np
    import matplotlib.pyplot as plt

    acc_hist = []

    for ep in range(epochs):
        print(f"\nEpoch {ep+1}/{epochs}")

        # treina 1 Úpoca
        model.fit(
            trainX, trainY,
            validation_data=(testX, testY),
            epochs=1,
            batch_size=32,
            verbose=0
        )

        # previsÒo
        pred = model.predict(testX, verbose=0)

        # =========================
        # DESNORMALIZA
        # =========================
        y_true = testY * (maxY - minY + 1e-8) + minY
        y_pred = pred * (maxY - minY + 1e-8) + minY

        # intensidade
        yt = y_true[:, idx_intensidade]
        yp = y_pred[:, idx_intensidade]

        # erro
        erro = np.abs(yt - yp)

        # acurßcia
        acc = np.mean(erro < threshold) * 100
        acc_hist.append(acc)

        print(f"Acurßcia (<{threshold}): {acc:.3f}%")

    # =========================
    # PLOT
    # =========================
    plt.figure()
    plt.plot(acc_hist)
    plt.title("EvoluþÒo da Acurßcia (%)")
    plt.xlabel("╔poca")
    plt.ylabel("Acurßcia (%)")
    plt.grid(True)
    plt.show()

    return acc_hist
acc_hist = treinar_com_acuracia(
    model,
    trainX, trainY,
    testX, testY,
    minY, maxY,
    epochs=15,
    threshold=0.5
)

def plot_amostras_aleatorias(
    model,
    testX, testY,
    minY, maxY,
    n_samples=5,
    window=100,
    idx_intensidade=0
):
    import numpy as np
    import matplotlib.pyplot as plt

    # previsÒo
    pred = model.predict(testX, verbose=0)

    # =========================
    # DESNORMALIZA
    # =========================
    y_true = testY * (maxY - minY + 1e-8) + minY
    y_pred = pred * (maxY - minY + 1e-8) + minY

    # garante espaþo suficiente pra janela
    max_start = len(y_true) - window
    idxs = np.random.choice(max_start, size=n_samples, replace=False)

    fig, axes = plt.subplots(n_samples, 1, figsize=(12, 3 * n_samples))

    if n_samples == 1:
        axes = [axes]

    for i, idx in enumerate(idxs):
        real = y_true[idx:idx+window, idx_intensidade]
        predv = y_pred[idx:idx+window, idx_intensidade]

        axes[i].plot(real, label="Real")
        axes[i].plot(predv, label="Previsto")

        axes[i].set_title(f"Janela iniciando em {idx}")
        axes[i].grid(True)
        axes[i].legend()

    plt.tight_layout()
    plt.show()
plot_amostras_aleatorias(
    model,
    testX, testY,
    minY, maxY,
    n_samples=5,
    window=100
)

# =========================
# PREVIS├O COMPLETA
# =========================

pred_full = model.predict(testX)

# denormaliza
pred_full = pred_full * (maxY - minY + 1e-8) + minY
real_full = testY * (maxY - minY + 1e-8) + minY

# =========================
# PLOT 300 PONTOS
# =========================

import matplotlib.pyplot as plt

n = 300

plt.figure()
plt.plot(real_full[:n].flatten(), label='Real')
plt.plot(pred_full[:n].flatten(), label='Pred')
plt.legend()

plt.title('Real vs Pred (300 primeiros pontos)')
plt.xlabel('Tempo')
plt.ylabel('Valor')

plt.show()
def acuracia_intensidade(
    y_true_norm,
    y_pred_norm,
    minY,
    maxY,
    idx_intensidade=0,   # Ýndice da coluna de intensidade
    threshold=0.5
):
    # =========================
    # DESNORMALIZA
    # =========================
    y_true = y_true_norm * (maxY - minY + 1e-8) + minY
    y_pred = y_pred_norm * (maxY - minY + 1e-8) + minY

    # pega s¾ intensidade
    y_true_i = y_true[:, idx_intensidade]
    y_pred_i = y_pred[:, idx_intensidade]

    # =========================
    # ERRO ABSOLUTO
    # =========================
    erro = np.abs(y_true_i - y_pred_i)

    # =========================
    # ACUR┴CIA
    # =========================
    acertos = np.sum(erro < threshold)
    acc = acertos / len(erro) * 100

    return round(acc, 3)
pred_test = model.predict(testX)

acc = acuracia_intensidade(
    testY,
    pred_test,
    minY,
    maxY,
    idx_intensidade=0,  # ajuste se necessßrio
    threshold=0.5
)

print(f"Acurßcia (erro < 0.5): {acc}%")
pred_test = model.predict(trainX)

acc = acuracia_intensidade(
    trainY,
    pred_test,
    minY,
    maxY,
    idx_intensidade=0,  # ajuste se necessßrio
    threshold=0.5
)

print(f"Acurßcia (erro < 0.5): {acc}%")
def acuracia_multistep(y_true_norm, y_pred_norm, minY, maxY, threshold=0.5):
    y_true = y_true_norm * (maxY - minY + 1e-8) + minY
    y_pred = y_pred_norm * (maxY - minY + 1e-8) + minY

    accs = []
    for i in range(y_true.shape[1]):
        erro = np.abs(y_true[:, i] - y_pred[:, i])
        acc = np.mean(erro < threshold) * 100
        accs.append(round(acc, 3))

    return accs
accs = acuracia_multistep(testY, pred_test, minY, maxY, threshold=0.5)

for i, acc in enumerate(accs):
    print(f"Step {i}: {acc}%")
from google.colab import syntax
from pandas_gbq import read_gbq

project_id = 'local-bliss-359814'

query = syntax.sql('''
SELECT
  *
FROM
  `ML.xtrain_horario_t_2026`
ORDER BY
  datahora DESC
LIMIT 10
''')

df = read_gbq(query, project_id=project_id, dialect='standard', progress_bar_type='tqdm')

# ordenar crescente para modelagem temporal
df = df.sort_values('datahora').reset_index(drop=True)

df.head()
import numpy as np
import pandas as pd

# supondo colunas:
# 'intensidade', 'direcao', 'datahora'

target_cols = ['intensidade', 'direcao']

# n·mero de lags (ajuste conforme necessßrio)
n_lags = 3

def create_lagged(df, cols, n_lags):
    data = df.copy()
    for col in cols:
        for lag in range(1, n_lags+1):
            data[f'{col}_lag{lag}'] = data[col].shift(lag)
    return data.dropna()

df_lagged = create_lagged(df, target_cols, n_lags)

X = df_lagged.drop(columns=target_cols + ['datahora'])
y = df_lagged[target_cols]
# =========================================
# CONFIG
# =========================================


target_cols = ['intensidade', 'direcao']
n_lags = 3
horizon = 6
USE_NORMALIZATION = True

# =========================================
# 1) LAGS
# =========================================
def create_lagged(df, cols, n_lags):
    data = df.copy()
    for col in cols:
        for lag in range(1, n_lags+1):
            data[f'{col}_lag{lag}'] = data[col].shift(lag)
    return data

# =========================================
# 2) MULTISTEP Y
# =========================================
def create_multistep_y(df, cols, horizon):
    data = pd.DataFrame(index=df.index)
    for col in cols:
        for h in range(1, horizon+1):
            data[f'{col}_t+{h}'] = df[col].shift(-h)
    return data

# =========================================
# 3) PIPELINE COMPLETO
# =========================================
df = df.sort_values('datahora').reset_index(drop=True)

df_lagged = create_lagged(df, target_cols, n_lags)
y_multi   = create_multistep_y(df_lagged, target_cols, horizon)

# juntar tudo e remover NaN
data_final = pd.concat([df_lagged, y_multi], axis=1).dropna()

# features e target
X_o = data_final.drop(columns=target_cols + ['datahora'] + list(y_multi.columns))
y   = data_final[y_multi.columns]

# =========================================
# 4) NUMPY + LIMPEZA
# =========================================
X_o = np.nan_to_num(X_o.to_numpy(), nan=0.0, posinf=0.0, neginf=0.0)
y   = np.nan_to_num(y.to_numpy(),   nan=0.0, posinf=0.0, neginf=0.0)

# =========================================
# 5) PEGAR ┌LTIMA JANELA
# =========================================
testX = X_o[-1:].copy()
testY = y[-1:].copy()

# =========================================
# 6) NORMALIZAÃ├O (USA PARAMS DO TREINO)
# =========================================
if USE_NORMALIZATION:
    testX = (testX - min_vals) / (max_vals - min_vals + 1e-8)
    testY = (testY - minY) / (maxY - minY + 1e-8)

# =========================================
# 7) PREVIS├O
# =========================================
pred_test = model.predict(testX)

# desnormalizar
pred_real = pred_test * (maxY - minY + 1e-8) + minY

# =========================================
# 8) SEPARAR SA═DAS
# =========================================
int_pred = pred_real[0][:horizon]
dir_pred = pred_real[0][horizon:]

# =========================================
# 9) TEMPO FUTURO
# =========================================
last_time = df['datahora'].iloc[-1]
future_times = [last_time + pd.Timedelta(hours=i+1) for i in range(horizon)]

# =========================================
# 10) PLOT
# =========================================
plt.figure(figsize=(12,8))

# Intensidade
plt.subplot(2,1,1)
plt.plot(df['datahora'], df['intensidade'], label='Real')
plt.plot(future_times, int_pred, '--', label='Previsto')
plt.title('Intensidade')
plt.legend()

# DireþÒo
plt.subplot(2,1,2)
plt.plot(df['datahora'], df['direcao'], label='Real')
plt.plot(future_times, dir_pred, '--', label='Previsto')
plt.title('DireþÒo')
plt.legend()

plt.tight_layout()
plt.show()
# Enable eager execution
tf.compat.v1.enable_eager_execution()
for col in df.columns:
    if 'direcao' in col.lower():
        s = df[col]
        print(f'\nCOLUNA: {col}')
        print('dtype:', s.dtype)
        print('NaN:', s.isna().sum())
        print('Zeros:', (s == 0).sum())
        print('┌nicos:', s.nunique())
        print('Valores ·nicos (amostra):', s.dropna().unique()[:10])
trainX.shape[1]
import requests
import pandas as pd
from datetime import date, timedelta
import time

cidades = [
    ("PORTO ALEGRE", "RS", -30.0324999, -51.2303767),
    ("CANOAS", "RS", -29.9216045, -51.1799525),
    ("SAO LEOPOLDO", "RS", -29.7544405, -51.1516497),
    ("NOVO HAMBURGO", "RS", -29.6905705, -51.1429035),
    ("GRAVATAI", "RS", -29.9440222, -50.9930938),
    ("SANTA MARIA", "RS", -29.6860512, -53.8069214),
    ("CACHOEIRA DO SUL", "RS", -30.0482234, -52.8901686),
    ("SANTA CRUZ DO SUL", "RS", -29.714209, -52.4285807),
    ("RIO GRANDE", "RS", -32.035, -52.0986)
]

# perÝodo (·ltimos 6 anos)
end_date = date.today()
start_date = end_date - timedelta(days=15) #7*365

dfs = []

for cidade, estado, lat, lon in cidades:
    print(f"Baixando: {cidade}")

    try:
        resp = requests.get(
            "https://archive-api.open-meteo.com/v1/archive",
            params={
                "latitude": lat,
                "longitude": lon,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "daily": "temperature_2m_mean,precipitation_sum",
                "timezone": "America/Sao_Paulo"
            },
            timeout=30
        )
        resp.raise_for_status()

        data = resp.json().get("daily", {})
        df_cidade = pd.DataFrame(data)

        if not df_cidade.empty:
            df_cidade["cidade"] = cidade
            df_cidade["estado"] = estado
            df_cidade["lat"] = lat
            df_cidade["lon"] = lon
            dfs.append(df_cidade)

        time.sleep(1)

    except Exception as e:
        print(f"Erro em {cidade}: {e}")

# concat final
df_final = pd.concat(dfs, ignore_index=True)

print(df_final.head())
print(df_final.shape)
df_final
df
import pandas as pd

# garantir datetime
df['datahora'] = pd.to_datetime(df['datahora'])
df_final['time'] = pd.to_datetime(df_final['time'])

# criar coluna de data (sem hora)
df['date'] = df['datahora'].dt.date
df_final['date'] = df_final['time'].dt.date

df_pivot = df_final.pivot(
    index='date',
    columns='cidade',
    values='precipitation_sum'
).reset_index()

df_merged = df.merge(df_pivot, on='date', how='left')

df_merged = df_merged.drop(columns='date')
df_merged.head
df_merged.to_csv("df_merged.csv", index=False)
# for trainning and monitoring evolution
run = []
best_acuracia = 0

for j in range(10):
  #Load the model in memory
  try:
    model = tf.keras.models.load_model('mnist_sgd.keras')
  except LoadError:
    print("mantido modelo padroa, best model nÒo calculado ainda")
    model = tf.keras.models.load_model('mnist_sgd.keras')
  #mckpt = tf.keras.callbacks.ModelCheckpoint('mnist_sgd_mom.h5', monitor='val_mean_squared_error', save_best_only=True, verbose=1)
  sgd_optimizer = tf.keras.optimizers.SGD(learning_rate=0.0005+np.random.random()/1000, momentum=0.9+np.random.random()/1000)
  #treinar a partir do ultimo treino
  model.fit(trainX,trainY,
            validation_data=(testX,testY),
            epochs=20,
            batch_size=32,
            callbacks = [mckpt])

  model.compile(optimizer='sgd', loss='mean_absolute_error', metrics=['mean_squared_error'])
  predtest = model.predict(testX)
  aux = acuracia(testY[:,2]*5,predtest[:,2]*5,trashhold)
  for i in range(0,12):
    print(acuracia(testY[:,i]*5,predtest[:,i]*5,trashhold))
  run.append(aux)
  if len(run)>0:
    if aux > best_acuracia:
      model.save('mnist_sgd_best.keras')
      best_acuracia = aux
  model.save('mnist_sgd.keras')


  print('ACERTIVIDADE TESTING',aux, '%')
  #plotar evolutivo acertividade test
  plt.plot(range(len(run)),run)
  plt.suptitle('Avolutivo assertividade')
  plt.show()
 predtest = model.predict(testX)
#predtest = model.predict(testX)
for i in range(0,12):
    print(acuracia(testY[:,i]*5,predtest[:,i]*5,trashhold))
 def acuracia(y,py,trashhold=0.3):
  ac=0
  for i in range(len(y)):
    if abs(y[i]-py[i]) <trashhold: ac+=1
  return round(ac*100/len(y),3)
#pred = model.predict(trainX)
trashhold=0.3
 predtest = model.predict(trainX)
 for i in range(0,12):
    print(acuracia(trainY[:,i]*5,predtest[:,i]*5,trashhold))


predtest = model.predict(testX)
print('ACERTIVIDADE TESTING',acuracia(testY*5,predtest*5,trashhold), '%')
print('ACERTIVIDADE TRAINING',acuracia(trainY*5,pred*5), '%')

#get_denormalized(model.predict(testX),norm)
#89.927


model.save("/tmp/test_model.keras")
model.export("/tmp/test_model")
!gsutil -m cp -r /tmp/test_model gs://modelos_praticagem/model_20260328_2/
#Load the model in memory
!gsutil -m cp -r gs://modelos_praticagem/model_20260328/test_model /tmp/
model = tf.keras.layers.TFSMLayer(
    "/tmp/test_model",
    call_endpoint="serving_default"
)
