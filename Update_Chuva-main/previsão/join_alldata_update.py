#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys
from decimal import Decimal, getcontext, ROUND_DOWN
import pandas as pd
from google.cloud import bigquery

PROJECT  = "local-bliss-359814"
DATASET  = "wherehouse"
TABLE_ID = f"{PROJECT}.{DATASET}.update_rawdata_5min_backfill"
CSV_FILE = "All-data - update.csv"

# 1. Mapeamento de nomes entre AllData e tabela BQ (sem chaves duplicadas!)
COL_MAP = {
    "timestamp"           : "data_station_davis",
    "umidade"             : "umidade",
    "valid_umidade"       : "valid_umidade",
    "pressao"             : "pressao",
    "valid_pressao"       : "valid_pressao",
    "ventointensidade"    : "ventointensidade",
    "valid_ventointensidade": "valid_ventointensidade",
    "ventodirecao"        : "ventodirecao",
    "ventonum"            : "ventonum",
    "valid_ventonum"      : "valid_ventonum",
    "temperatura"         : "temperatura",
    "valid_temperatura"   : "valid_temperatura",
    "sensacaotermica"     : "sensacaotermica",
    "valid_sensacaotermica": "valid_sensacaotermica",
    "status"              : "status",
    "data_inicio"         : "data_inicio",
    "numero"              : "numero",
    "descricao"           : "descricao",
    "data"                : "data",
    "tipo"                : "tipo",
    "data lua"            : "data_lua",
    "nascer_do_sol"       : "nascer_do_sol",
    "por_do_sol"          : "por_do_sol",
    "matutino"            : "matutino",
    "vespertino"          : "vespertino",
    "tipoMare"            : "tipo_mare_getControls",
    "data_hidromares"     : "data_hidromares",
    "Intensidade 15m"     : "intensidade_15m",
    "Direção 15m"         : "direcao_15m",
    "Intensidade 13,5m"   : "intensidade_13_5m",
    "Direção 13,5m"       : "direcao_13_5m",
    "Intensidade 12m"     : "intensidade_12m",
    "Direção 12m"         : "direcao_12m",
    "Intensidade 10,5m"   : "intensidade_10_5m",
    "Direção 10,5m"       : "direcao_10_5m",
    "Intensidade 9m"      : "intensidade_9m",
    "Direção 9m"          : "direcao_9m",
    "Intensidade 7,5m"    : "intensidade_7_5m",
    "Direção 7,5m"        : "direcao_7_5m",
    "Intensidade Superfície": "intensidade_superficie",
    "Direção Superfície"  : "direcao_superficie",
    "Intensidade 6m"      : "intensidade_6m",
    "Direção 6m"          : "direcao_6m",
    "Intensidade 3m"      : "intensidade_3m",
    "Direção 3m"          : "direcao_3m",
    "Intensidade 1,5m"    : "intensidade_1_5m",
    "valid_intensidade_1_5m": "valid_intensidade_1_5m",
    "Direção 1,5m"        : "direcao_1_5m",
    "dataMare"   : "data_mare_getMare",
    "Altura Prev"         : "altura_prev_getmare",
    "data_mare_real_getMare": "data_mare_real_getMare",
    "Altura medida"       : "altura_real_getmare",
    "valid_altura_real_getmare": "valid_altura_real_getmare",
    "api_mare"            : "api_mare",
    "api_hidromares"      : "api_hidromares",
    "api_estatistica"     : "api_estatistica",
    "motivo"              : "motivo",
}

CAMPOS_BQ = [
    'timestamp','timestamp_end','data_station_davis','umidade','valid_umidade','pressao','valid_pressao','ventointensidade',
    'valid_ventointensidade','ventodirecao','ventonum','valid_ventonum','temperatura','valid_temperatura','sensacaotermica','valid_sensacaotermica','status',
    'data_inicio','numero','descricao','data','tipo','data_lua','nascer_do_sol','por_do_sol','matutino','vespertino','data_mare_getControls','altura_medida_getControls','valid_altura_medida_getControls','tipo_mare_getControls','data_hidromares','intensidade_15m','valid_intensidade_15m','direcao_15m','intensidade_13_5m','valid_intensidade_13_5m','direcao_13_5m','intensidade_12m','valid_intensidade_12m','direcao_12m','intensidade_10_5m','valid_intensidade_10_5m','direcao_10_5m','intensidade_9m','valid_intensidade_9m','direcao_9m','intensidade_7_5m','valid_intensidade_7_5m','direcao_7_5m','intensidade_superficie','valid_intensidade_superficie','direcao_superficie','intensidade_6m','valid_intensidade_6m','direcao_6m','intensidade_3m','valid_intensidade_3m','direcao_3m','intensidade_1_5m','valid_intensidade_1_5m','direcao_1_5m','data_mare_getMare','altura_prev_getmare','valid_altura_prev_getmare','data_mare_real_getMare','altura_real_getmare','valid_altura_real_getmare','api_mare','api_hidromares','api_estatistica','motivo'
]

CAMPOS_NUMERIC = set([
    'umidade','pressao','ventointensidade','ventonum','temperatura','sensacaotermica','numero',
    'intensidade_superficie','altura_prev_getmare','altura_real_getmare','intensidade_15m','direcao_15m','intensidade_13_5m','direcao_13_5m','intensidade_12m','direcao_12m','intensidade_10_5m','direcao_10_5m','intensidade_9m','direcao_9m','intensidade_7_5m','direcao_7_5m','direcao_superficie','intensidade_6m','direcao_6m','intensidade_3m','direcao_3m','intensidade_1_5m','direcao_1_5m','altura_medida_getControls',
])

getcontext().prec = 38
def to_decimal(x):
    try:
        if pd.isna(x) or x is None or str(x).strip() == "":
            return None
        dec = Decimal(str(x).replace(',', '.'))
        return dec.quantize(Decimal('1.000000000'), rounding=ROUND_DOWN)
    except Exception:
        return None

if not os.path.exists(CSV_FILE):
    sys.exit(f"❌ Arquivo {CSV_FILE} não encontrado.")
raw = pd.read_csv(CSV_FILE, dtype=str, encoding="utf-8-sig")
print("📋 Colunas CSV:", raw.columns.tolist())

for src, dst in COL_MAP.items():
    if src in raw.columns:
        raw = raw.rename(columns={src: dst})
df = raw

for c in CAMPOS_BQ:
    if c not in df.columns:
        df[c] = None
df = df[CAMPOS_BQ]

df['timestamp'] = df['data_station_davis']

for col in df.columns:
    if col in ['data_lua','data']:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce").dt.date
    elif col in [
        'data_station_davis','data_inicio','nascer_do_sol','por_do_sol','matutino','vespertino',
        'data_mare_getControls','data_hidromares','data_mare_getMare','data_mare_real_getMare',
        'timestamp','timestamp_end'
    ]:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors="coerce", utc=True)
    elif col in CAMPOS_NUMERIC:
        df[col] = df[col].apply(to_decimal).astype("object")
    elif col.startswith('valid_'):
        df[col] = df[col].map(lambda x: True if str(x).strip().lower()=='true' else False if str(x).strip().lower()=='false' else None)

df = df.dropna(how="all", subset=CAMPOS_BQ)

client = bigquery.Client(project=PROJECT)

result = client.query(
    f"SELECT MIN(timestamp) AS min_ts FROM `{TABLE_ID}`"
).result().to_dataframe()
min_ts = result["min_ts"].iloc[0]
print("Timestamp mais antigo no BQ:", min_ts or "Tabela vazia")

# AJUSTE: garantir que ambos estejam com timezone UTC!
if min_ts is not None:
    ts_compare = pd.to_datetime(min_ts)
    if ts_compare.tzinfo is None:
        ts_compare = ts_compare.tz_localize("UTC")
    else:
        ts_compare = ts_compare.tz_convert("UTC")
    linhas_antes = df[df["timestamp"] < ts_compare]
else:
    linhas_antes = df

if linhas_antes.empty:
    print("ℹ️ Nenhuma linha nova a ser inserida (tudo já está no BQ).")
    sys.exit(0)

print(f"➕ {len(linhas_antes):,} linhas serão inseridas (até {linhas_antes['timestamp'].max()}).")
print("Colunas finais:", linhas_antes.columns.tolist())
print("Tipos finais:\n", linhas_antes.dtypes)

job_cfg = bigquery.LoadJobConfig(
    write_disposition="WRITE_APPEND",
    create_disposition="CREATE_IF_NEEDED"
)
client.load_table_from_dataframe(linhas_antes, TABLE_ID, job_config=job_cfg).result()
print(f"✅ Inseridas {len(linhas_antes):,} linhas em {TABLE_ID} (até {linhas_antes['timestamp'].max()})")
