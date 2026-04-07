#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from google.cloud import bigquery
import pandas as pd
import numpy as np

# ======== CONFIG ========
BQ_PROJECT = "local-bliss-359814"
BQ_DATASET = "wherehouse_tratado"
BQ_TABLE_SRC = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_5min"
# ========================

# ======== INTERAÇÃO DE TERMINAL ========
print("Selecione o tipo de interpolação:")
print("  1. Linear")
print("  2. Quadrática")
print("  3. Polinomial (grau 3)")
print("  4. Spline (cúbica)")
escolha = input("Digite o número da interpolação desejada (1-4): ").strip()

if escolha == "1":
    metodo = "linear"
    sufixo = "linear"
    kwargs = {}
elif escolha == "2":
    metodo = "quadratic"
    sufixo = "quadratica"
    kwargs = {}
elif escolha == "3":
    metodo = "polynomial"
    sufixo = "polynomial"
    kwargs = {"order": 3}
elif escolha == "4":
    metodo = "spline"
    sufixo = "spline"
    kwargs = {"order": 3}
else:
    print("❌  Opção inválida. Saindo...")
    exit(1)

BQ_TABLE_DEST = f"{BQ_PROJECT}.{BQ_DATASET}.mestre_5min_{sufixo}"

print(f"\nMétodo escolhido: {metodo} {kwargs if kwargs else ''}")
print(f"Tabela destino: {BQ_TABLE_DEST}")

# ======== BIGQUERY E CAMPOS ========
client = bigquery.Client()
query = f"SELECT * FROM `{BQ_TABLE_SRC}` ORDER BY timestamp_br"
df = client.query(query).to_dataframe()
print(f"Lidos {len(df)} registros de mestre_5min.")

campos_quadraticos = [
    # INMET
    'inmet_datetime', 'temperatura_inst_inmet', 'temperatura_max_inmet', 'temperatura_min_inmet',
    'umidade_inst_inmet', 'umidade_max_inmet', 'umidade_min_inmet', 'pto_orvalho_inst_inmet',
    'pto_orvalho_max_inmet', 'pto_orvalho_min_inmet', 'pressao_inst_inmet', 'pressao_max_inmet',
    'pressao_min_inmet', 'vento_vel_m_s_inmet', 'vento_dir_deg_inmet', 'vento_raj_m_s_inmet',
    'chuva_inmet', 'radiacao_inmet', 'dt_utc_inmet', 'data_inmet', 'hora_utc_inmet',
    'timestamp_execucao_inmet',
    # OpenWeather NOW
    'openweather_timestamp', 'ow_city', 'ow_city_id', 'ow_coord_lon', 'ow_coord_lat',
    'ow_weather_id', 'ow_weather_main', 'ow_weather_desc', 'ow_weather_icon', 'ow_base',
    'ow_temp', 'ow_feels_like', 'ow_temp_min', 'ow_temp_max', 'ow_pressure', 'ow_humidity',
    'ow_sea_level', 'ow_grnd_level', 'ow_visibility', 'ow_wind_speed', 'ow_wind_deg',
    'ow_wind_gust', 'ow_clouds', 'ow_rain_1h', 'ow_rain_3h', 'ow_snow_1h', 'ow_snow_3h',
    'ow_dt', 'ow_timestamp', 'ow_sys_type', 'ow_sys_id', 'ow_sys_country', 'ow_sys_sunrise',
    'ow_sys_sunset', 'ow_timezone_offset', 'ow_cod', 'ow_timestamp_utc',
    # FORECAST
    'forecast_timestamp_execucao', 'fc_timestamp_utc', 'fc_city_name', 'fc_lat', 'fc_lon',
    'fc_temp', 'fc_feels_like', 'fc_temp_min', 'fc_temp_max', 'fc_pressure', 'fc_sea_level',
    'fc_grnd_level', 'fc_humidity', 'fc_weather_main', 'fc_weather_desc', 'fc_weather_icon',
    'fc_clouds_all', 'fc_wind_speed', 'fc_wind_deg', 'fc_wind_gust', 'fc_visibility', 'fc_pop',
    'fc_rain_3h', 'fc_snow_3h', 'fc_sys_pod', 'fc_timezone', 'fc_cod', 'fc_dt_txt',
    # Air-Pollution
    'air_aqi', 'air_pm2_5', 'air_pm10', 'air_co', 'air_no', 'air_no2', 'air_o3', 'air_so2', 'air_nh3',
]

# ======== INTERPOLAÇÃO ========
for campo in campos_quadraticos:
    if campo not in df.columns:
        continue
    # tenta converter para float
    try:
        df[campo] = pd.to_numeric(df[campo], errors='coerce')
    except Exception:
        pass
    # Interpola se houver dados
    if df[campo].notnull().sum() > 2:
        try:
            df[campo] = df[campo].interpolate(method=metodo, limit_direction='both', **kwargs)
        except Exception as e:
            print(f"⚠️  Falha em {campo} com {metodo}: {e}")
            df[campo] = df[campo].fillna(method='ffill').fillna(method='bfill')
    else:
        df[campo] = df[campo].fillna(method='ffill').fillna(method='bfill')
    # Limita para 4 casas decimais se for float/numeric
    if pd.api.types.is_float_dtype(df[campo]):
        df[campo] = df[campo].round(4)

print("Campos interpolados com sucesso.")
# Limita para 4 casas decimais apenas se for do tipo float

# ======== SALVA NO BIGQUERY ========
job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
    create_disposition="CREATE_IF_NEEDED"
)
job = client.load_table_from_dataframe(df, BQ_TABLE_DEST, job_config=job_config)
job.result()

print(f"\n✅ Tabela {BQ_TABLE_DEST} criada/preenchida com sucesso!")
