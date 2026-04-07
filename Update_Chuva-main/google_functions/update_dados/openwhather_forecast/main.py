import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

API_KEY = os.getenv("OPENWEATHER_API_KEY", "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI")
CITIES = ["Rio Grande", "São José do Norte", "Pelotas", "Tapes", "Arambaré", "Camaquã", "São Lourenço do Sul"]
BQ_TABLE_ID = os.getenv("BQ_TABLE_ID", "local-bliss-359814.wherehouse.dados_openweather_forecast")
LOG_LEVEL = int(os.getenv("LOG_LEVEL", 1))

def log(msg, lvl=1):
    if LOG_LEVEL >= lvl:
        print(f"[LOG][{datetime.now()}] {msg}")

SCHEMA_BQ = {
    "timestamp_execucao": "str",
    "timestamp_utc": "str",
    "city_name": "str",
    "lat": "float",
    "lon": "float",
    "temp": "float",
    "feels_like": "float",
    "temp_min": "float",
    "temp_max": "float",
    "pressure": "float",
    "sea_level": "float",
    "grnd_level": "float",
    "humidity": "float",
    "weather_id": "float", 
    "weather_main": "str",
    "weather_description": "str",
    "weather_icon": "str",
    "clouds_all": "float",
    "wind_speed": "float",
    "wind_deg": "float",
    "wind_gust": "float",
    "visibility": "float",
    "pop": "float",
    "rain_3h": "float",
    "snow_3h": "float",
    "sys_pod": "str",
    "timezone": "float",
    "cod": "float",
    "dt_txt": "str"
}

def collect_once():
    rows = []
    timestamp_execucao = datetime.now(timezone(timedelta(hours=-3))).strftime('%Y-%m-%d %H:%M:%S')
    for city in CITIES:
        log(f"🌐  {city}", 1)
        try:
            r = requests.get(
                "https://api.openweathermap.org/data/2.5/forecast",
                params={"q": f"{city},BR", "appid": API_KEY, "units": "metric"},
                timeout=10
            )
            r.raise_for_status()
            j = r.json()
            for entry in j.get("list", []):
                main = entry.get("main", {})
                wind = entry.get("wind", {})
                clouds = entry.get("clouds", {})
                rain = entry.get("rain", {})
                snow = entry.get("snow", {})
                weather = entry.get("weather", [{}])[0]
                rows.append({
                    "timestamp_utc": entry.get("dt_txt"),
                    "timestamp_execucao": timestamp_execucao,
                    "city_name": city,
                    "lat": j.get("city", {}).get("coord", {}).get("lat"),
                    "lon": j.get("city", {}).get("coord", {}).get("lon"),
                    "temp": main.get("temp"),
                    "feels_like": main.get("feels_like"),
                    "temp_min": main.get("temp_min"),
                    "temp_max": main.get("temp_max"),
                    "pressure": main.get("pressure"),
                    "sea_level": main.get("sea_level"),
                    "grnd_level": main.get("grnd_level"),
                    "humidity": main.get("humidity"),
                    "weather_id": weather.get("id"),   
                    "weather_main": weather.get("main"),
                    "weather_description": weather.get("description"),
                    "weather_icon": weather.get("icon"),
                    "clouds_all": clouds.get("all"),
                    "wind_speed": wind.get("speed"),
                    "wind_deg": wind.get("deg"),
                    "wind_gust": wind.get("gust"),
                    "visibility": entry.get("visibility"),
                    "pop": entry.get("pop"),
                    "rain_3h": rain.get("3h"),
                    "snow_3h": snow.get("3h"),
                    "sys_pod": entry.get("sys", {}).get("pod"),
                    "timezone": j.get("city", {}).get("timezone"),
                    "cod": j.get("cod"),
                    "dt_txt": entry.get("dt_txt")
                })
        except Exception as e:
            log(f"❌  {city}: {e}", 0)

    if not rows:
        log("⚠️  Nenhum dado coletado", 0)
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    log(f"➡️  Colunas do DataFrame: {df.columns.tolist()}", 2)
    log(df.head().to_string(), 2)
    return df

def padroniza_df_bq(df):
    # Adiciona colunas faltantes e coloca na ordem certa
    for col, tipo in SCHEMA_BQ.items():
        if col not in df.columns:
            df[col] = "" if tipo == "str" else float('nan')
    df = df[list(SCHEMA_BQ.keys())]
    # Cast dos tipos certos
    for col, tipo in SCHEMA_BQ.items():
        if tipo == "str":
            df[col] = df[col].astype(str).replace({"None": "", "nan": "", "NaT": ""})
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    # Formata datas para não ter microssegundos
    for col in ["timestamp_utc", "timestamp_execucao", "dt_txt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
            df[col] = df[col].fillna("")
    # Limpa listas/dicts
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if isinstance(x, (dict, list, bytes)) else x)
    # FORÇA TODOS NUMERIC PARA FLOAT64!
    for col, tipo in SCHEMA_BQ.items():
        if tipo == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(float)
    # Debug (opcional)
    log("===== TIPOS DAS COLUNAS =====", 1)
    log(df.dtypes, 1)
    log("===== PRIMEIRA LINHA =====", 1)
    log(df.head(1).to_dict(), 1)
    log("===== PRIMEIRAS 3 LINHAS =====", 2)
    log(df.head(3).to_dict(), 2)
    return df


def write_bq(df):
    if df.empty:
        log("⚠️  DataFrame vazio, não será enviado para o BigQuery.", 0)
        return
    try:
        client = bigquery.Client()
        job = client.load_table_from_dataframe(
            df,
            BQ_TABLE_ID,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
        )
        job.result()
        log(f"✅  {len(df)} registros enviados para o BigQuery!", 1)
    except Exception as e:
        log(f"❌  Erro ao enviar ao BigQuery: {e}", 0)

def openweather_forecast(event, context):
    log("⏳ Iniciando função openweather_forecast", 1)
    df = collect_once()
    df = padroniza_df_bq(df)
    write_bq(df)
    log("🏁 Execução finalizada", 1)

