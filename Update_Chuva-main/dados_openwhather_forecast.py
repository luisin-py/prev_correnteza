#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, argparse, tempfile, requests, pandas as pd, signal, sys
from datetime import datetime, timezone
from google.cloud import bigquery

API_KEY = os.getenv("OPENWEATHER_API_KEY", "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI")
CITIES = ["Rio Grande", "São José do Norte", "Pelotas", "Tapes", "Arambaré", "Camaquã", "São Lourenço do Sul"]
BQ_TABLE_ID = "local-bliss-359814.wherehouse.dados_openweather_forecast"

parser = argparse.ArgumentParser()
parser.add_argument("-l", "--log", type=int, default=1, help="Log 0=quiet, 1=normal, 2=verbose")
LOG = parser.parse_args().log
def log(msg, lvl=1): print(msg) if LOG >= lvl else None

try:
    intervalo_min = int(input("⏱️  A cada quantos minutos deseja coletar os dados? (ex: 15min, 7200min '5dias'): ").strip())
    if intervalo_min <= 0: raise ValueError
    SLEEP_SEC = intervalo_min * 60
except ValueError:
    print("❌  Valor inválido. Use um número inteiro positivo.")
    sys.exit(1)

def epoch_to_dt(ts): return datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
def collect_once():
    rows = []
    for city in CITIES:
        log(f"🌐  {city}")
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
                    "timestamp_utc": datetime.utcnow().replace(tzinfo=timezone.utc),
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
            log(f"❌  {city}: {e}")
        time.sleep(1)

    if not rows:
        log("⚠️  Nenhum dado coletado")
        return

    df = pd.DataFrame(rows)
    df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    print("➡️  Colunas do DataFrame:", df.columns.tolist())
    print(df.head())


    try:
        client = bigquery.Client()
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
            df.to_csv(tmp.name, index=False, encoding="utf-8")
            job_cfg = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False
            )
            with open(tmp.name, "rb") as f:
                job = client.load_table_from_file(f, BQ_TABLE_ID, job_config=job_cfg)
                job.result()
        log(f"✅  {len(df)} registros enviados")
    except Exception as e:
        log(f"❌  Erro BigQuery: {e}")


def graceful_exit(sig, frame):
    log("\n👋  Interrompido; saindo…", 0)
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

while True:
    start = time.time()
    collect_once()
    elapsed = time.time() - start
    time.sleep(max(0, SLEEP_SEC - elapsed))

