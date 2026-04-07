#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, time, unicodedata, argparse, tempfile
import requests, pandas as pd
from datetime import datetime, timezone
from google.cloud import bigquery
import signal, sys

# ===============================
# CONFIGURAÇÕES
# ===============================
API_KEY = os.getenv("OPENWEATHER_API_KEY", "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI")
CITIES = [
    "Rio Grande", "São José do Norte", "Pelotas",
    "Tapes", "Arambaré", "Camaquã", "São Lourenço do Sul"
]
BQ_TABLE_ID = "local-bliss-359814.wherehouse.dados_openwhather"
# -------------------------------- #

# ---------- logging cli --------- #
parser = argparse.ArgumentParser()
parser.add_argument("-l", "--log", type=int, default=1,
                    help="Log 0=quiet, 1=normal, 2=verbose")
LOG = parser.parse_args().log
def log(msg, lvl=1):
    if LOG >= lvl:
        print(msg)
# -------------------------------- #

# ---------- entrada interativa do usuário ---------- #
try:
    intervalo_min = int(input("⏱️  A cada quantos minutos deseja coletar os dados? (ex: 5, 15): ").strip())
    if intervalo_min <= 0:
        raise ValueError
    SLEEP_SEC = intervalo_min * 60
except ValueError:
    print("❌  Valor inválido. Use um número inteiro positivo (em minutos).")


def norm(txt):
    return unicodedata.normalize("NFC", str(txt)) if txt is not None else None

def epoch_to_dt(ts):
    return datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None

def collect_once():
    """Coleta dados uma vez, envia ao BigQuery."""
    rows = []
    for city in CITIES:
        log(f"🌐  {city}", 1)
        try:
            r = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": f"{city},BR", "appid": API_KEY, "units": "metric"},
                timeout=10
            )
            r.raise_for_status()
            j = r.json()
            if LOG >= 2:
                log(f"🧾 JSON {city}: {j}", 2)

            w     = j.get("weather", [{}])[0]
            main  = j.get("main", {})
            wind  = j.get("wind", {})
            clouds= j.get("clouds", {})
            rain  = j.get("rain", {})
            snow  = j.get("snow", {})
            sys   = j.get("sys", {})

            rows.append({
                "city":                norm(j.get("name")),
                "city_id":             j.get("id"),
                "coord_lon":           j.get("coord", {}).get("lon"),
                "coord_lat":           j.get("coord", {}).get("lat"),
                "weather_id":          w.get("id"),
                "weather_main":        norm(w.get("main")),
                "weather_description": norm(w.get("description")),
                "weather_icon":        w.get("icon"),
                "base":                j.get("base"),
                "main_temp":           main.get("temp"),
                "main_feels_like":     main.get("feels_like"),
                "main_temp_min":       main.get("temp_min"),
                "main_temp_max":       main.get("temp_max"),
                "main_pressure":       main.get("pressure"),
                "main_humidity":       main.get("humidity"),
                "main_sea_level":      main.get("sea_level"),
                "main_grnd_level":     main.get("grnd_level"),
                "visibility":          j.get("visibility"),
                "wind_speed":          wind.get("speed"),
                "wind_deg":            wind.get("deg"),
                "wind_gust":           wind.get("gust"),
                "clouds_all":          clouds.get("all"),
                "rain_1h":             rain.get("1h"),
                "rain_3h":             rain.get("3h"),
                "snow_1h":             snow.get("1h"),
                "snow_3h":             snow.get("3h"),
                "dt":                  epoch_to_dt(j.get("dt")),
                "sys_type":            sys.get("type"),
                "sys_id":              sys.get("id"),
                "sys_country":         sys.get("country"),
                "sys_sunrise":         epoch_to_dt(sys.get("sunrise")),
                "sys_sunset":          epoch_to_dt(sys.get("sunset")),
                "timezone_offset":     j.get("timezone"),
                "cod":                 j.get("cod"),
                "timestamp_utc":       datetime.utcnow().replace(tzinfo=timezone.utc)
            })

        except Exception as e:
            log(f"❌  {city}: {e}", 1)
        time.sleep(1)  # micro-delay evita burst

    if not rows:
        log("⚠️  Nenhum dado coletado", 1)
        return

    df = pd.DataFrame(rows)
    for col in ["dt", "sys_sunrise", "sys_sunset", "timestamp_utc"]:
        df[col] = pd.to_datetime(df[col]).dt.strftime("%Y-%m-%d %H:%M:%S")

    try:
        client = bigquery.Client()
        with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
            df.to_csv(tmp.name, index=False, encoding="utf-8")
            job_cfg = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=False  # schema já existe na tabela
            )
            with open(tmp.name, "rb") as f:
                job = client.load_table_from_file(f, BQ_TABLE_ID, job_config=job_cfg)
                job.result()
        log(f"✅  {len(df)} registros enviados", 1)
    except Exception as e:
        log(f"❌  Erro BigQuery: {e}", 1)

def graceful_exit(sig, frame):
    log("\n👋  Interrompido; saindo…", 0)
    sys.exit(0)

signal.signal(signal.SIGINT, graceful_exit)
signal.signal(signal.SIGTERM, graceful_exit)

# ---------- loop principal ---------- #
while True:
    start = time.time()
    collect_once()
    elapsed = time.time() - start
    sleep_for = max(0, SLEEP_SEC - elapsed)
    log(f"⏳  Próxima coleta em {sleep_for:.0f}s", 1)
    time.sleep(sleep_for)
