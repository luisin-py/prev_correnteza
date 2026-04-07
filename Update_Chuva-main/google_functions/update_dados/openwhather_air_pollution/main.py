#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
openweather_airpollution.py – coleta qualidade do ar para múltiplas cidades
"""

import os, requests, time
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

# ───────── Config ─────────
API_KEY     = os.getenv("OPENWEATHER_API_KEY", "SUA_CHAVE_AQUI")
CITIES      = [
    "Rio Grande", "São José do Norte", "Pelotas",
    "Tapes", "Arambaré", "Camaquã", "São Lourenço do Sul"
]
BQ_TABLE_ID = os.getenv(
    "BQ_TABLE_ID",
    "local-bliss-359814.wherehouse.dados_openweather_air_pollution"
)

# ───────── Schema BQ ─────────
SCHEMA_BQ = [
    bigquery.SchemaField("timestamp_execucao", "STRING"),
    bigquery.SchemaField("timestamp_utc", "STRING"),
    bigquery.SchemaField("city_name", "STRING"),
    bigquery.SchemaField("lat", "NUMERIC"),
    bigquery.SchemaField("lon", "NUMERIC"),
    bigquery.SchemaField("aqi", "NUMERIC"),
    bigquery.SchemaField("co", "NUMERIC"),
    bigquery.SchemaField("no", "NUMERIC"),
    bigquery.SchemaField("no2", "NUMERIC"),
    bigquery.SchemaField("o3", "NUMERIC"),
    bigquery.SchemaField("so2", "NUMERIC"),
    bigquery.SchemaField("pm2_5", "NUMERIC"),
    bigquery.SchemaField("pm10", "NUMERIC"),
    bigquery.SchemaField("nh3", "NUMERIC"),
]

# ───────── Helpers ─────────
def iso_utc(ts):          # epoch → ISO-8601 UTC
    return datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def now_brasilia():       # timestamp da execução
    return (datetime.now(timezone.utc) - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

def geocode(city):
    """Retorna (lat, lon) usando Geocoding API (cache simples em memória)."""
    if city in geocode.cache:
        return geocode.cache[city]
    r = requests.get(
        "https://api.openweathermap.org/geo/1.0/direct",
        params={"q": f"{city},BR", "limit": 1, "appid": API_KEY},
        timeout=10,
    )
    r.raise_for_status()
    j = r.json()
    if not j:
        raise RuntimeError(f"geocode vazio para {city}")
    lat, lon = j[0]["lat"], j[0]["lon"]
    geocode.cache[city] = (lat, lon)
    # respeita rate-limit geocoding (60/min) – intervalo mínimo 1 s
    time.sleep(1)
    return lat, lon
geocode.cache = {}

def padroniza(row, meta):
    comps = row["components"]
    return {
        "timestamp_execucao": now_brasilia(),
        "timestamp_utc"     : iso_utc(row["dt"]),
        "city_name"         : meta["city"],
        "lat"               : meta["lat"],
        "lon"               : meta["lon"],
        "aqi"               : row["main"]["aqi"],
        "co"                : comps.get("co"),
        "no"                : comps.get("no"),
        "no2"               : comps.get("no2"),
        "o3"                : comps.get("o3"),
        "so2"               : comps.get("so2"),
        "pm2_5"             : comps.get("pm2_5"),
        "pm10"              : comps.get("pm10"),
        "nh3"               : comps.get("nh3"),
    }

def ensure_table_exists(client):
    try:
        client.get_table(BQ_TABLE_ID)
    except Exception:
        table = bigquery.Table(BQ_TABLE_ID, schema=SCHEMA_BQ)
        client.create_table(table)
        print("📦 Tabela criada no BigQuery.")

# ───────── Core ─────────
def collect_all():
    rows = []
    for city in CITIES:
        try:
            lat, lon = geocode(city)
            url = "https://api.openweathermap.org/data/2.5/air_pollution"
            r = requests.get(url, params={"lat": lat, "lon": lon, "appid": API_KEY}, timeout=10)
            r.raise_for_status()
            data = r.json()
            row_std = padroniza(data["list"][0], {"city": city, "lat": lat, "lon": lon})
            rows.append(row_std)
            print(f"✅ {city}: AQI={row_std['aqi']}")
        except Exception as e:
            print(f"⚠️  {city}: {e}")
    return rows

def write_bq(rows):
    if not rows:
        print("⚠️  Nada para enviar ao BigQuery.")
        return
    client = bigquery.Client()
    ensure_table_exists(client)
    errors = client.insert_rows_json(BQ_TABLE_ID, rows, row_ids=[None] * len(rows))
    if errors:
        print("❌  Erros ao inserir:", errors)
    else:
        print(f"🚀 {len(rows)} registros enviados ao BigQuery.")

# ───────── Entry-point Cloud Function ─────────
def openweather_airpollution(event, context):
    rows = collect_all()
    write_bq(rows)

# Execução local (para testes)
if __name__ == "__main__":
    openweather_airpollution(None, None)
