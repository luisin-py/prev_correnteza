#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from google.cloud import bigquery

API_KEY = os.getenv("OPENWEATHER_API_KEY", "COLOQUE_SUA_CHAVE_OPENWEATHER_AQUI")
CITIES = ["Rio Grande", "São José do Norte", "Pelotas", "Tapes", "Arambaré", "Camaquã", "São Lourenço do Sul"]
BQ_TABLE_ID = "local-bliss-359814.wherehouse.dados_openweather_air_pollution"

def now_brasilia():
    return datetime.now(timezone(timedelta(hours=-3)))

def ensure_table_exists():
    client = bigquery.Client()
    table_ref = bigquery.TableReference.from_string(BQ_TABLE_ID)
    try:
        client.get_table(table_ref)
    except Exception:
        schema = [
            bigquery.SchemaField("timestamp_utc", "TIMESTAMP"),
            bigquery.SchemaField("timestamp_execucao", "TIMESTAMP"),
            bigquery.SchemaField("city_name", "STRING"),
            bigquery.SchemaField("lat", "FLOAT"),
            bigquery.SchemaField("lon", "FLOAT"),
            bigquery.SchemaField("aqi", "INTEGER"),
            bigquery.SchemaField("co", "FLOAT"),
            bigquery.SchemaField("no", "FLOAT"),
            bigquery.SchemaField("no2", "FLOAT"),
            bigquery.SchemaField("o3", "FLOAT"),
            bigquery.SchemaField("so2", "FLOAT"),
            bigquery.SchemaField("pm2_5", "FLOAT"),
            bigquery.SchemaField("pm10", "FLOAT"),
            bigquery.SchemaField("nh3", "FLOAT"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print("🆕 Tabela criada com sucesso.")

def collect_air_pollution_data():
    rows = []
    for city in CITIES:
        print(f"📍 Coletando para: {city}")
        try:
            geocode = requests.get(
                "http://api.openweathermap.org/geo/1.0/direct",
                params={"q": f"{city},BR", "appid": API_KEY, "limit": 1}
            ).json()
            if not geocode:
                continue
            lat = geocode[0]["lat"]
            lon = geocode[0]["lon"]

            r = requests.get(
                "https://api.openweathermap.org/data/2.5/air_pollution",
                params={"lat": lat, "lon": lon, "appid": API_KEY}
            )
            r.raise_for_status()
            data = r.json()
            if not data.get("list"):
                continue

            values = data["list"][0]
            comps = values.get("components", {})
            row = {
                "timestamp_utc": datetime.utcfromtimestamp(values["dt"]),
                "timestamp_execucao": now_brasilia(),
                "city_name": city,
                "lat": lat,
                "lon": lon,
                "aqi": values.get("main", {}).get("aqi"),
                "co": comps.get("co"),
                "no": comps.get("no"),
                "no2": comps.get("no2"),
                "o3": comps.get("o3"),
                "so2": comps.get("so2"),
                "pm2_5": comps.get("pm2_5"),
                "pm10": comps.get("pm10"),
                "nh3": comps.get("nh3"),
            }
            rows.append(row)
        except Exception as e:
            print(f"❌ Erro para {city}: {e}")
    return pd.DataFrame(rows)

def write_to_bigquery(df):
    if df.empty:
        print("⚠️ Nenhum dado para enviar.")
        return
    client = bigquery.Client()
    job = client.load_table_from_dataframe(
        df,
        BQ_TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    )
    job.result()
    print(f"✅ {len(df)} registros enviados com sucesso.")

if __name__ == "__main__":
    ensure_table_exists()
    df = collect_air_pollution_data()
    write_to_bigquery(df)

