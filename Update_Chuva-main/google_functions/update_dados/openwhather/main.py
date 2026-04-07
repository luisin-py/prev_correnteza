#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OpenWeather → BigQuery (Cloud Functions Gen 2).
Compatível com o schema:
  • NUMERIC   → decimal.Decimal
  • FLOAT64   → float
  • INT64     → pandas Int64 (nullable)
  • TIMESTAMP → datetime64[ns] (naive)
"""

from __future__ import annotations
import logging, os, unicodedata
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import List, Optional

import pandas as pd
import requests
from google.cloud import bigquery

# ─────── Config ───────
DEBUG_LOGS  = True
API_KEY     = os.getenv("OPENWEATHER_API_KEY")
TIMEOUT     = int(os.getenv("OW_TIMEOUT", "10"))
LOCAL_TZ    = timezone(timedelta(hours=-3))
BQ_TABLE_ID = os.getenv(
    "BQ_TABLE_ID", "local-bliss-359814.wherehouse.dados_openwhather"
)

CITIES = [
    c.strip() for c in os.getenv(
        "CITIES",
        "Rio Grande,São José do Norte,Pelotas,Tapes,Arambaré,Camaquã,São Lourenço do Sul"
    ).split(",") if c.strip()
]

if not API_KEY:
    raise RuntimeError("Defina OPENWEATHER_API_KEY!")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger()

# ─────── Helper funcs ───────
norm = lambda s: unicodedata.normalize("NFC", str(s)) if s is not None else None
def to_decimal(v):
    try:
        return Decimal(str(v)) if v not in (None, "") else None
    except (InvalidOperation, ValueError):
        return None
def epoch_naive(ts: Optional[int]):
    return datetime.utcfromtimestamp(ts) if ts else None  # naive timestamp

# ─────── Tipagem alvo ───────
NUMERIC_COLS = {
    "coord_lon","coord_lat",
    "main_temp","main_feels_like","main_temp_min","main_temp_max",
    "wind_speed","wind_gust",
    "rain_1h","rain_3h","snow_1h","snow_3h",
}
FLOAT_COLS = {"sys_type", "sys_id"}          # ← NOVO
INT64_COLS = {
    "city_id","weather_id",
    "main_pressure","main_humidity","main_sea_level","main_grnd_level",
    "visibility","wind_deg","clouds_all","timezone_offset","cod",
}
DATETIME_COLS = {"timestamp","timestamp_utc","dt","sys_sunrise","sys_sunset"}

# ─────── Coleta ───────
def run_collection() -> pd.DataFrame:
    rows, now_utc = [], datetime.utcnow()
    now_local = datetime.now(LOCAL_TZ).replace(tzinfo=None)

    for city in CITIES:
        try:
            r = requests.get(
                "https://api.openweathermap.org/data/2.5/weather",
                params={"q": f"{city},BR", "appid": API_KEY, "units": "metric"},
                timeout=TIMEOUT,
            )
            r.raise_for_status()
            d = r.json()
            if DEBUG_LOGS:
                log.info("🔍 JSON (%s): %s", city, d)

            w, m, wn  = d.get("weather",[{}])[0], d.get("main",{}), d.get("wind",{})
            cl, rn, sn, sy = d.get("clouds",{}), d.get("rain",{}), d.get("snow",{}), d.get("sys",{})

            rows.append({
                "timestamp"      : now_local,
                "timestamp_utc"  : now_utc,
                "city"           : norm(d.get("name")),
                "city_id"        : d.get("id"),
                "coord_lon"      : to_decimal(d.get("coord",{}).get("lon")),
                "coord_lat"      : to_decimal(d.get("coord",{}).get("lat")),
                "weather_id"     : w.get("id"),
                "weather_main"   : norm(w.get("main")),
                "weather_description": norm(w.get("description")),
                "weather_icon"   : w.get("icon"),
                "base"           : d.get("base"),
                "main_temp"      : to_decimal(m.get("temp")),
                "main_feels_like": to_decimal(m.get("feels_like")),
                "main_temp_min"  : to_decimal(m.get("temp_min")),
                "main_temp_max"  : to_decimal(m.get("temp_max")),
                "main_pressure"  : m.get("pressure"),
                "main_humidity"  : m.get("humidity"),
                "main_sea_level" : m.get("sea_level"),
                "main_grnd_level": m.get("grnd_level"),
                "visibility"     : d.get("visibility"),
                "wind_speed"     : to_decimal(wn.get("speed")),
                "wind_deg"       : wn.get("deg"),
                "wind_gust"      : to_decimal(wn.get("gust")),
                "clouds_all"     : cl.get("all"),
                "rain_1h"        : to_decimal(rn.get("1h")),
                "rain_3h"        : to_decimal(rn.get("3h")),
                "snow_1h"        : to_decimal(sn.get("1h")),
                "snow_3h"        : to_decimal(sn.get("3h")),
                "dt"             : epoch_naive(d.get("dt")),
                "sys_type"       : sy.get("type"),
                "sys_id"         : sy.get("id"),
                "sys_country"    : sy.get("country"),
                "sys_sunrise"    : epoch_naive(sy.get("sunrise")),
                "sys_sunset"     : epoch_naive(sy.get("sunset")),
                "timezone_offset": d.get("timezone"),
                "cod"            : d.get("cod"),
            })
            log.info("✅ %s coletado", city)
        except Exception as e:
            log.error("⚠️  %s: %s", city, e)

    return pd.DataFrame(rows)

# ─────── Gravação ───────
def write_bq(df: pd.DataFrame):
    if df.empty:
        log.warning("DataFrame vazio — nada a gravar.")
        return

    # conversões
    for c in df.columns:
        try:
            if c in DATETIME_COLS:
                df[c] = pd.to_datetime(df[c], errors="coerce")
            elif c in INT64_COLS:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
            elif c in FLOAT_COLS:
                df[c] = pd.to_numeric(df[c], errors="coerce").astype(float)
            elif c in NUMERIC_COLS:
                # já Decimal; só garante None quando vázio
                df[c] = df[c].where(df[c].notnull(), None)
            else:
                df[c] = df[c].astype("string")
        except Exception as exc:
            log.error("❌ conversão coluna %s -> %s: %s", c, df[c].dtype, exc)
            raise

    if DEBUG_LOGS:
        log.info("🔬 dtypes finais\n%s", df.dtypes)
        log.info("🟢 preview\n%s", df.head())

    bigquery.Client().load_table_from_dataframe(
        df, BQ_TABLE_ID,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    ).result()
    log.info("🚀 %d linhas gravadas em %s", len(df), BQ_TABLE_ID.split('.')[-1])

# ─────── entry-point ───────
def run_openweather(event, context):
    log.info("🚦 Início coleta OpenWeather")
    write_bq(run_collection())
    log.info("🏁 Fim coleta OpenWeather")

run_script = run_openweather
