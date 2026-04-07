#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_chuvas_cloudrun.py – Fetches rainfall‑station CSVs from INMET, loads them
into BigQuery and deduplicates there.  Designed for **one‑shot** execution on
Cloud Run: every HTTP request to /run triggers exactly one full pass over all
stations – *no infinite loops, no terminal prompts*.

Environment variables expected by the container
───────────────────────────────────────────────
BQ_TABLE_ID      fully‑qualified destination table (default shown below)
LOG_LEVEL        0 = silent, 1 = progress, 2 = +data (default 1)
CHROME_BINARY    path to chromium browser (Cloud Run ≈ "/usr/bin/chromium-browser")
CHROMEDRIVER     path to chromedriver (Cloud Run ≈ "/usr/bin/chromedriver")
TIMEOUT_SEC      page load timeout for Selenium (default 20)
"""

import os, time, glob, logging, tempfile
from datetime import datetime, timedelta

import pandas as pd
from flask import Flask, jsonify

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException

from google.cloud import bigquery

# ───────────────────────────── Configuration ─────────────────────────────
BASE_DIR     = tempfile.gettempdir()
DOWNLOAD_DIR = os.path.join(BASE_DIR, "chuvas_dl")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

BQ_TABLE = os.getenv("BQ_TABLE_ID", "local-bliss-359814.wherehouse.dados_openwhather")
LOG_LVL  = int(os.getenv("LOG_LEVEL", "1"))
TIMEOUT  = int(os.getenv("TIMEOUT_SEC", "20"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%d/%m/%Y %H:%M:%S",
)

ESTACOES = {
    "A801": "Porto Alegre",
    "A802": "Rio Grande",
    "A878": "Mostardas",
    "A838": "Camaquã",
    "A887": "Pelotas",
}

MAP = {
    "Data": "data",                "Hora (UTC)": "hora_utc",
    "Temp. Ins. (C)": "temperatura_inst",      "Temp. Max. (C)": "temperatura_max",
    "Temp. Min. (C)": "temperatura_min",      "Umi. Ins. (%)": "umidade_inst",
    "Umi. Max. (%)": "umidade_max",           "Umi. Min. (%)": "umidade_min",
    "Pto Orvalho Ins. (C)": "pto_orvalho_inst", "Pto Orvalho Max. (C)": "pto_orvalho_max",
    "Pto Orvalho Min. (C)": "pto_orvalho_min", "Pressao Ins. (hPa)": "pressao_inst",
    "Pressao Max. (hPa)": "pressao_max",       "Pressao Min. (hPa)": "pressao_min",
    "Vel. Vento (m/s)": "vento_vel_m_s",      "Dir. Vento (m/s)": "vento_dir_deg",
    "Raj. Vento (m/s)": "vento_raj_m_s",      "Radiacao (KJ/m²)": "radiacao",
    "Chuva (mm)": "chuva",
}

# BigQuery client is thread‑safe; keep one global instance
bq_client = bigquery.Client()

# ───────────────────────────── Core functions ─────────────────────────────

def _chromedriver():
    """Return a Selenium Chrome WebDriver pre‑configured for Cloud Run."""
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    })
    # Cloud Run custom paths
    opts.binary_location = os.getenv("CHROME_BINARY", "/usr/bin/chromium")
    service = Service(os.getenv("CHROMEDRIVER", "/usr/bin/chromedriver"))
    return webdriver.Chrome(service=service, options=opts)


def baixar_estacao(est: str) -> str | None:
    """Download CSV for a single station and return local file path (or None on error)."""
    try:
        if LOG_LVL >= 1:
            print(f"➡️  Baixando {est}…", flush=True)

        drv  = _chromedriver()
        wait = WebDriverWait(drv, TIMEOUT)

        try:
            # 1. Abre página
            for _ in range(3):
                try:
                    drv.get("https://tempo.inmet.gov.br/TabelaEstacoes/")
                    break
                except UnexpectedAlertPresentException:
                    drv.switch_to.alert.accept(); time.sleep(2)

            # 2. Força sidebar visível & seleciona automática
            time.sleep(5)
            sb = drv.find_element(By.CLASS_NAME, "ui.vertical.ui.overlay.left.sidebar.menu")
            drv.execute_script("arguments[0].classList.add('visible');", sb)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Automáticas']"))).click()

            # 3. Mostra lista de estações e clica na desejada
            menus = drv.find_elements(By.CLASS_NAME, "menu.transition")
            if len(menus) >= 3:
                drv.execute_script("arguments[0].classList.add('visible');", menus[2])
            for el in drv.find_elements(By.XPATH, "//div[@class='item']"):
                if est in el.text:
                    el.click(); break

            # 4. Gera tabela & baixa CSV
            drv.execute_script("arguments[0].click();",
                wait.until(EC.element_to_be_clickable((By.XPATH, '//button[text()="Gerar Tabela"]'))))
            down = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div/div/div/span/a")))
            ActionChains(drv).move_to_element(down).perform(); down.click()

            # 5. Aguarda arquivo
            for _ in range(30):
                csvs = [p for p in os.listdir(DOWNLOAD_DIR) if p.endswith(".csv")]
                if csvs: break
                time.sleep(1)
        finally:
            drv.quit()

        csv_baixado = max(glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")), key=os.path.getctime)
        csv_destino = os.path.join(DOWNLOAD_DIR, f"{est}.csv")
        if os.path.exists(csv_destino):
            os.remove(csv_destino)
        os.rename(csv_baixado, csv_destino)
        logging.info("CSV salvo: %s", os.path.basename(csv_destino))
        return csv_destino

    except Exception as exc:
        logging.error("Erro ao baixar estação %s: %s", est, exc)
        return None


def processar_csv(csv_path: str) -> int:
    """Parse CSV, upload to BigQuery. Returns number of new rows inserted."""
    est = os.path.basename(csv_path).split(".")[0]
    df  = pd.read_csv(csv_path, sep=";", dtype=str, na_values=["", "nan"])

    # Mantém apenas linhas com valor de chuva válido
    chuva_num = df["Chuva (mm)"].str.replace(",", ".").astype(float, errors="ignore")
    df = df[chuva_num.notna()]
    if df.empty:
        return 0

    df["dt_utc"] = pd.to_datetime(df["Data"] + " " + df["Hora (UTC)"].str.zfill(4),
                                   format="%d/%m/%Y %H%M")

    registros = []
    for _, r in df.iterrows():
        rec = {
            "estacao": est,
            "dt_utc":  r["dt_utc"].to_pydatetime(),
        }
        for orig, new in MAP.items():
            if new == "hora_utc":
                rec["hora_utc"] = r["Hora (UTC)"].zfill(4)
            elif new == "data":
                rec["data"] = r["dt_utc"].date()
            else:
                v = r.get(orig, "")
                rec[new] = float(str(v).replace(",", ".")) if v not in (None, "") else None
        registros.append(rec)

    if not registros:
        return 0

    # DataFrame → BigQuery
    df_bq = pd.DataFrame(registros)
    job = bq_client.load_table_from_dataframe(
        df_bq, BQ_TABLE,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",)
    )
    job.result()  # bloqueia até concluir
    logging.info("BigQuery inseridas: %d (%s)", len(df_bq), est)
    return len(df_bq)


def deduplicar_bq():
    """Remove duplicatas no destino baseado em (estacao, dt_utc)."""
    logging.info("Executando deduplicação BigQuery…")
    dedup_sql = f"""
    CREATE OR REPLACE TABLE `{BQ_TABLE}` AS
    SELECT * EXCEPT(row_num) FROM (
      SELECT *, ROW_NUMBER() OVER (PARTITION BY estacao, dt_utc) AS row_num
      FROM `{BQ_TABLE}`
    ) WHERE row_num = 1
    """
    bq_client.query(dedup_sql).result()


# ───────────────────────────── Flask app entrypoint ─────────────────────────────
app = Flask(__name__)

@app.get("/")
def health():
    """Cheap health‑check."""
    return "ok", 200


@app.post("/run")
@app.get("/run")
def run_once():
    """Trigger one end‑to‑end data collection pass."""
    total_new = 0
    for est in ESTACOES:
        csv = baixar_estacao(est)
        if csv:
            total_new += processar_csv(csv)
            os.remove(csv)

    deduplicar_bq()

    payload = {"rows_inserted": total_new, "stations": len(ESTACOES)}
    if LOG_LVL >= 1:
        print("✔️  Execução concluída:", payload, flush=True)
    return jsonify(payload)


if __name__ == "__main__":
    # For local testing; Cloud Run sets PORT automatically
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
