#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_chuvas_on_linux.py  –  modo “1 estação por vez”
"""

import os, time, glob, logging, datetime
from datetime import timedelta
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException

import mysql.connector
from google.cloud import bigquery

# CONFIGURAÇÕES
BASE_DIR     = "/mnt/ssd/Projetos/Trabalho/Kevi/Praticagem/update_apis/Update_Chuva"
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

MYSQL_CFG = {
    "host": "localhost", "user": "root", "password": "Praticag3m2025",
    "database": "praticagem", "autocommit": True,
}
BQ_TABLE = "local-bliss-359814.wherehouse.dados_inmet_estacoes_backfill"

logging.basicConfig(
    filename=os.path.join(DOWNLOAD_DIR, "log.txt"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%d/%m/%Y %H:%M",
)

nivel = input("Nível de log (1=só proc, 2=+dados, 3=+nav): ")
LEVEL_TERM = int(nivel) if nivel.isdigit() and int(nivel) in (1, 2, 3) else 1
logging.info("==== Início do script (nível %s) ====", LEVEL_TERM)

# Pergunta backfill
BACKFILL_DIAS = 0
resposta = input("Deseja selecionar data de início? (sim/não): ").strip().lower()
if resposta == "sim":
    try:
        dias = input("Quantos dias de backfill deseja baixar? (máximo 180): ").strip()
        BACKFILL_DIAS = int(dias)
        if BACKFILL_DIAS < 1 or BACKFILL_DIAS > 180:
            print("⚠️ Insira um número entre 1 e 180. Usando 0 (sem backfill).")
            BACKFILL_DIAS = 0
    except Exception:
        print("⚠️ Valor inválido. Usando 0 (sem backfill).")
        BACKFILL_DIAS = 0

ESTACOES = {
    "A801": "Porto Alegre",
    "A802": "Rio Grande",
    # "A878": "Mostardas",
    "A838": "Camaquã",
    "A887": "Pelotas"
}

MAP = {
    "Data": "data", "Hora (UTC)": "hora_utc",
    "Temp. Ins. (C)": "temperatura_inst", "Temp. Max. (C)": "temperatura_max",
    "Temp. Min. (C)": "temperatura_min", "Umi. Ins. (%)": "umidade_inst",
    "Umi. Max. (%)": "umidade_max", "Umi. Min. (%)": "umidade_min",
    "Pto Orvalho Ins. (C)": "pto_orvalho_inst", "Pto Orvalho Max. (C)": "pto_orvalho_max",
    "Pto Orvalho Min. (C)": "pto_orvalho_min", "Pressao Ins. (hPa)": "pressao_inst",
    "Pressao Max. (hPa)": "pressao_max", "Pressao Min. (hPa)": "pressao_min",
    "Vel. Vento (m/s)": "vento_vel_m_s", "Dir. Vento (m/s)": "vento_dir_deg",
    "Raj. Vento (m/s)": "vento_raj_m_s", "Radiacao (KJ/m²)": "radiacao",
    "Chuva (mm)": "chuva",
}

COLS_MYSQL = (
    "estacao,dt_utc,data,hora_utc,temperatura_inst,temperatura_max,temperatura_min,"
    "umidade_inst,umidade_max,umidade_min,pto_orvalho_inst,pto_orvalho_max,pto_orvalho_min,"
    "pressao_inst,pressao_max,pressao_min,vento_vel_m_s,vento_dir_deg,vento_raj_m_s,"
    "radiacao,chuva"
)

def baixar_estacao(est, headless=True):
    try:
        if LEVEL_TERM >= 1:
            print(f"\n➡️  Baixando {est}")

        opts = webdriver.ChromeOptions()
        if headless:
            opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_experimental_option("prefs", {
            "download.default_directory": DOWNLOAD_DIR,
            "download.prompt_for_download": False,
        })
        drv = webdriver.Chrome(options=opts)
        wait = WebDriverWait(drv, 20)

        try:
            for _ in range(3):
                try:
                    drv.get("https://tempo.inmet.gov.br/TabelaEstacoes/")
                    break
                except UnexpectedAlertPresentException:
                    drv.switch_to.alert.accept()
                    time.sleep(2)

            time.sleep(5)

            sb = drv.find_element(By.CLASS_NAME, "ui.vertical.ui.overlay.left.sidebar.menu")
            drv.execute_script("arguments[0].classList.add('visible');", sb)
            wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Automáticas']"))).click()

            menus = drv.find_elements(By.CLASS_NAME, "menu.transition")
            if len(menus) >= 3:
                drv.execute_script("arguments[0].classList.add('visible');", menus[2])

            for el in drv.find_elements(By.XPATH, "//div[@class='item']"):
                if est in el.text:
                    el.click()
                    break

            time.sleep(2)

            if BACKFILL_DIAS > 0:
                data_inicio = (datetime.date.today() - timedelta(days=BACKFILL_DIAS)).isoformat()
                try:
                    campo_ini = wait.until(EC.element_to_be_clickable((
                        By.CSS_SELECTOR,
                        "#root > div.pushable.sidebar-content > div.ui.vertical.ui.overlay.left.visible.sidebar.menu > div:nth-child(2) > div:nth-child(8) > input[type=date]"
                    )))
                    drv.execute_script("""
                        const el = arguments[0];
                        const value = arguments[1];
                        const lastValue = el.value;
                        el.value = value;
                        const event = new Event('input', { bubbles: true });
                        const tracker = el._valueTracker;
                        if (tracker) { tracker.setValue(lastValue); }
                        el.dispatchEvent(event);
                        el.dispatchEvent(new Event('change', { bubbles: true }));
                    """, campo_ini, data_inicio)
                    if LEVEL_TERM >= 1:
                        print(f"⏳ Data inicial corrigida para {data_inicio}")
                except Exception as e:
                    print(f"⚠️ Erro ao corrigir data inicial: {e}")

            time.sleep(1)
            drv.execute_script("arguments[0].click();",
                wait.until(EC.element_to_be_clickable((By.XPATH, '//button[text()="Gerar Tabela"]'))))
            down = wait.until(EC.element_to_be_clickable(
                (By.XPATH, "/html/body/div[1]/div[2]/div[2]/div/div/div/span/a")))
            ActionChains(drv).move_to_element(down).perform()
            down.click()

            for _ in range(30):
                csvs = [p for p in os.listdir(DOWNLOAD_DIR) if p.endswith(".csv")]
                if csvs:
                    break
                time.sleep(1)

        finally:
            drv.quit()

        csv_baixado = sorted(
            glob.glob(os.path.join(DOWNLOAD_DIR, "*.csv")), key=os.path.getctime)[-1]
        csv_destino = os.path.join(DOWNLOAD_DIR, f"{est}.csv")
        if os.path.exists(csv_destino):
            os.remove(csv_destino)
        os.rename(csv_baixado, csv_destino)
        if LEVEL_TERM >= 2:
            print("📁", os.path.basename(csv_destino))
        logging.info("CSV salvo: %s", os.path.basename(csv_destino))
        return csv_destino

    except Exception as e:
        print(f"❌ Erro ao baixar {est}: {e}")
        logging.error("Erro ao baixar estação %s: %s", est, e)
        return None

# ─────────────────────── 2) PROCESSAR & ENVIAR ───────────────────────
def processar_csv(csv_path):
    try:
        est = os.path.basename(csv_path).split(".")[0]
        df  = pd.read_csv(csv_path, sep=";", dtype=str, na_values=["", "nan"])

        chuva_num = df["Chuva (mm)"].str.replace(",", ".").astype(float, errors="ignore")
        df = df[chuva_num.notna()]
        if df.empty:
            if LEVEL_TERM >= 2: print(f"⚠️ Nada novo para {est}")
            return

        df["dt_utc"] = pd.to_datetime(df["Data"] + " " + df["Hora (UTC)"].str.zfill(4),
                                      format="%d/%m/%Y %H%M")

        cnx = mysql.connector.connect(**MYSQL_CFG)
        cur = cnx.cursor()
        ids = df["dt_utc"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
        if ids:
            cur.execute(
                f"SELECT dt_utc FROM dados_inmet_estacoes_backfill "
                f"WHERE estacao=%s AND dt_utc IN ({', '.join(['%s']*len(ids))})",
                [est]+ids)
            existentes = {row[0] for row in cur.fetchall()}
            df = df[~df["dt_utc"].isin(existentes)]
        if df.empty:
            cur.close(); cnx.close()
            if LEVEL_TERM >= 2: print(f"⚠️ Nada novo depois de filtro MySQL – {est}")
            return

        # --- pega hora do servidor aqui! ---
        agora_local = datetime.datetime.now()

        registros = []
        for _, r in df.iterrows():
            rec = {
                "estacao": est,
                "dt_utc":  r["dt_utc"].to_pydatetime(),
                "data":    r["dt_utc"].date(),
                "hora_utc":r["Hora (UTC)"].zfill(4)
            }
            for orig, new in MAP.items():
                if new in ("data", "hora_utc"): continue
                v = r.get(orig, "")
                rec[new] = float(str(v).replace(",", ".")) if v not in (None,"") else None
            rec["timestamp_execucao"] = agora_local  # Envia como datetime
            registros.append(rec)

        sql = ("INSERT INTO dados_inmet_estacoes_backfill ("+COLS_MYSQL+") VALUES ("+
               ",".join("%("+c.strip()+")s" for c in COLS_MYSQL.split(","))+") "
               "ON DUPLICATE KEY UPDATE chuva=VALUES(chuva)")
        cur.executemany(sql, registros)
        cur.close(); cnx.close()
        if LEVEL_TERM >= 1: print(f"✅ MySQL +{len(registros)}")
        logging.info("MySQL inseridas: %d (%s)", len(registros), est)

        df_bq = pd.DataFrame(registros)
        # Se der erro de tipo, descomente a linha abaixo:
        # df_bq["timestamp_execucao"] = pd.to_datetime(df_bq["timestamp_execucao"])
        bigquery.Client().load_table_from_dataframe(
            df_bq, BQ_TABLE,
            job_config=bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                autodetect=True,
                schema_update_options=["ALLOW_FIELD_ADDITION"]
            )
        ).result()
        if LEVEL_TERM >= 1: print(f"✅ BigQuery +{len(df_bq)}")
        logging.info("BigQuery inseridas: %d (%s)", len(df_bq), est)

    except Exception as e:
        print(f"❌ Erro ao processar {csv_path}: {e}")
        logging.error("Erro ao processar CSV %s: %s", csv_path, e)

# ─────────────────────── util ───────────────────────
def dormir_ate_proxima_hora():
    agora = datetime.datetime.now()
    proxima = (agora + timedelta(hours=1)).replace(minute=0, second=5, microsecond=0)
    delta = (proxima - agora).total_seconds()
    if LEVEL_TERM >= 1:
        print(f"⏳ Dormindo {int(delta)} s até {proxima.time()} ...")
    time.sleep(delta)

def limpar_data_dir():
    for f in glob.glob(os.path.join(DOWNLOAD_DIR, "*")):
        if not f.endswith(".txt"):
            try:
                os.remove(f)
            except OSError:
                pass

# ─────────────────────────── MAIN LOOP ───────────────────────────
if __name__ == "__main__":
    while True:
        for est in ESTACOES:
            try:
                csv = baixar_estacao(est, headless=(LEVEL_TERM < 3))
                if csv:
                    processar_csv(csv)
                    if os.path.exists(csv): os.remove(csv)
            except Exception as e:
                print(f"❌ Erro inesperado ao processar estação {est}: {e}")
                logging.error("Erro inesperado ao processar estação %s: %s", est, e)

        limpar_data_dir()
        dormir_ate_proxima_hora()
