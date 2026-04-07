#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
update_chuvas_on_linux.py  –  coleta horária + back-fill de até 10 anos

• Usuário informa quantos anos deseja retroceder (0–10).
• O período é quebrado em blocos ≤ 6 meses (limite do INMET).
• Para cada bloco, o script ajusta “Data Inicial” e “Data Final” no
  portal, baixa o CSV, renomeia  ESTACAO_YYYYMMDD_YYYYMMDD.csv.
• Depois de completar todos os blocos da estação, processa e envia
  ao MySQL + BigQuery, então parte para a próxima estação.
• Qualquer exceção gera log, mas o loop continua rodando.
"""

import os, time, glob, logging, tempfile, datetime
from datetime import date, timedelta
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import UnexpectedAlertPresentException

import mysql.connector
from google.cloud import bigquery

# usar o ssd para memoria utilizada pelo selenium
import tempfile, os
tempfile.tempdir = "/mnt/ssd/tmp"
os.environ["TMPDIR"] = "/mnt/ssd/tmp"

BASE_DIR     = "/mnt/ssd/Projetos/Trabalho/Kevi/Praticagem/update_apis/Update_Chuva"
DOWNLOAD_DIR = os.path.join(BASE_DIR, "data"); os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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

nivel      = input("Nível de log (1=só proc, 2=+dados, 3=+nav): ")
LEVEL_TERM = int(nivel) if nivel.isdigit() and int(nivel) in (1,2,3) else 1
logging.info("==== Início do script (nível %s) ====", LEVEL_TERM)

# ───────── perguntar anos de back-fill ─────────
try:
    anos = int(input("Quantos anos de back-fill (0-30)? ").strip() or "0")
    if not 0 <= anos <= 30: raise ValueError
except ValueError:
    anos = 0
hoje, DATA_INICIAL = date.today(), date.today() - timedelta(days=365*anos)
print(f"📅 Período solicitado: {DATA_INICIAL} → {hoje}")

ESTACOES = {
    "A801": "Porto Alegre",
    "A802": "Rio Grande",
    "A878": "Mostardas",
    "A838": "Camaquã",
    "A887": "Pelotas",
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

def blocos_seis_meses(ini:date, fim:date):
    cur = ini
    while cur <= fim:
        bloco_fim = min(cur + timedelta(days=182), fim)
        yield cur, bloco_fim
        cur = bloco_fim + timedelta(days=1)

def selenium_download(est, dt_ini, dt_fim, headless=True):
    try:
        if LEVEL_TERM>=1:
            print(f"   📆 {dt_ini} → {dt_fim}")
        opts = webdriver.ChromeOptions()
        if headless: opts.add_argument("--headless")
        opts.add_argument("--no-sandbox"); opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument(f"--user-data-dir={tempfile.mkdtemp(dir=tempfile.tempdir)}")
        opts.add_experimental_option("prefs", {
            "download.default_directory": DOWNLOAD_DIR,
            "download.prompt_for_download": False,
        })
        drv  = webdriver.Chrome(options=opts)
        wait = WebDriverWait(drv, 20)

        try:
            drv.get("https://tempo.inmet.gov.br/TabelaEstacoes/")
            time.sleep(4)

            sb = drv.find_element(By.CLASS_NAME, "ui.vertical.ui.overlay.left.sidebar.menu")
            drv.execute_script("arguments[0].classList.add('visible');", sb)
            wait.until(EC.element_to_be_clickable(
                (By.XPATH,"//button[text()='Automáticas']"))).click()

            menus = drv.find_elements(By.CLASS_NAME,"menu.transition")
            if len(menus)>=3:
                drv.execute_script("arguments[0].classList.add('visible');", menus[2])

            for el in drv.find_elements(By.XPATH,"//div[@class='item']"):
                if est in el.text:
                    el.click(); break

            def set_date(css_selector, iso_value):
                campo = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, css_selector)))
                drv.execute_script("""
                    const el=arguments[0],v=arguments[1],last=el.value;
                    el.value=v;
                    const evt=new Event('input',{bubbles:true});
                    const tr=el._valueTracker;if(tr){tr.setValue(last);}
                    el.dispatchEvent(evt);
                    el.dispatchEvent(new Event('change',{bubbles:true}));
                """, campo, iso_value)

            css_ini = ("#root > div.pushable.sidebar-content > div.ui.vertical.ui.overlay.left."
                       "visible.sidebar.menu > div:nth-child(2) > div:nth-child(8) > input[type=date]")
            css_fim = ("#root > div.pushable.sidebar-content > div.ui.vertical.ui.overlay.left."
                       "visible.sidebar.menu > div:nth-child(2) > div:nth-child(10) > input[type=date]")

            set_date(css_ini, dt_ini.isoformat())
            set_date(css_fim, dt_fim.isoformat())
            if LEVEL_TERM>=1:
                print(f"     ⏳ Datas ajustadas")

            time.sleep(0.5)
            btn = wait.until(EC.element_to_be_clickable((By.XPATH,'//button[text()="Gerar Tabela"]')))
            drv.execute_script("arguments[0].scrollIntoView()", btn)
            drv.execute_script("arguments[0].click()", btn)

            down = wait.until(EC.element_to_be_clickable(
                (By.XPATH,"/html/body/div[1]/div[2]/div[2]/div/div/div/span/a")))
            ActionChains(drv).move_to_element(down).perform(); down.click()

            for _ in range(40):
                if any(f.endswith(".csv") for f in os.listdir(DOWNLOAD_DIR)):
                    break
                time.sleep(1)

        finally:
            drv.quit()

        ultimo = sorted(glob.glob(os.path.join(DOWNLOAD_DIR,"*.csv")),
                        key=os.path.getctime)[-1]
        novo   = f"{est}_{dt_ini:%Y%m%d}_{dt_fim:%Y%m%d}.csv"
        os.rename(ultimo, os.path.join(DOWNLOAD_DIR, novo))
        if LEVEL_TERM>=2:
            print("     📁", novo)
        logging.info("CSV salvo: %s", novo)
        return True

    except Exception as e:
        print(f"     ❌ Falha {dt_ini}->{dt_fim}: {e}")
        logging.error("Falha bloco %s %s-%s: %s", est, dt_ini, dt_fim, e)
        return False

def processar_csv(csv_path):
    try:
        est = os.path.basename(csv_path).split("_")[0]
        df  = pd.read_csv(csv_path, sep=";", dtype=str, na_values=["","nan"])
        df  = df[df["Chuva (mm)"].str.replace(",",".").astype(float,errors="ignore").notna()]
        if df.empty: return
        df["dt_utc"]=pd.to_datetime(df["Data"]+" "+df["Hora (UTC)"].str.zfill(4),
                                    format="%d/%m/%Y %H%M")

        cnx=mysql.connector.connect(**MYSQL_CFG); cur=cnx.cursor()
        ids=df["dt_utc"].dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
        if ids:
            cur.execute(f"SELECT dt_utc FROM dados_inmet_estacoes_backfill "
                        f"WHERE estacao=%s AND dt_utc IN ({','.join(['%s']*len(ids))})",
                        [est]+ids)
            existentes={r[0] for r in cur.fetchall()}
            df=df[~df["dt_utc"].isin(existentes)]
        if df.empty:
            cur.close(); cnx.close(); return

        agora_local = datetime.datetime.now()
        recs=[]
        for _,r in df.iterrows():
            rec={"estacao":est,"dt_utc":r["dt_utc"].to_pydatetime(),
                 "data":r["dt_utc"].date(),"hora_utc":r["Hora (UTC)"].zfill(4)}
            for orig,new in MAP.items():
                if new in ("data","hora_utc"): continue
                v=r.get(orig,"")
                rec[new]=float(str(v).replace(",",".") ) if v not in ("",None) else None
            rec["timestamp_execucao"] = agora_local
            recs.append(rec)

        sql = ("INSERT INTO dados_inmet_estacoes_backfill ("+COLS_MYSQL+") VALUES ("+
               ",".join("%("+c.strip()+")s" for c in COLS_MYSQL.split(","))+") "
               "ON DUPLICATE KEY UPDATE chuva=VALUES(chuva)")
        cur.executemany(sql,recs); cur.close(); cnx.close()
        if LEVEL_TERM>=1: print(f"✅ MySQL +{len(recs)}")

        df_bq = pd.DataFrame(recs)
        bigquery.Client().load_table_from_dataframe(
            df_bq, BQ_TABLE,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND",
                                              autodetect=True,
                                              schema_update_options=["ALLOW_FIELD_ADDITION"])
        ).result()
        if LEVEL_TERM>=1: print(f"✅ BigQuery +{len(df_bq)}")

    except Exception as e:
        print(f"❌ Erro processamento {csv_path}: {e}")
        logging.error("Process CSV %s: %s", csv_path, e)

def coletar_estacao(est):
    ok=False
    for ini,fim in blocos_seis_meses(DATA_INICIAL, hoje):
        ok |= selenium_download(est, ini, fim, headless=(LEVEL_TERM<3))

    if ok:
        for csv in glob.glob(os.path.join(DOWNLOAD_DIR,f"{est}_*.csv")):
            processar_csv(csv)
            os.remove(csv)
    else:
        print(f"⚠️ Nenhum CSV baixado para {est}")

def limpar_data_dir():
    for f in glob.glob(os.path.join(DOWNLOAD_DIR,"*")):
        if not f.endswith(".txt"): os.remove(f)

def dormir_ate_proxima_hora():
    agora=datetime.datetime.now()
    prox =(agora+timedelta(hours=1)).replace(minute=0,second=5,microsecond=0)
    time.sleep((prox-agora).total_seconds())

if __name__=="__main__":
    while True:
        for est in ESTACOES:
            coletar_estacao(est)
        limpar_data_dir()
        dormir_ate_proxima_hora()
