import time
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(CREDENTIALS_PATH), project="local-bliss-359814")

q_hourly = """
SELECT 
  FORMAT_TIMESTAMP('%H:%M', datahora_alvo) as Hora,
  previsto_2h_congelado as Previsao,
  medicao_real_boia as Real,
  erro_2h_nos as Erro,
  acerto_perc_2h as Assertividade
FROM `local-bliss-359814.wherehouse_tratado.vw_analise_correnteza`
WHERE DATE(datahora_alvo) = '2026-04-06'
  AND previsto_2h_congelado IS NOT NULL
  AND medicao_real_boia IS NOT NULL
ORDER BY datahora_alvo ASC
"""

q_daily = """
SELECT 
  ROUND(AVG(acerto_perc_2h), 2) as Assertividade_Media_Dia
FROM `local-bliss-359814.wherehouse_tratado.vw_analise_correnteza`
WHERE DATE(datahora_alvo) = '2026-04-06'
  AND previsto_2h_congelado IS NOT NULL
  AND medicao_real_boia IS NOT NULL
"""

print("=== ASSERTIVIDADE HORA A HORA (06/04/2026) ===")
try:
    df_hourly = bq_client.query(q_hourly).to_dataframe()
    if df_hourly.empty:
        print("Nenhum cruzamento encontrado para o dia selecionado onde haja previsto e real simultaneamente.")
    else:
        print(df_hourly.to_string(index=False))
        
    df_daily = bq_client.query(q_daily).to_dataframe()
    if not df_daily.empty:
        print("\n==============================================")
        print(f"🎯 MÉDIA DO DIA: {df_daily['Assertividade_Media_Dia'].iloc[0]} % de Acurácia!")
        print("==============================================")
except Exception as e:
    print(f"Erro: {e}")
