from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(CREDENTIALS_PATH), project="local-bliss-359814")

query = """
CREATE OR REPLACE VIEW `local-bliss-359814.wherehouse_tratado.vw_analise_correnteza` AS
SELECT 
  P.datahora_alvo,
  ROUND(P.correnteza_superficie_prevista_dinamica, 3) AS previsto_dinamico,
  ROUND(P.correnteza_superficie_prevista_primeiro_calculo, 3) AS previsto_2h_congelado,
  ROUND(H.intensidade_superficie, 3) AS medicao_real_boia,
  
  -- Erro da dinâmica
  ROUND(ABS(H.intensidade_superficie - P.correnteza_superficie_prevista_dinamica), 3) AS erro_dinamico_nos,
  
  -- Erro do cálculo congelado (2h de lead time)
  ROUND(ABS(H.intensidade_superficie - P.correnteza_superficie_prevista_primeiro_calculo), 3) AS erro_2h_nos,
  
  -- Acurácia Percentual Dinâmica
  ROUND(GREATEST(0, (1 - (ABS(H.intensidade_superficie - P.correnteza_superficie_prevista_dinamica) / NULLIF(H.intensidade_superficie, 0))) * 100), 1) AS acerto_perc_dinamico,

  -- Acurácia Percentual 2H
  ROUND(GREATEST(0, (1 - (ABS(H.intensidade_superficie - P.correnteza_superficie_prevista_primeiro_calculo) / NULLIF(H.intensidade_superficie, 0))) * 100), 1) AS acerto_perc_2h
  
FROM `local-bliss-359814.wherehouse_tratado.previsoes_oficiais` P
LEFT JOIN `local-bliss-359814.wherehouse_tratado.mestre_hour_tratada` H
  ON P.datahora_alvo = CAST(H.timestamp_br AS TIMESTAMP)
ORDER BY P.datahora_alvo DESC;
"""
bq_client.query(query).result()
print("View criada com sucesso!")
