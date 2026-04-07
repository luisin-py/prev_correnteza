from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
bq_client = bigquery.Client(credentials=service_account.Credentials.from_service_account_file(CREDENTIALS_PATH), project="local-bliss-359814")

q = "SELECT MAX(direcao_superficie), MAX(ventonum) FROM `local-bliss-359814.wherehouse_tratado.mestre_hour_tratada`"
print(bq_client.query(q).to_dataframe())
