from google.oauth2 import service_account
from google.cloud import bigquery

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

query = "TRUNCATE TABLE `local-bliss-359814.wherehouse_tratado.previsoes_oficiais`"
bq_client.query(query).result()
print("Tabela truncada com sucesso!")
