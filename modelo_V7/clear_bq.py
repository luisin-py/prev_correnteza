import warnings
warnings.filterwarnings('ignore')

from google.cloud import bigquery
from google.oauth2 import service_account

CREDENTIALS_PATH = r"C:\Users\LUIS\Desktop\previsoes_dados_agua\2026_kevi\chave.json"
PROJECT_ID = "local-bliss-359814"
TABLE_PREVISOES = f"{PROJECT_ID}.wherehouse_tratado.previsoes_oficiais"

credentials = service_account.Credentials.from_service_account_file(CREDENTIALS_PATH)
bq_client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

print(f"Limpando a tabela {TABLE_PREVISOES}...")
query = f"TRUNCATE TABLE `{TABLE_PREVISOES}`"
bq_client.query(query).result()

print("Tabela completamente apagada (zerada) com sucesso!")
