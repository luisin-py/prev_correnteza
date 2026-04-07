# 🌧️ Update_Chuva – Coleta e Backfill de Dados Pluviométricos INMET

Este projeto automatiza o download, tratamento e envio de dados meteorológicos (chuva e outros) de estações do INMET, utilizando **Selenium** para scraping da interface web do INMET, **MySQL** como banco local e **BigQuery** como destino em nuvem.

## 📌 Funcionalidades

- ⏳ Coleta automatizada por hora de estações meteorológicas
- 🌧️ Download de dados horário em formato CSV diretamente do portal do INMET
- 🔄 Processamento e inserção incremental no MySQL e BigQuery
- 🗓️ Suporte a backfill de até 180 dias retroativos
- ✅ Tratamento de exceções e robustez para execução contínua
- 🧪 Logs detalhados de execução no terminal e arquivo `log.txt`

---

## ⚙️ Requisitos

Instale os seguintes pacotes antes de rodar:

```bash
pip install selenium pandas mysql-connector-python google-cloud-bigquery
sudo apt install xvfb  

Você também precisará de:

Google Cloud SDK com autenticação 
configurada (GOOGLE_APPLICATION_CREDENTIALS)

Banco de dados MySQL acessível

Navegador Chrome + ChromeDriver compatível com sua versão


🚀 Execução
python3 update_chuvas_on_linux.py

Ao iniciar, o script perguntará:

O nível de log:

1 – Apenas processos principais

2 – Inclui dados lidos

3 – Exibe logs detalhados do navegador
# Para rodar em segundo plano execute com "xvfb-run -a" na frente do 
# comando python3, isso permitira que o selenium abra os navegadores 
# sem que o usuário veja.
xvfb-run -a
ex: xvfb-run -a python3 dados_inmet_estacoes.py

Se deseja fazer backfill:

Se "sim", informe o número de dias (máximo: 180)

Caso contrário, seguirá com o download da hora atual

🗃️ Estrutura do projeto
.
├── update_chuvas_on_backfill.py   # Script principal
├── data/                       # Armazena os arquivos CSV baixados
├── log.txt                     # Arquivo de log de execução
└── README.md                   # Este documento


🛠️ Configurações internas
Estações monitoradas
Você pode editar a variável ESTACOES no script para ajustar quais
estações serão monitoradas:

ESTACOES = {
    "A801": "Porto Alegre",
    "A802": "Rio Grande",
    "A878": "Mostardas",
    ...}


Conexões
MySQL: defina as credenciais no dicionário MYSQL_CFG

BigQuery: configure a tabela-alvo em BQ_TABLE e garanta que as 
credenciais estejam disponíveis via variável GOOGLE_APPLICATION_CREDENTIALS

📋 Exemplo de log no terminal
➡️  Baixando A801
📁 A801.csv
✅ MySQL +24
✅ BigQuery +24
⏳ Dormindo 3597 s até 12:00:05 ...

✅ Lista de Scripts desenvolvidos e seus propósitos:

dados_inmet_estacoes.py
Coleta os dados meteorológicos atuais de estações do INMET via navegador automatizado (Selenium).
O script pode rodar de forma visível ou em modo headless (nível de log 2).
Salva os dados no MySQL e BigQuery, substituindo duplicatas.

dados_inmet_estacoes_backfill_180dias.py
Script de backfill histórico para estações INMET, recuperando dados retroativos dos últimos 180 dias.
Utiliza automatização com Selenium para baixar os CSVs por estação.
Ideal para inicializar a base com dados recentes.

dados_inmet_estacoes_backfill_10anos.py
Executa o backfill completo de até 10 anos de dados históricos por estação INMET.
Automatiza a navegação no site do INMET e salva os arquivos CSV para posterior processamento.
Utilizado principalmente para recriar ou popular grandes volumes de dados desde o início da operação da estação.

dados_inmet_estacoes_backfill_10anos_linux.py
Versão do script de 10 anos otimizada para rodar em servidores Linux com ambientes sem interface gráfica.
Usa xvfb (ambiente gráfico virtual) para simular uma tela e permitir que o navegador rode em modo invisível, mesmo que não seja totalmente headless.