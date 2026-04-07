import datetime
import os
import time
import pandas as pd
#Connect with the api  
# cd C:\Users\Usuario\AppData\Local\Programs\Python\Python312\Scripts
# abrir jypter: jupyter lab
from googleapiclient.discovery import build
from google.oauth2 import service_account
import shutil
import logging  # Importe o módulo logging

download_path = "C:\\Users\\Usuario\\Documents\\Mares\\update_chuva\\123\\chrome-win64\\"
download_destination = os.path.join(download_path,"dados/")
download_deleta = 'C:\\Users\\Usuario\\Documents\\Mares\\update_chuva\\123\\chrome-win64\\'

# Configuração do log
log_filename = os.path.join(download_path, 'log.txt')
logging.basicConfig(filename=log_filename, 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s', 
                    datefmt='%d/%m/%Y %H:%M')

logging.warning('inicio script')
taksPath = download_path+'task.txt'
f = open(r''+taksPath,'a')
f.write(f'{datetime.datetime.now()} - runned \n')

def download_estacao(estacao):

    from selenium import webdriver
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC

    options = webdriver.ChromeOptions()
    # Defina o diretório de download para o novo caminho
    options.add_argument('--disable-web-security')
    options.add_argument("user-data-dir=" + download_path)

    #options.add_argument("--headless") # opcional: para executar o navegador em segundo plano
    prefs = {"download.default_directory": download_path, "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing_for_trusted_sources_enabled": False,
            "safebrowsing.enabled": False}
    options.add_experimental_option("prefs", prefs)

    # Não especifique o caminho do ChromeDriver no Selenium 4.x
    driver = webdriver.Chrome(options=options)
    # Abrir a página do formulário
    try:
        # Abrir a página do formulário
        driver.get("https://tempo.inmet.gov.br/TabelaEstacoes/")
        driver.minimize_window()
        wait = WebDriverWait(driver, 15)

        time.sleep(5)

        # Find the element you want to modify
        element = driver.find_element(By.CLASS_NAME,"ui.vertical.ui.overlay.left.sidebar.menu")
        # Use execute_script() to modify the class
        driver.execute_script("arguments[0].setAttribute('class','ui.vertical.ui.overlay.left.visible.sidebar.menu');", element)

        time.sleep(3)


        # Seelciona apenas estações automaticas
        automaticas_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[text()='Automáticas']")))
        automaticas_button.click()
        logging.info("Foi clicado em automaticas")
        time.sleep(2)


        element = driver.find_elements(By.CLASS_NAME,"menu.transition")
        if len(element) < 3:
            logging.info("Não existem pelo menos 3 elementos com a classe 'menu.transition'")
        else:
            terceiro_elemento = element[2]
        # fixa menu aberto
        driver.execute_script("arguments[0].setAttribute('class','visible.menu.transition');", terceiro_elemento)
        time.sleep(2)
        logging.info("search activated") 
    except Exception as e:
        logging.info("Erro na fase de abertura ", e)

    try:
        # localiza e clica na opção "CAMAQUA (A838)"
        element = driver.find_element(By.XPATH, "//div[@class='item' and contains(span/text(), '"+estacao+"')]") # div[@class="item" and span/text()="CAMAQUA (A838)"]'
        element.click()
        time.sleep(1)
        element.send_keys(Keys.RETURN)
    except Exception as e:
        logging.info("Erro ao Selecionar estacão: ", e)

    #'''
    # obter data de hoje
    #data_atual = date.today()
    #data_formatada = data_atual.strftime('%d/%m/%Y')
    #logging.info(data_formatada)


    time.sleep(5)
    # Gerar CSV
    try:
        button = wait.until(EC.element_to_be_clickable((By.XPATH, '//button[text()="Gerar Tabela"]')))
        button.click()
        time.sleep(3)
    except Exception as e:
        logging.info("Erro em clicar em Gerar Tabela: ", e)

    # baixar o arquivo de dados
    try:
        buttonDownload = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'ui button') and text()='Baixar CSV']")))
        buttonDownload.click()
        time.sleep(5)
        #webdriver.sendKeys(Keys.RETURN)
    except Exception as e:
        logging.info("Erro em clicar em Baixar CSV: ", e)
    finally:
        # close the webdriver
        driver.quit()
        logging.info("Rename_file("+estacao+")")
        Rename_file(estacao)

def move_files_to_destination(files):
    try:
        for file in files:
            file_name = os.path.basename(file)
            destination_path = os.path.join(download_destination, file_name)
            shutil.move(file, destination_path)
            logging.info(f"Moved {file} to {destination_path}")
    except Exception as e:
        logging.info("Error moving files:", e)

def get_files_list(download_path):
    files=[]
    try:
        files = [f for f in os.listdir(download_path) if f.endswith('.csv') and (f.startswith('A') or f.startswith('generated'))] # get all csv files in the directory
        files = [os.path.join(download_path, f) for f in files] # add directory path to each file
        files.sort(key=lambda x: os.stat(x).st_ctime) # sort by creation time
        logging.info(files[-1])
    except Exception as e:
        logging.info("error geting files names:", e)
    #print(files)
    return files

def Rename_file(estacao):
    try: 
        files = get_files_list(download_path)
        file = files[-1] # get most recent file in file
        logging.info("arquivo pra renomear"+ file)
        # rename the downloaded file
        data_atual = datetime.datetime.now()
        data_formatada = data_atual.strftime('%d_%m_%Y_%H_%M_%S')
        new_filename = str(estacao) + "_" + str(data_formatada) + ".csv"
        new_file_path = os.path.join(download_path, new_filename)
        logging.info(new_file_path)
        os.rename(file, new_file_path)
        logging.info("File renamed to: ", new_file_path)
        read_all_csvs(new_file_path)

    except Exception as e:
        logging.info("An error occurred in Rename_file: ", e)
    time.sleep(1)

def read_a_csv(name_file):
    try:
        data = pd.read_csv(name_file, on_bad_lines='skip', header=0, delimiter=';', usecols=['Data', 'Hora (UTC)', 'Chuva (mm)']) #, usecols=['Data', 'Hora (UTC)', 'Chuva (mm)']
        return data
    except Exception as e:
        logging.info("An error reading csv: "+name_file, e)

def read_all_csvs(name_file=""):
    try:
        if name_file == "":
            name_file = get_files_list(download_path)

        if not name_file:
            logging.info("Nenhum arquivo encontrado.")
            return None

        result = read_a_csv(name_file[0])
        if result is not None:
            estacao = str(name_file[0]).replace("C:\\Users\\Usuario\\Documents\\Mares\\update_chuva\\123\\chrome-win64\\", '').split('_')[0]
            result = result.assign(estacao=estacao)
            logging.info('numero de csvs para ler:' + str(len(name_file))) 

        for i in range(1, len(name_file)):
            file = name_file[i]
            data = read_a_csv(file)

            #poe a coluna com a estação
            #print(str(file).replace('C:\\Users\\Usuario\\Documents\\Mares\\update_chuva\\123\\chrome-win64\\', '').split('_'))
            esta = str(file).replace('C:\\Users\\Usuario\\Documents\\Mares\\update_chuva\\123\\chrome-win64\\', '').split('_')[0][-4:]
            data = data.assign(estacao=esta)
            result = pd.concat([result, data], axis=0, ignore_index=True)

            # Remove duplicatas
            result = result.drop_duplicates()

            result.to_csv(download_path + '/merged.csv', sep=';', index=False)

        return result

    except Exception as e:
        logging.info(f"Ocorreu um erro inesperado: {e}")
        return None

def trata_merged(data):
    # Garante que a coluna 'Chuva (mm)' seja tratada como string e substitui ',' por '.'
    chuva_mm_str = data['Chuva (mm)'].astype(str).str.replace(',', '.')
    
    # Converte a coluna para numérico, tratando erros
    chuva_mm_numeric = pd.to_numeric(chuva_mm_str, errors='coerce')

    # Cria uma máscara para filtrar os valores maiores que zero
    mask = chuva_mm_numeric > 0

    # Aplica a máscara ao DataFrame original para filtrar as linhas
    result = data[mask]

    return result

def envia_dados_pro_sheet(data):
    
    # If modifying data, use the appropriate scopes
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = download_path+'keys.json'
    creds = None
    creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    spreadsheet_id = '19rNAWhggTDcDRGfzwYlaz5UYz1XUhH1vkopjL0xWqYc'

    service = build('sheets', 'v4', credentials=creds)
    tuple_data = ''
    try:
        aux = data.values
        tuple_data = [tuple(aux[i]) for i in range(len(aux))]
    except IndexError:
        logging.info("Erro: " + IndexError)

    # Cria range    
    worksheet_name = 'conect!'
    cell_range_insert = 'B2'
    values = tuple(tuple_data)
    value_range_body = {
        'majorDimension': 'ROWS',
        'values': values
    }

    # limpa o intervalo antes de adicionar
    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=worksheet_name + "B2:E",
        body={}
    ).execute()

    # Adiciona dados a planilha
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name + cell_range_insert,
        body=value_range_body
    ).execute()
    

    logging.info("Dados colados no sheet com sucesso!")

# Main script running
if 1: #while 1: 
    
    if 1:
        files = get_files_list(download_path)
        logging.info("move_files_to_destination(files, "+download_destination+")")
        move_files_to_destination(files)

    if 1:
        estacoes_cidades = {
            "A878": "Mostardas",
            "A838": "Camaquã",
            "A887": "Pelotas",
            "A801": "Porto Alegre",
            "A802": "Rio Grande"
        }

        for estacao, cidade in estacoes_cidades.items():
            logging.info(f"Baixando dados para a estação {estacao} em {cidade}...")
            download_estacao(estacao)

    #Trata csvs e envia a sheets
    if 1:
        data = read_all_csvs()
        data = trata_merged(data)
        print(data)
        logging.info("Dados retornados, mandar ao planilhas na sequencia")
        logging.info(data)
        envia_dados_pro_sheet(data)

'''
    for conta in range(360):
        time.sleep(10)
        print("Passou "+str((conta+1)*10)+ " segundos des da ultima execução, próxima execução em "+str( 3600-conta*10-10) +" segundos.")
'''





"""

            if data is not None:
                data['Chuva (mm)'] = data['Chuva (mm)'].fillna(0)
                # Substitui vírgulas por pontos e tenta converter para float
                data['Chuva (mm)'] = data['Chuva (mm)'].astype(str).str.replace(',', '.', regex=False)
                data['Chuva (mm)'] = pd.to_numeric(data['Chuva (mm)'], errors='coerce')

                # Seleciona apenas as linhas onde 'Chuva (mm)' é maior que 0
                #data = data.loc[data['Chuva (mm)'] > 0]

                # Imprime o tipo de dados da coluna 'Chuva (mm)'
                # logging.info("tipo coluna")
                # logging.info(data['Chuva (mm)'].dtypes)

            result['Chuva (mm)'] = result['Chuva (mm)'].astype(str)
            result = result.loc[result['Chuva (mm)'] != '0']
            result = result.loc[result['Chuva (mm)'] != 'nan']
            data['Chuva (mm)'] = data['Chuva (mm)'].astype(str).str.replace('.', ',', regex=False)

    get ids sheets
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheets = sheet_metadata.get('sheets', '')
    for sheet in sheets:
        title = sheet.get("properties", {}).get("title", "Sheet1")
        sheet_id = sheet.get("properties", {}).get("sheetId", 0)
        logging.info(f"{title} ({sheet_id})")
    # create a sample dataframe
    data = pd.DataFrame({
        'Col A': ['Apple','Apple'],
        'Col B': ['Orange','Apple'],
        'Col C': ['Watermelon','Apple'],
        'Col D': ['Banana','Apple']
    })
    aux = data.values
    b = [tuple(aux[i]) for i in range(len(aux))]
    logging.info(tuple(b))

    service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range='Sales North'
    ).execute()


    worksheet_name = 'Sales North!'
    cell_range_insert = 'B2'
    values = (
        ('Col A', 'Col B', 'Col C', 'Col D'),
        ('Apple', 'Orange', 'Watermelon', 'Banana')
    )
    value_range_body = {
        'majorDimension': 'COLUMNS',
        'values': values
    }

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name + cell_range_insert,
        body=value_range_body
    ).execute()


    values.append


    worksheet_name = 'Sales North!'
    cell_range_insert = 'B2'
    values = (
        ('Col E', 'Col F', 'Col G', 'Col H'),
        ('Toyota', 'Honda', 'Tesla', 'BMW')
    )
    value_range_body = {
        'majorDimension': 'COLUMNS',
        'values': values
    }

    service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        valueInputOption='USER_ENTERED',
        range=worksheet_name + cell_range_insert,
        body=value_range_body
    ).execute()


    """




        # Setar data de hoje
        #element = driver.find_element(By.XPATH, '//input[@type="date"]')  
        #driver.execute_script("arguments[0].setAttribute('value', '2023-03-18');", element)