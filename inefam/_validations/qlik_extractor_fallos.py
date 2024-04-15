import os
import glob 
import logging

from time import sleep

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC

from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, StaleElementReferenceException

from os import getenv
from dotenv import load_dotenv

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#URL Qlik
WEB_URL = 'https://navpc-qs.jnj.com/sense/app/f2d87ae4-141d-48ea-ba90-c1e2f236a870/sheet/bed9cd0d-2c5b-49ee-9ff4-15bbcf5625e3/state/0'

#Variables principales 
BASE_TIMEOUT_TIME = 20
BASE_POOL_FREQUENCY=1
exceptions_count = 0
MAX_EXCEPTIONS_COUNT=5

#Selectores de la pagina web
MTD_CHECKBOX_SELECTOR = (By.XPATH, '//*[@id="cd8edafa-fe3d-4bba-a7bb-d29cd66eca3d_content"]/div/div/div/ul/li[3]')
LAST_MONTH_OPTION_SELECTOR = (By.XPATH, '//*[@id="f938d4d8-0c95-4d4d-9291-d514dca69756_content"]/div/div[2]/div[1]/div/ul/li[1]//span[@class="ng-binding ng-scope"]')
MONTH_YEAR_INPUT_SELECTOR = (By.XPATH,'//*[@id="f938d4d8-0c95-4d4d-9291-d514dca69756_title"]/h1/a')
MONTH_YEAR_SEARCH_SELECTOR = (By.CSS_SELECTOR, 'input.lui-search__input')
GRID_SELECTOR = (By.XPATH, '//*[@id="grid"]/div[6]')
FALLOS_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[7]/div[1]/div/div[1]/article/div[1]/div/div/div/div/div/ul/li[1]')
INSTITUTION_SELECTOR = (By.XPATH,'/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[4]/div[1]/div/div[1]/article/div[1]/div/div/div/div/div/div/label[2]/input')
CLAVECBCM_SELECTOR = (By.XPATH,'/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[4]/div[1]/div/div[1]/article/div[1]/div/div/div/div/div/div/label[6]/input')
THREE_DOTS_GRID_OPTION_SELECTOR =  (By.XPATH,'/html/body/div[10]/div/div/div[2]/div[1]')
EXPORT_GROUP_SELECTOR = (By.XPATH,'//*[@id="export-group"]')
EXPORT_SELECTOR = (By.XPATH,'//*[@id="export"]')
EXPORT_DATA_OPTION_SELECTOR = (By.XPATH,'//*[@id="data-export-settings-dialog"]/div[3]/button[2]')
EXPORT_BUTTON_SELECTOR = (By.XPATH,'//*[@id="export-dialog"]/div/div[2]/p[2]/a')

#Iniciar navegador
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-extension")
preferences = {"download.default_directory": os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_fallos"), "safebrowsing.enabled":"false"}
chrome_options.add_experimental_option("prefs", preferences)
web_driver = webdriver.Chrome(options=chrome_options) 

#Abrir URL
logging.info("Iniciando Qlik scrapping")
web_driver.get(WEB_URL)

#FILTRAR UTILIZANDO MTD
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(MTD_CHECKBOX_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(MTD_CHECKBOX_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar seleccionar el MTD, reitento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue

#IDENTIFICAR ULTIMO AÑO DEL DASHBOARD
logging.info("Validando último mes...")
last_month_option = web_driver.find_element(*LAST_MONTH_OPTION_SELECTOR).text
last_year_number = last_month_option[3:]
logging.info(f"Último mes reportado en Qlink {last_month_option}")

#CLASIFICAR LOS ELEMENTOS POR FALLOS
logging.info("Filtrando resultados por Institucion")
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(FALLOS_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.element_to_be_clickable(FALLOS_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue
    
#CLASIFICAR LOS ELEMENTOS POR CLAVE CByCM
logging.info("Filtrando resultados por Subchannel")
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(CLAVECBCM_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.element_to_be_clickable(CLAVECBCM_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue

#CLASIFICAR LOS ELEMENTOS POR INSTITUCION
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(INSTITUTION_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.element_to_be_clickable(INSTITUTION_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue

#EXTRAER TODOS LOS MESES DE LOS ULTIMOS 5 AÑOS
#Dar click sobre el input del filtro
logging.info("Extrayendo información de los ultimos 5 años")
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.presence_of_element_located(MONTH_YEAR_INPUT_SELECTOR))
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(MONTH_YEAR_INPUT_SELECTOR))\
    .click()

#Buscar cada uno de los ultimos 5 años
for x in range(5):
    #Insertar el mes respectivo
    year_to_insert = int(last_year_number) - x
    WebDriverWait(web_driver, BASE_TIMEOUT_TIME, BASE_POOL_FREQUENCY) \
        .until(EC.element_to_be_clickable(MONTH_YEAR_SEARCH_SELECTOR)) \
        .send_keys(f'*{year_to_insert}')
    
    #Aplicar el filtro
    WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
        .until(EC.element_to_be_clickable(MONTH_YEAR_SEARCH_SELECTOR)) \
        .send_keys(Keys.ENTER)
    logging.info(f'{year_to_insert} seleccionado con exito, continuando la selección para el siguiente año')
    sleep(1)
    
#DESCARGAR EL DASHBOARD
#Click derecho sobre la tabla
logging.info("Descargando archivos")
element = web_driver.find_element(*GRID_SELECTOR)
actions = ActionChains(web_driver)
actions.context_click(element).perform()

#Click sobre los tres puntos para descargar
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.presence_of_element_located(THREE_DOTS_GRID_OPTION_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar abrir el menu de descarga, reitento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError("El tiempo de espera se agoto para realizar la descarga de datos desde qlik")
        continue

#Exportar el Excel  
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(EXPORT_GROUP_SELECTOR))\
    .click()
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(EXPORT_SELECTOR))\
    .click()
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(EXPORT_DATA_OPTION_SELECTOR))\
    .click()
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(EXPORT_BUTTON_SELECTOR))\
    .click()
sleep(10)

#CAMBIAR EL NOMBRE AL ARCHIVO DESCARGADO MÁS RECIENTE
# Obtener la lista de archivos en la carpeta de descargas ordenados por fecha de modificación
files_in_download_directory = glob.glob(os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_fallos", '*'))
files_in_download_directory.sort(key=os.path.getmtime)

# Obtener el archivo más reciente
latest_downloaded_file = files_in_download_directory[-1]

# Cambiar el nombre del archivo más reciente a "dash_tableau"
new_file_path = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_fallos", "dash_qlik.xlsx")
os.rename(latest_downloaded_file, new_file_path)
logging.info("Scraping Qlik INEFAM, finalizado con éxito")    
