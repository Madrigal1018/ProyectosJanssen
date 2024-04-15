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
WEB_URL = 'https://navpc-qs.jnj.com/sense/app/f2d87ae4-141d-48ea-ba90-c1e2f236a870/sheet/76f182cc-739d-4587-a810-40365cc230c7/state/0'

#Variables principales 
BASE_TIMEOUT_TIME = 60
BASE_POOL_FREQUENCY=1
exceptions_count = 0
MAX_EXCEPTIONS_COUNT=5
SHEET_NAMES = ['IMSS', 'ISSSTE', 'CENSIDA']
VAR_NAMES = ['var', 'stock']

#Selectores de la pagina web
ALL_CONTENT_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]')
NOINDEX_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[9]/div[1]/div/div[1]/article/div[1]/div/div/div/div/div/ul/li[2]')
MTD_FILTER_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[12]/div[1]/div/div[1]/article/div[1]/div/div/div/div/div/ul/li[3]')
SCALE_FILTER_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[8]/div[1]/div/div[1]/article/div[1]/div/div/qv-filterpane/div/div/div/div[2]/span')
SCALE_OPTION_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div/ng-transclude/div/div[3]/div/article/div[1]/div/div/div/div[2]/div[1]/div/ul/li[1]/div[2]')
SCALE_APPLY_BUTTON_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div/ng-transclude/div/div[2]/div/ul/li[5]/button')

CHART_MOLECULE_SELECTOR = (By.XPATH, '/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[6]')
LINE_CHART_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div/ng-transclude/ul/li[2]')
DOWNLOAD_BUTTON_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div/ng-transclude/ul/li[4]')
DATA_BUTTON_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div/ng-transclude/ul/li[4]')
EXPORT_URL_DATA_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div[2]/div/div[2]/p[2]/a')
EXPORT_BUTTON_MOLECULE_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div[2]/div[3]/button[2]')
CLOSE_BUTTON_SELECTOR = (By.XPATH, '/html/body/div[13]/div/div/div[2]/div/div[3]/button')

#Iniciar navegador
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-extension")
preferences = {"download.default_directory": os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_consumos"), "safebrowsing.enabled":"false"}
chrome_options.add_experimental_option("prefs", preferences)
web_driver = webdriver.Chrome(options=chrome_options) 

#Abrir URL
logging.info("Iniciando Qlik scrapping")
web_driver.get(WEB_URL)

#DESCARGAR TABLA QUE CONTIENE LA INFORMACIÓN
#Filtrar la tabla a No Index
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(NOINDEX_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(NOINDEX_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar filtrar el dashboard, reitento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue

#Filtar a MTD
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(MTD_FILTER_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(MTD_FILTER_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar filtrar el dashboard, reitento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue

#Filtrar a Scala 1
sleep(20)
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_all_elements_located(ALL_CONTENT_SELECTOR))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(SCALE_FILTER_SELECTOR))\
            .click()
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(SCALE_OPTION_SELECTOR))\
            .click()
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(SCALE_APPLY_BUTTON_SELECTOR))\
            .click()
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        
        #Hacer clic en ESC para cerrar el filtro 
        actions = ActionChains(web_driver)
        actions.send_keys(Keys.ESCAPE)
        actions.perform()
        logging.error(f"Un error ocurrio al intentar filtrar el dashboard, reitento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
        continue 
    
    #Descargar la tabla que tiene contiene la información de las diferentes variables
for i in range(1,4):
    #Selector dinamico
    SHEET_SELECTOR =(By.XPATH, f'/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[3]/div[1]/div/div[1]/article/div[1]/div/div/div/div/button[{i}]')
    
    #Abrir el dash de la institución especifica
    while exceptions_count <= MAX_EXCEPTIONS_COUNT:
        try:
            WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                .until(EC.element_to_be_clickable(SHEET_SELECTOR))\
                .click()
            sleep(20)
            break
        except Exception as e:
            exceptions_count += 1
            logging.error(f"Un error ocurrio al intentar filtrar el dashboard, reitento {exceptions_count}")
            if exceptions_count == MAX_EXCEPTIONS_COUNT:
                raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
            continue    

    for x in range(1,3):
        CHART_SELECTOR = (By.XPATH, f'/html/body/div[4]/div[4]/div/div[2]/div/div/div[3]/div/div/div[3]/div[2]/div[11]/div[1]/div/div[1]/article/div[1]/div/div/div/div[5]/div[1]/ul/li[{x}]/span[2]')
        #Seleccionar el dash que contiene la variable relevante
        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
            try:
                WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                    .until(EC.element_to_be_clickable(CHART_SELECTOR))\
                    .click()
                break
            except Exception as e:
                exceptions_count += 1
                logging.error(f"Un error ocurrio al intentar descargar el dashboard, reintento {exceptions_count}")
                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
                continue
            
        while True:
            try:
                #Click derecho sobre la tabla
                element = web_driver.find_element(*CHART_SELECTOR)
                actions = ActionChains(web_driver)
                actions.context_click(element).perform()
                
                #Descargar los datos de la pestaña respectiva
                WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                    .until(EC.element_to_be_clickable(LINE_CHART_SELECTOR))\
                    .click()
                WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
                    .until(EC.element_to_be_clickable(DOWNLOAD_BUTTON_SELECTOR))\
                    .click()
                WebDriverWait(web_driver,BASE_TIMEOUT_TIME)\
                    .until(EC.element_to_be_clickable(DATA_BUTTON_SELECTOR))\
                    .click()
                WebDriverWait(web_driver,BASE_TIMEOUT_TIME)\
                    .until(EC.element_to_be_clickable(EXPORT_URL_DATA_SELECTOR))\
                    .click()
                WebDriverWait(web_driver,BASE_TIMEOUT_TIME)\
                    .until(EC.element_to_be_clickable(CLOSE_BUTTON_SELECTOR))\
                    .click()
                sleep(20)
                exceptions_count = 0
                break
            except Exception as e:
                exceptions_count += 1
                logging.error(f"Un error ocurrio al intentar descargar el dashboard, reintento {exceptions_count}")
                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina {WEB_URL} este funcionando correctamente")
                continue
            
        #CAMBIAR EL NOMBRE AL ARCHIVO DESCARGADO MÁS RECIENTE
        # Obtener la lista de archivos en la carpeta de descargas ordenados por fecha de modificación
        files_in_download_directory = glob.glob(os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_consumos", '*'))
        files_in_download_directory.sort(key=os.path.getmtime)

        # Obtener el archivo más reciente
        latest_downloaded_file = files_in_download_directory[-1]
        
        #Obtener el nombre correspondiente
        var_name = VAR_NAMES[x-1] 
        sheet_name = SHEET_NAMES[i-1]

        # Cambiar el nombre del archivo más reciente a "dash_tableau"
        new_file_path = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_consumos", f"dash_qlik_{sheet_name}_{var_name}.xlsx")
        os.rename(latest_downloaded_file, new_file_path)  
    
    #Informar finalización del scrappin
    logging.info(f"Scraping Qlik INEFAM CONSUMOS {sheet_name}, finalizado con éxito")