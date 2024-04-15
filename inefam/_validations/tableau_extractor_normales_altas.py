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
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException
from os import getenv
from dotenv import load_dotenv


#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#URL Tableau
WEB_URL = 'https://alphadogs.ai/views/04_INEFAM_ALTAS/ANLISISGENERAL/d1eee7f1-1e91-4923-9e8d-76d2b047872b/98add5b8-430a-4dae-bcae-2c4b7d6286ac'

#Variables principales
BASE_TIMEOUT_TIME=20
exceptions_count = 0
MAX_EXCEPTIONS_COUNT=5


#Selectores de la pagina web
LOGIN_INPUT_USER_SELECTOR = (By.CSS_SELECTOR,'input[name="username"]')
LOGIN_INPUT_PASSWORD_SELECTOR = (By.CSS_SELECTOR,'input[name="password"]')
LOGIN_FORM_SELECTOR = (By.CSS_SELECTOR,'form[data-tb-test-id="username-and-password-login-form"]')

DATA_VISUALIZATION_IFRAME_SELECTOR = (By.TAG_NAME, 'iframe')
YEAR_BOX_SELECTOR = (By.ID, 'tableau_base_widget_LegacyCategoricalQuickFilter_1')
YEAR_BOX_ITEMS_SELECTOR = (By.ID, 'tableau_base_widget_LegacyCategoricalQuickFilter_1_menu')
ITEMS_SELECTOR = (By.CLASS_NAME, "FIText")
YEAR_BOX_APPLY_BUTTON_SELECTOR = (By.XPATH, '//*[@id="tableau_base_widget_LegacyCategoricalQuickFilter_1_menu"]/div[3]/button[2]')

DONWLOAD_BUTTON_SELECTOR = (By.ID, 'download')
DOWNLOAD_MENU_TOOLBAR_SELECTOR = (By.ID, 'viz-viewer-toolbar-download-menu')
EXPORT_EXCEL_BUTTON_SELECTOR = (By.CSS_SELECTOR, 'button[data-tb-test-id="export-crosstab-export-Button"]')
CROSSTAB_SHEET_411_SELECTOR = (By.CSS_SELECTOR, '#export-crosstab-options-dialog-Dialog-BodyWrapper-Dialog-Body-Id > div > div:nth-child(1) > div.f1lp596a > div > div > div:nth-child(1)')

#Iniciar navegador
chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument("--start-maximized")
chrome_options.add_argument("--disable-extension")
preferences = {"download.default_directory": os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_altas"), "safebrowsing.enabled":"false"}
chrome_options.add_experimental_option("prefs", preferences)
web_driver = webdriver.Chrome(options=chrome_options)     

#Abrir URL
logging.info("Iniciando Tableau scrapping")
web_driver.get(WEB_URL)

#INICIAR SESIÓN EN LA PAGINA WEB DE INEFAM
#Esperar que la pagina de inicio de sesión este disponible
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_all_elements_located(LOGIN_FORM_SELECTOR))
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar iniciar sesión, reintento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
        continue

#Ingreso de nombre de usuario y contraseña 
logging.info("Autenticando servicio")
WebDriverWait(web_driver,BASE_TIMEOUT_TIME)\
    .until(EC.element_to_be_clickable(LOGIN_INPUT_USER_SELECTOR))\
    .send_keys(getenv("INEFAM_TABLEAU_USERNAME"))
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.element_to_be_clickable(LOGIN_INPUT_PASSWORD_SELECTOR))\
    .send_keys(getenv("INEFAM_TABLEAU_PASSWORD"))
#Envio del formulario de inicio de sesión    
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.element_to_be_clickable(LOGIN_FORM_SELECTOR))\
    .submit()
logging.info("Autenticación satisfactoria")

#ESPERA DE CARGA DE DASHBOARD DE INEFAM ALTAS Y CAMBIO DE IFRAME
logging.info("Cargando INEFAM Altas dashboard")
while exceptions_count <= MAX_EXCEPTIONS_COUNT:
    try:
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.presence_of_element_located(DATA_VISUALIZATION_IFRAME_SELECTOR))
        exceptions_count = 0
        break
    except Exception as e:
        exceptions_count += 1
        logging.error(f"Un error ocurrio al intentar cargar el dashboard de INEFAM, reintento {exceptions_count}")
        if exceptions_count == MAX_EXCEPTIONS_COUNT:
            raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
        continue
web_driver.switch_to.frame(web_driver.find_element(*DATA_VISUALIZATION_IFRAME_SELECTOR))

#SELECCIONAR LOS ULTIMOS 5 AÑOS DEL DASHBOARD
logging.info("Aplicando los filtros respectivos al dashboard")
#Hacer click sobre el filtro de seleccion de años
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.presence_of_element_located(YEAR_BOX_SELECTOR)) \
    .click()

#Esperar a que el selector de años este presente, obtener los textos y crear un diccionario de años
year_selector = WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.presence_of_element_located(YEAR_BOX_ITEMS_SELECTOR))
fitext_texts = [element.get_attribute("title") for element in year_selector.find_elements(*ITEMS_SELECTOR)]
ano_dict = {filter_name: index for index, filter_name in enumerate(fitext_texts[1:], start=0)}

#Obtener los últimos 5 índices y crear una lista de tuplas con índices y valores
last_5_indices = list(ano_dict.values())[-5:]
ano_indices_and_values = [(ano_text, ano_index) for ano_text, ano_index in ano_dict.items()]

#Hacer clic en las últimos 5 años y aceptar:
for ano_text, ano_index in ano_indices_and_values:
    YEAR_BOX_OPTION_SELECTOR = (By.NAME, f'FI_federated.1wy836d1nmrlip123rwg91dek9ml,none:ANNUAL:nk5767673572315659006_6161601966840413189_{ano_index}')
    if ano_index in last_5_indices:
        year_option_selected = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.presence_of_element_located(YEAR_BOX_OPTION_SELECTOR))
        year_option_selected.click()
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.element_to_be_clickable(YEAR_BOX_APPLY_BUTTON_SELECTOR))\
    .click()

#DESCARGAR LOS DATOS NECESARIOS
logging.info("Descargando datos")
while True:
    try:
        #Hacer clic en ESC para cerrar el filtro de los años
        actions = ActionChains(web_driver)
        actions.send_keys(Keys.ESCAPE)
        actions.perform()
        
        #Hacer click en el botón de descarga
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
            .until(EC.element_to_be_clickable(DONWLOAD_BUTTON_SELECTOR)) \
            .click()
        break
    except Exception as e:
        continue

#Esperar a que aparexca el menú de descarga en la barra de eherramientas y darle clic
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.presence_of_element_located(DOWNLOAD_MENU_TOOLBAR_SELECTOR))
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(DOWNLOAD_MENU_TOOLBAR_SELECTOR)) \
    .click()

#Esperar a que aparezca la hoja de datos especifica, verificar si esta seleccionada y seleccionarla si no lo esta
WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.presence_of_element_located(CROSSTAB_SHEET_411_SELECTOR))
crosstab_sheet411 = web_driver.find_element(*CROSSTAB_SHEET_411_SELECTOR)
is_selected = crosstab_sheet411.get_attribute("aria-selected")
bool_value = False if is_selected.lower() == 'false' else True
if not bool_value:
    WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
    .until(EC.element_to_be_clickable(CROSSTAB_SHEET_411_SELECTOR)) \
    .click() 

#Esperar a que aparezca el botón de descarga de excel y darle clic
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.presence_of_element_located(EXPORT_EXCEL_BUTTON_SELECTOR))
WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
    .until(EC.element_to_be_clickable(EXPORT_EXCEL_BUTTON_SELECTOR))\
    .click()
sleep(10) 

#CAMBIAR EL NOMBRE AL ARCHIVO DESCARGADO MÁS RECIENTE
# Obtener la lista de archivos en la carpeta de descargas ordenados por fecha de modificación
files_in_download_directory = glob.glob(os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_altas", '*'))
files_in_download_directory.sort(key=os.path.getmtime)

# Obtener el archivo más reciente
latest_downloaded_file = files_in_download_directory[-1]

# Cambiar el nombre del archivo más reciente a "dash_tableau"
new_file_path = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_altas", "dash_tableau_ALTAS.xlsx")
os.rename(latest_downloaded_file, new_file_path)
logging.info("Scrapping tablue finalizado con éxito")