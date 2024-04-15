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

#Lista con diccionarios de URL & Names
URL_AND_NAME = [{'url':'https://alphadogs.ai/views/16_INEFAMINSABIUNOPS/IMSSUNOPS/1267c80b-ad9e-4f15-a7a3-8d27c09be1c2/92e30143-1a71-4d3e-ad75-b9de6185745a',
                 'name':'IMSS',
                 'selector': 2,
                 'item selector': 'FI_federated.1h5zhxc1mztna61biktx30q8bhyj,none:ANNUAL:nk12980795386211298697_14527594746266232979_'},
                {'url':'https://alphadogs.ai/views/16_INEFAMINSABIUNOPS/ISSSTEUNOPS/429e06f4-9088-4da2-9296-72a4f59b339a/99b24d14-0d87-4463-889a-3ec85f301ece',
                 'name':'ISSSTE',
                 'selector': 1,
                 'item selector': 'FI_federated.1fcjrpu1cf049l16pkl1s0nny3e4,none:ANNUAL:nk14977234689283573073_2531225479551319104_'}
                ]

#Variables principales
BASE_TIMEOUT_TIME=20
exceptions_count = 0
MAX_EXCEPTIONS_COUNT=5
LOGGED_IN = False

#Selectores de la pagina web
LOGIN_INPUT_USER_SELECTOR = (By.CSS_SELECTOR,'input[name="username"]')
LOGIN_INPUT_PASSWORD_SELECTOR = (By.CSS_SELECTOR,'input[name="password"]')
LOGIN_FORM_SELECTOR = (By.CSS_SELECTOR,'form[data-tb-test-id="username-and-password-login-form"]')

DATA_VISUALIZATION_IFRAME_SELECTOR = (By.TAG_NAME, 'iframe')
ITEMS_SELECTOR = (By.CLASS_NAME, "FIText")

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

for item in URL_AND_NAME:
    #Selector dinamico
    YEAR_BOX_SELECTOR = (By.ID, f'tableau_base_widget_LegacyCategoricalQuickFilter_{item['selector']}')
    YEAR_BOX_ITEMS_SELECTOR = (By.ID, f'tableau_base_widget_LegacyCategoricalQuickFilter_{item['selector']}_menu')
    YEAR_BOX_APPLY_BUTTON_SELECTOR = (By.XPATH, f'//*[@id="tableau_base_widget_LegacyCategoricalQuickFilter_{item['selector']}_menu"]/div[3]/button[2]')
    
    #Informar al usuario
    logging.info(f'Iniciando Tableau scrapping de {item['name']}')
    web_driver.get(item['url'])
    
    #INICIAR SESIÓN EN LA PAGINA WEB DE INEFAM
    if not LOGGED_IN:
        #Esperar que la pagina de inicio de sesión este disponible
        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
            try:
                WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                    .until(EC.presence_of_all_elements_located(LOGIN_FORM_SELECTOR))
                exceptions_count = 0
                break
            except Exception as e:
                exceptions_count += 1
                logging.error(f'Un error ocurrio al intentar iniciar sesión, reintento {exceptions_count}')
                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                    raise TimeoutError (f'Tiempo de espera agotado, verifica que la pagina web {item['url']} este funcionando correctamente')
                continue
        
        #Ingreso de nombre de usuario y contraseña 
        logging.info("Autenticando servicio")
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(LOGIN_INPUT_USER_SELECTOR))\
            .send_keys(getenv("INEFAM_TABLEAU_USERNAME"))
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(LOGIN_INPUT_PASSWORD_SELECTOR))\
            .send_keys(getenv("INEFAM_TABLEAU_PASSWORD"))
        # Envio del formulario de inicio de sesión    
        WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
            .until(EC.element_to_be_clickable(LOGIN_FORM_SELECTOR))\
            .submit()
        logging.info("Autenticación satisfactoria")
        LOGGED_IN = True 
    
    #ESPERA DE CARGA DE DASHBOARD DE INEFAM ALTAS Y CAMBIO DE IFRAME
    logging.info(f"Cargando INEFAM {item['name']} dashboard")
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
                raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {item['url']} este funcionando correctamente")
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
    ano_dict = {filter_name: index for index, filter_name in enumerate(fitext_texts[:], start=0)}

    #Obtener los últimos 3 índices y crear una lista de tuplas con índices y valores
    last_3_indices = list(ano_dict.values())[-3:]
    ano_indices_and_values = [(ano_text, ano_index) for ano_text, ano_index in ano_dict.items()]
    
    #Hacer clic en las últimos 5 años y aceptar:
    for ano_text, ano_index in ano_indices_and_values:
        #Selector dinamico
        YEAR_BOX_TEMPLATE_OPTION_SELECTOR = (By.NAME, f'{item['item selector']}{ano_index-1}')
        if ano_index in last_3_indices:
            #Seleccionar el año del ciclo respectivo
            year_option_selected = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                .until(EC.presence_of_element_located(YEAR_BOX_TEMPLATE_OPTION_SELECTOR))
            year_option_selected.click()

    #Aplicar los filtros
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

    #Esperar a que aparezca el menú de descarga en la barra de eherramientas y darle clic
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
    files_in_download_directory = glob.glob(os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_altas",'*'))
    files_in_download_directory.sort(key=os.path.getmtime)

    # Obtener el archivo más reciente
    latest_downloaded_file = files_in_download_directory[-1]

    # Cambiar el nombre del archivo más reciente a "dash_tableau"
    new_file_path = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "scrapping_altas",f"dash_tableau_{item['name']}.xlsx")
    os.rename(latest_downloaded_file, new_file_path)
    logging.info(f"Scrapping tablue {item['name']} finalizado con éxito")