import logging
import pandas as pd

from time import sleep
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

import os
from os import getenv
from dotenv import load_dotenv
from tableauscraper import TableauScraper as TS

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#URL Tableau
WEB_URL = 'https://public.tableau.com/shared/CJPX54XFH?%3Adisplay_static_image=y&%3AbootstrapWhenNotified=true&%3Aembed=true&%3Alanguage=en-US&:embed=y&:showVizHome=n&:apiID=host0#navType=1&navSrc=Parse'

#Variables principales
BASE_TIMEOUT_TIME=20
exceptions_count = 0
MAX_EXCEPTIONS_COUNT=10

df_fonasa_month = pd.DataFrame(columns=[
    "Establecimiento Origen-alias",
    "Problema de salud-alias",
    "Región de Origen-alias",
    "Principio Activo",
    "Mes",
    "Año",
    "Estado",
    "SUM(Number of Records)-alias"])

#Selectores de la pagina web
ESTADO_FILTER_SELECTOR = (By.XPATH, '/html/body/div[2]/div[3]/div[1]/div[1]/div/div[2]/div[4]/div/div/div/div/div[11]/div/div/div/div/div')
ESTADO_BOX_ITEMS_SELECTOR = (By.ID, 'tableau_base_widget_LegacyCategoricalQuickFilter_3_menu')
PRACTIVO_FILTER_SELECTOR = (By.XPATH, '/html/body/div[2]/div[3]/div[1]/div[1]/div/div[2]/div[4]/div/div/div/div/div[12]/div/div/div/div/div')
PRACTIVO_BOX_ITEMS_SELECTOR = (By.ID, 'tableau_base_widget_LegacyCategoricalQuickFilter_4_menu')
PERIOD_FILTER_SELECTOR = (By.XPATH, '/html/body/div[2]/div[3]/div[1]/div[1]/div/div[2]/div[4]/div/div/div/div/div[14]/div/div/div/div/div')
PERIOD_BOX_ITEMS_SELECTOR = (By.ID,'tableau_base_widget_LegacyCategoricalQuickFilter_6_menu')
ITEMS_SELECTOR = (By.CLASS_NAME, "FIText")

SHARE_SELECTOR = (By.ID,'share')
LINK_SELECTOR = (By.XPATH, '/html/body/div[8]/div/div/div/div/div[2]/div/div/div[3]/span/span/div/div/div/div[1]/input')
CLOSE_SELECTOR = (By.XPATH, '/html/body/div[8]/div/div/div/div/div[1]/div/div[2]/div/button')

#Funciones definidas
#Iniciar navegador
def initialize_driver():
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument("--start-maximized")
    chrome_options.add_argument("--disable-extension")
    preferences = {"download.default_directory": fr"{getenv("DOWNLOAD_DIRECTORY")}\output", "safebrowsing.enabled":"false"}
    chrome_options.add_experimental_option("prefs", preferences)
    web_driver = webdriver.Chrome(options=chrome_options)
    return web_driver

#Obtener un diccionario de todas las opciones disponibles en los filtros
def get_filter_dict(web_driver,filter_selector,box_items_selector):
    WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
        .until(EC.presence_of_element_located(filter_selector)) \
        .click()
    menu_selector = WebDriverWait(web_driver, BASE_TIMEOUT_TIME) \
        .until(EC.presence_of_element_located(box_items_selector))
    fitext_texts = [element.get_attribute("title") for element in menu_selector.find_elements(*ITEMS_SELECTOR)]
    filter_dict = {filter_name: index for index, filter_name in enumerate(fitext_texts[1:], start=0)}
    close_filter_with_esc(web_driver)
    return filter_dict

#Utilizar la tecla escape
def close_filter_with_esc(web_driver):
    actions = ActionChains(web_driver)
    actions.send_keys(Keys.ESCAPE)
    actions.perform()
    sleep(2)

#Dar click a un elemento
def click_element(element):
    element.click()
    sleep(2)

# Seleccionar un elemento si no está seleccionado
def select_checkbox(checkbox_element):
    if not checkbox_element.is_selected():
        click_element(checkbox_element)
        sleep(2)

#Deseleccionar un elemento
def deselect_checkbox(checkbox_element):
    if checkbox_element.is_selected():
        click_element(checkbox_element)
        sleep(2)

#Iniciar el navegador
web_driver = initialize_driver()

#Abrir URL y cambiar de iframe
logging.info("Iniciando Tableau scrapping")
web_driver.get(WEB_URL)

#Obtenemos los diccionarios relevantes
logging.info("Descargando información de los ultimos 12 meses")
estado_dict = get_filter_dict(web_driver, ESTADO_FILTER_SELECTOR, ESTADO_BOX_ITEMS_SELECTOR)
practivo_dict = get_filter_dict(web_driver,PRACTIVO_FILTER_SELECTOR, PRACTIVO_BOX_ITEMS_SELECTOR)
periodo_dict = get_filter_dict(web_driver, PERIOD_FILTER_SELECTOR, PERIOD_BOX_ITEMS_SELECTOR)
last_12_indices = list(periodo_dict.values())[-12:]
period_indices_and_values = [(period_text, period_index) for period_text, period_index in periodo_dict.items()]

#ITERAR PARA LOS ESTAFOS ACTIVO E INACTIVO, PARA LOS PRINCIPIOS ACTIVOS Y PARA LOS ULTIMOS 12 PERIODOS
for estado_text, estado_index in estado_dict.items():
    for practivo_text, practivo_index in practivo_dict.items():
        for periodo_text, periodo_index in period_indices_and_values:
            if periodo_index in last_12_indices:
                while True:
                    try:
                        #Selectores dinamicos
                        ESTADO_ITEM_SELECTOR_ACTUAL = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+2}]/div[2]/input')
                        ESTADO_ITEM_SELECTOR_ANTERIOR = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+1}]/div[2]/input')
                        PRACIVO_ITEM_SELECTOR_ACTUAL = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+2}]/div[2]/input')
                        PRACIVO_ITEM_SELECTOR_ANTERIOR = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+1}]/div[2]/input')
                        PERIOD_ITEM_SELECTOR_ACTUAL = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+2}]/div[2]/input')
                        PERIOD_ITEM_SELECTOR_ANTERIOR = (By.XPATH, f'/html/body/div[8]/div[2]/div/div/div[{estado_index+1}]/div[2]/input')
                        
                        #Abrir filtro de estado
                        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
                            try:
                                element_to_click = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                    .until(EC.presence_of_element_located(ESTADO_FILTER_SELECTOR))
                                click_element(element_to_click)
                                exceptions_count = 0
                                break
                            except Exception as e:
                                exceptions_count += 1
                                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
                                continue
                        
                        #Seleccionar el elemento del ciclo
                        checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                            .until(EC.presence_of_element_located(ESTADO_ITEM_SELECTOR_ACTUAL))
                        select_checkbox(checkbox_element)
                        
                        #Deseleccionar el elemento anterior y cerrar el filtro
                        if estado_index != 0:
                            checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                .until(EC.presence_of_element_located(ESTADO_ITEM_SELECTOR_ANTERIOR))
                            deselect_checkbox(checkbox_element)
                            close_filter_with_esc(web_driver)
                        else:
                            close_filter_with_esc(web_driver)
                        
                        #Abrir filtro de principio activo
                        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
                            try:
                                element_to_click = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                    .until(EC.presence_of_element_located(PRACTIVO_FILTER_SELECTOR))
                                click_element(element_to_click)
                                exceptions_count = 0
                                break
                            except Exception as e:
                                exceptions_count += 1
                                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
                                continue
                        
                        #Seleccionar el elemento del ciclo
                        checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                            .until(EC.presence_of_element_located(PRACIVO_ITEM_SELECTOR_ACTUAL))
                        select_checkbox(checkbox_element)
                        
                        #Deseleccionar el elemento anterior y cerrar el filtro
                        if practivo_index != 0:
                            checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                .until(EC.presence_of_element_located(PRACIVO_ITEM_SELECTOR_ANTERIOR))
                            deselect_checkbox(checkbox_element)
                            close_filter_with_esc(web_driver)
                        else:
                            close_filter_with_esc(web_driver)
                        
                        #Abrir filtro de periodo
                        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
                            try:
                                element_to_click = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                    .until(EC.presence_of_element_located(PERIOD_FILTER_SELECTOR))
                                click_element(element_to_click)
                                exceptions_count = 0
                                break
                            except Exception as e:
                                exceptions_count += 1
                                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
                                continue
                        
                        #Seleccionar el elemento del ciclo
                        checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                            .until(EC.presence_of_element_located(PERIOD_ITEM_SELECTOR_ACTUAL))
                        select_checkbox(checkbox_element)
                        
                        #Deseleccionar el elemento anterior y cerrar el filtro
                        if periodo_index != 0:
                            checkbox_element = WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                .until(EC.presence_of_element_located(PERIOD_ITEM_SELECTOR_ANTERIOR))
                            deselect_checkbox(checkbox_element)
                            close_filter_with_esc(web_driver)

                        else:
                            close_filter_with_esc(web_driver)
                        
                        #Abrir la pestaña de compatir
                        sleep(2)
                        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
                            try:
                                element_to_click= WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                    .until(EC.element_to_be_clickable(SHARE_SELECTOR))
                                click_element(element_to_click)
                                exceptions_count = 0
                                break
                            except Exception as e:
                                exceptions_count += 1
                                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
                                continue
                            
                        #Copiar link
                        while exceptions_count <= MAX_EXCEPTIONS_COUNT:
                            try:
                                element_to_click= WebDriverWait(web_driver, BASE_TIMEOUT_TIME)\
                                    .until(EC.element_to_be_clickable(LINK_SELECTOR))
                                click_element(element_to_click)
                                link_input = WebDriverWait(web_driver,BASE_TIMEOUT_TIME)\
                                .until(EC.presence_of_element_located(LINK_SELECTOR))
                                break
                            except Exception as e:
                                exceptions_count += 1
                                if exceptions_count == MAX_EXCEPTIONS_COUNT:
                                    raise TimeoutError(f"Tiempo de espera agotado, verifica que la pagina web {WEB_URL} este funcionando correctamente")
                                continue
                        link_value = link_input.get_attribute("value")
                        close_filter_with_esc(web_driver)
                        
                        #Procesar la URL para generar el DataFrame y concatenar al DataFrame final
                        Month, Year = periodo_text.split(" ")
                        Month = Month.capitalize()
                        ts = TS()
                        ts.loads(link_value)
                        ws = ts.getWorksheet("0501 CD Establecimiento")
                        if not ws.data.empty:
                            data = ws.data
                            df = pd.DataFrame(data, columns=df_fonasa_month.columns)
                            df["Principio Activo"] = practivo_text
                            df["Estado"] = estado_text
                            df["Mes"] = Month
                            df["Año"] = Year
                            df = df[(df["Establecimiento Origen-alias"] != "%all%") & (df["Problema de salud-alias"] != "%all%")]
                            df_fonasa_month = pd.concat([df_fonasa_month, df], ignore_index=True)
                        
                        #Reiniciar el navegador en el ultimo ciclo
                        if periodo_index == max(last_12_indices):
                            web_driver.refresh()
                        break
                    except Exception as e:
                        web_driver.refresh()
                        print(e)
                        continue

#GUARDAR EL DATAFRAME EN UN ARCHIVO DE BACKUP
output_directory = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "fonasa_backup")
file_path = os.path.join(output_directory, "dbfonasa_ult_descarga.csv")
df_fonasa_month.to_csv(file_path, index=False, encoding="latin1")
logging.info("Descarga ultimos 12 meses realizada con exito")