import pandas as pd
import sys
import os
import re
import logging

from os import getenv
from dotenv import load_dotenv

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#FUNCIONES PARA OBTENER DATAFRAMES DE DIFERENTES FUENTES
#Función para obtener el dataframe del archivo de CENABAST switches
def get_cenabast_swit_df() -> pd.DataFrame:
    #Variables generales
    folder_cenabast_dist = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "cenabast", "cenabast_switches.csv")
    cenabast_switches_df= pd.read_csv(folder_cenabast_dist, encoding='UTF-8-SIG', low_memory=False)

    #Filtrar los Nombres Genericos por los principios activos especificos
        # Lista de textos a buscar
    textos_a_buscar = ['Abatacept', 'Adalimumab', 'Etanercept', 'Golimumab', 'Infliximab', 'Rituximab', 'Secukinumab', 'Tocilizumab', 'Tofacitinib']

        # Crear una expresión regular que coincide con alguno de los textos en la lista
    patron = '|'.join(textos_a_buscar)

        # Filtrar el DataFrame utilizando la expresión regular
    cenabast_switches_df = cenabast_switches_df[cenabast_switches_df['NombreGenerico'].str.contains(patron, case=False)]
    
    #Extraer la fecha
    cenabast_switches_df['Fecha'] = cenabast_switches_df['anio'].astype(str) + '-' + cenabast_switches_df['Mes'].astype(str) + '-' + '01'
    cenabast_switches_df['Fecha'] = pd.to_datetime(cenabast_switches_df['Fecha'],format='%Y-%m-%d')

    #Extraer el principio activo en una nueva columna
    cenabast_switches_df['Principio activo'] = cenabast_switches_df['NombreGenerico'].str.extract(f'({patron})', flags=re.IGNORECASE)
    cenabast_switches_df['Principio activo'] = cenabast_switches_df['Principio activo'].str.title()
    
    #Unificar el origen en la base de datos maestra de CENABAST
    cenabast_switches_df['Origen'] = cenabast_switches_df['Origen'].str.lower()
    cenabast_switches_df['Origen'] = cenabast_switches_df['Origen'].str.strip()
    cenabast_switches_df['Origen'] = cenabast_switches_df['Origen'].replace(['traspaso', 'fonasa', 'minsal', '', None], 'otros')
    cenabast_switches_df['Origen'] = cenabast_switches_df['Origen'].str.title()
    
    #Poner todas las columnas que se utilizarán en un merge en minusculas
    cenabast_switches_df['NombreDestinatario'] = cenabast_switches_df['NombreDestinatario'].str.lower()
    cenabast_switches_df['ComunaDestinatario'] = cenabast_switches_df['ComunaDestinatario'].str.lower()
    
    return cenabast_switches_df

#Función para obtener los dataframes de diferentes archivos
def get_inputs_df(folder) -> pd.DataFrame:
    df = pd.read_csv(folder, encoding='latin1', low_memory=False)
    return df

#FUNCIÓN PRINCIPAL
def run():
    #GENERAR BASE DE DATOS CENABAST MAESTRA
    #Variables generales para importar del local
    folder_split = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "splitproblemasalud.csv")
    folder_ratiopacientes = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "dict", "ratiopacientes_dict.csv")
    folder_establecimiento_dict = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "dict", "establecimiento_dict.csv")
    folder_region_dict = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "dict", "region_dict.csv")
    
    #Variable para guardar los outputs generados
    folder_cenabast_master = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "dbcenabast_master.csv")
    
    #Consultando diferentes fuentes de datos
    logging.info('Consultado la base de datos de CENABAST')
    cenabast_master_df = get_cenabast_swit_df()
    logging.info('Consultado los diccionarios de establecimientos y región')
    establecimiento_dict_df = get_inputs_df(folder_establecimiento_dict)
    region_dict_df = get_inputs_df(folder_region_dict)
    logging.info('Consultado el archivo de split de problema de salud por principio activo y ratio')
    split_df = get_inputs_df(folder_split)
    ratiopacientes_df = get_inputs_df(folder_ratiopacientes)
    
    #Utilizar los diccionarios para encontrar los estableciimentos de CENABAST en función de los establecimientos de FONASA
    establecimiento_dict_df = establecimiento_dict_df.set_index('CENABAST')['FONASA'].to_dict()
    cenabast_master_df['Establecimiento destino'] = cenabast_master_df['NombreDestinatario'].map(establecimiento_dict_df)
    
    #Utilizar los diccioinarios para encontrar las regiones de cada una de las comunas
    region_dict_df = region_dict_df.set_index('Comuna')['Region'].to_dict()
    cenabast_master_df['Region'] = cenabast_master_df['ComunaDestinatario'].map(region_dict_df)
        
    #Integrar la base de datos maestra con el split de problema de salud por principio activo y el de ratio de pacientes por problema de salud, principio activo y denominación
    cenabast_master_df = cenabast_master_df.merge(split_df, on=['Principio activo'], how='left')
    cenabast_master_df = cenabast_master_df.merge(ratiopacientes_df, on=['Principio activo', 'Problema de salud', 'Denominacion'], how= 'left')
    
    #Multiplicar las unidades de cenabast por el split 
    cenabast_master_df['Split'] = cenabast_master_df['CantidadDespacho'] * cenabast_master_df['Indice']
    
    #Convertir la unidades dividades en pacientes totales
    cenabast_master_df['Pacientes Totales'] = cenabast_master_df['Split']/cenabast_master_df['Ratio']
    
    # Redondear la columna "Pacientes Totales" a 2 decimales
    cenabast_master_df['Pacientes Totales'] = cenabast_master_df['Pacientes Totales'].round(2)
    
    #GENERAR ESPACIOS EN BLANCO PARA PREGUNTAR AL EQUIPO DE CHILE
    #Variables generales
    folder_establecimiento_chile = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "preguntar_chile", "preguntar_establecimiento.csv")
    folder_ratio_chile = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "preguntar_chile", "preguntar_ratio.csv")
    
    #Dejar los establecimientos por preguntarle al equipo de chile
    definir_chile_establecimientos_df = cenabast_master_df[cenabast_master_df['Establecimiento destino'].isna()]
    definir_chile_establecimientos_df = definir_chile_establecimientos_df[['NombreDestinatario']].drop_duplicates()
    
    #Dejar los ratios de conversión por preguntarle al equipo de chile
    definir_chile_ratio_df = cenabast_master_df[cenabast_master_df['Ratio'].isna()]
    definir_chile_ratio_df = definir_chile_ratio_df[['Denominacion']].drop_duplicates()
    
    #Exportar los archivos
    definir_chile_establecimientos_df.to_csv(folder_establecimiento_chile, index=False, encoding='latin1')
    definir_chile_ratio_df.to_csv(folder_ratio_chile, index=False, encoding='latin1')
    
    #EXPORTAR LA DB DE CENABAST ESTRUCTURADA
    #Estructurar la base de datos
    cenabast_master_df = cenabast_master_df[['Fecha', 'Principio activo', 'Problema de salud', 'Region', 'Establecimiento destino', 'Pacientes Totales', 'Origen']]
    
    #Exportar el archivo maestro de CENABAST
    cenabast_master_df.to_csv(folder_cenabast_master, index=False, encoding='UTF-8-SIG')
    
    #Mensaje final
    logging.info("Database CENABAST generada con exito")
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)

