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
#Función para obtener el dataframe del archivo de CENABAST
def get_cenabast() -> pd.DataFrame:
    #Variables generales
    COLUMNS = ['Año', 'Mes', 'Nombre canal de distribución',
       'Nombre cliente destinatario', 'Nombre producto genérico',  
       'Nombre producto comercial', 'Nombre región', 'Cantidad de pedido']
    CHUNKSIZE = 1000000
    folder_cenabast = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "cenabast", "cenabast.csv")
    cenabast_filt = []
    
    #Importar el archivo por lotes por problemas de memoria
    for chunk in pd.read_csv(folder_cenabast, sep = ';', encoding='utf8', header=0, chunksize=CHUNKSIZE, usecols=COLUMNS):
        #Filtrar por ley recarte soto
        chunk = chunk[chunk['Nombre canal de distribución'] == 'Ley Ricarte Soto']
        #Filtrar por principios activos especificos con un patron de expresión regular
        textos_a_buscar = ['abatacept', 'adalimumab', 'etanercept', 'golimumab', 'infliximab', 'rituximab', 'secukinumab', 'tocilizumab', 'tofacitinib']
        patron = '|'.join(textos_a_buscar)
        # Filtrar el dataframe por la expresión regular y extraer del nombre generico la regexp
        chunk = chunk[chunk['Nombre producto genérico'].str.contains(patron, case=False)]
        chunk['Principio activo'] = chunk['Nombre producto genérico'].str.extract(f'({patron})', flags=re.IGNORECASE)
        chunk['Principio activo'] = chunk['Principio activo'].str.title()
        #Calcular la columna fecha
        chunk['Fecha'] = chunk['Año'].astype(str) + '-' + chunk['Mes'].astype(int).astype(str) + '-' + '01'
        chunk['Fecha'] = pd.to_datetime(chunk['Fecha'], errors='coerce')
        cenabast_filt.append(chunk)
    cenabast_df = pd.concat(cenabast_filt)
    
    #Encontrar los problemas de salud en función de la base de datos de FONASA
    folder_split = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "splitproblemasalud.csv")
    split_df = pd.read_csv(folder_split, encoding='latin1')
    cenabast_df = cenabast_df.merge(split_df, on=['Principio activo'], how='left')
    
    #Encontrar los pacientes en función del ratio de pacientes predefinido
    folder_ratio = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "dict", "ratiopacientes_dict.csv")
    ratio_df = pd.read_csv(folder_ratio, encoding='latin1')
    cenabast_df = cenabast_df.merge(ratio_df, on=['Principio activo', 'Problema de salud', 'Nombre producto comercial'], how= 'left')
    
    #Utilizar un diccionario para encontrar los estableciimentos de CENABAST en función de los establecimientos de FONASA
    folder_esta = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "input", "dict", "establecimiento_dict.csv")
    establecimiento_df = pd.read_csv(folder_esta, encoding='latin1')
    establecimiento_dict = establecimiento_df.set_index('CENABAST')['FONASA'].to_dict()
    cenabast_df['Nombre cliente destinatario'] = cenabast_df['Nombre cliente destinatario'].str.lower()
    cenabast_df['Establecimiento destino'] = cenabast_df['Nombre cliente destinatario'].map(establecimiento_dict)
    
    #Calculo de pacientes activos
    cenabast_df['Split'] = cenabast_df['Cantidad de pedido'] * cenabast_df['Indice']
    cenabast_df['Pacientes Totales'] = cenabast_df['Split']/cenabast_df['Ratio']
    cenabast_df['Pacientes Totales'] = cenabast_df['Pacientes Totales']
    return cenabast_df

#FUNCIÓN PRINCIPAL
def run():
    #GENERAR BASE DE DATOS CENABAST MAESTRA
    cenabast_df = get_cenabast()
    
    #GENERAR ESPACIOS EN BLANCO PARA PREGUNTAR AL EQUIPO DE CHILE
    #Variables generales
    folder_establecimiento_chile = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "preguntar_chile", "preguntar_establecimiento.csv")
    folder_ratio_chile = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "preguntar_chile", "preguntar_ratio.csv")
    folder_cenabast = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "dbcenabast_master.csv")
    
    #Dejar los establecimientos y ratios por preguntarle al equipo de chile
    to_define_est_df = cenabast_df[cenabast_df['Establecimiento destino'].isna()]
    to_define_est_df = to_define_est_df[['Nombre cliente destinatario']].drop_duplicates()
    to_define_ratio_df = cenabast_df[cenabast_df['Ratio'].isna()]
    to_define_ratio_df = to_define_ratio_df.drop_duplicates(subset=['Principio activo', 'Problema de salud', 'Nombre producto comercial'], keep='first')
    
    #Exportar los archivos
    to_define_est_df.to_csv(folder_establecimiento_chile, index=False, encoding='latin1')
    to_define_ratio_df = to_define_ratio_df.loc[:,['Principio activo', 'Problema de salud', 'Nombre producto comercial']]
    to_define_ratio_df.to_csv(folder_ratio_chile, index=False, encoding='latin1')
    cenabast_df = cenabast_df.loc[:, ['Fecha', 'Principio activo', 'Problema de salud', 'Nombre región', 'Establecimiento destino', 'Pacientes Totales']]
    cenabast_df.to_csv(folder_cenabast, index=False, encoding='latin1')
    
    #Mensaje final
    logging.info("Database CENABAST generada con exito")
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)
