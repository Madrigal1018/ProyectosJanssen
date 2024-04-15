import pandas as pd
import sys
import os
import logging

from os import getenv
from dotenv import load_dotenv

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#FUNCIONES PARA OBTENER DATAFRAMES DE DIFERENTES FUENTES
#Función para obtener el dataframe del ultimo archivo de fonasa descargado
def get_fonasa_ult_12_meses_df() -> pd.DataFrame:
    #Variables generales
    folder_ult_12_meses = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "fonasa_backup", "dbfonasa_ult_descarga.csv")
    month_to_number = {
                        'January': '01', 'Enero': '01',
                        'February': '02', 'Febrero': '02',
                        'March': '03', 'Marzo': '03',
                        'April': '04', 'Abril': '04',
                        'May': '05', 'Mayo': '05',
                        'June': '06', 'Junio': '06',
                        'July': '07', 'Julio': '07',
                        'August': '08', 'Agosto': '08',
                        'September': '09', 'Septiembre': '09',
                        'October': '10', 'Octubre': '10',
                        'November': '11', 'Noviembre': '11',
                        'December': '12', 'Diciembre': '12'
                        }
    
    #Importar el archivo .csv a un dataframe
    fonasa_ult_12_meses_df = pd.read_csv(folder_ult_12_meses,encoding="latin1")
    
    #Modificar los nombres de meses a numeros de mes y generar un columna de fecha en la db de la ultima descarga, eliminando las columnas de mes y año
    fonasa_ult_12_meses_df['Mes'] = fonasa_ult_12_meses_df['Mes'].map(month_to_number)
    fonasa_ult_12_meses_df['Fecha'] = fonasa_ult_12_meses_df['Año'].astype(str) + '-' + fonasa_ult_12_meses_df['Mes'] + '-' + '01'
    fonasa_ult_12_meses_df['Fecha'] = pd.to_datetime(fonasa_ult_12_meses_df['Fecha'],format='%Y-%m-%d')
    fonasa_ult_12_meses_df = fonasa_ult_12_meses_df.drop(['Mes', 'Año'], axis=1)
    
    #Renombrar las columnas en el dataframe y reordenarlas
    fonasa_ult_12_meses_df = fonasa_ult_12_meses_df.rename(columns={'Establecimiento Origen-alias': 'Establecimiento Origen',
                                                                    'Problema de salud-alias': 'Problema de salud',
                                                                    'Región de Origen-alias': 'Región de Origen',
                                                                    'Principio Activo': 'Principio activo',
                                                                    'SUM(Number of Records)-alias': 'Número de registros'
                                                                    })
    fonasa_ult_12_meses_df = fonasa_ult_12_meses_df[['Fecha', 
                                                    'Principio activo', 
                                                    'Problema de salud', 
                                                    'Estado', 
                                                    'Región de Origen', 
                                                    'Establecimiento Origen', 
                                                    'Número de registros'
                                                    ]]
    return fonasa_ult_12_meses_df

#Función para obtener el dataframe del la base de datos maestra de FONASA
def get_fonasa_master_df() -> pd.DataFrame:
    #Variables generales
    folder_master = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "dbfonasa_master.csv")
    
    #Importar el archivo .csv a un dataframe
    fonasa_master_df = pd.read_csv(folder_master,encoding="latin1")
    
    #Modificar el formato de al columna fechas en la base de datos maestro
    fonasa_master_df['Fecha'] = pd.to_datetime(fonasa_master_df['Fecha'], format='%Y-%m-%d')
    return fonasa_master_df

#Función para obtener el dataframe del la base de datos maestra anterior de FONASA.
#Esto se realiza para poder correr el bot varias veces y continuar obteniendo el informe de diferencias correcto.
def get_fonasa_master_anterior_df() -> pd.DataFrame:
    #Variables generales
    folder_master_anterior = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "fonasa_historico")
    files = [file for file in os.listdir(folder_master_anterior) if file.endswith('.csv')]
    files.sort(key=lambda x: os.path.getmtime(os.path.join(folder_master_anterior, x)), reverse=True)
    
    #Identificar el archivo de fonasa master anterior
    fonasa_master_anterior_file = files[1]

    #Importar el archivo .csv a un dataframe
    fonasa_master_anterior_df = pd.read_csv(os.path.join(folder_master_anterior, fonasa_master_anterior_file), encoding="latin1")
    
    #Modificar el formato de al columna fechas en la base de datos maestro
    fonasa_master_anterior_df['Fecha'] = pd.to_datetime(fonasa_master_anterior_df['Fecha'], format='%Y-%m-%d')
    return fonasa_master_anterior_df

#FUNCION PRINCIPAL
def run():
    #ACTUALIZAR LA DB MAESTRA DE FONASA
    logging.info("Actualizando la base de datos maestra de FONASA con la ultima información descargada")
    
    #Variables generales
    folder_master = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "dbfonasa_master.csv")
    
    #Consultando diferentes fuentes de datos
    fonasa_ult_12_meses_df = get_fonasa_ult_12_meses_df()
    fonasa_master_df = get_fonasa_master_df()
    
    #Eliminar de la db maestra todas las fechas duplicadas que estan en la db de los ultimos 12 meses
    min_date = fonasa_ult_12_meses_df['Fecha'].min()
    fonasa_master_actualizada_df = fonasa_master_df[fonasa_master_df['Fecha']< min_date]
    fonasa_master_actualizada_df = pd.concat([fonasa_master_actualizada_df,fonasa_ult_12_meses_df], ignore_index=True)
    
    #Unificar el nombre del principio activo
    fonasa_master_actualizada_df['Principio activo'] = fonasa_master_actualizada_df['Principio activo'].replace('laronidasa', 'Laronidasa')
    
    #Exportar la db maestra actualizada 
    fonasa_master_actualizada_df.to_csv(folder_master, index=False, encoding='latin1')
    
    #Exportar la db maestra actualizada en la carpeta de historicos
    max_date = fonasa_master_actualizada_df['Fecha'].max()
    max_year = max_date.year
    max_month = max_date.month
    folder_historico = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "fonasa_historico", f"dbfonasa_{max_year}{max_month}.csv")
    fonasa_master_actualizada_df.to_csv(folder_historico, index=False, encoding='latin1')

    #REALIZAR EL INFORME DE DIFERENCIAS
    logging.info("Realizando el informe de diferencias retroactivas")
    
    #Variables generales
    folder_diferencias = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "informediferencias.csv")
    principios_activos_inmunologia = ['Abatacept', 'Adalimumab', 'Etanercept', 'Golimumab', 'Infliximab', 'Rituximab', 'Secukinumab', 'Tocilizumab', 'Tofacitinib']
    
    #Consultar la fuente de datos faltante
    fonasa_master_anterior = get_fonasa_master_anterior_df()
    
    #Agrupar los dataframes en una sola linea para poder realizar la comparación, dado que no se cuenta con una columna ID
    fonasa_master_actualizada_df = fonasa_master_actualizada_df.groupby(['Fecha', 'Principio activo', 'Problema de salud', 'Estado', 'Región de Origen', 'Establecimiento Origen'])['Número de registros'].sum().reset_index()
    fonasa_master_anterior = fonasa_master_anterior.groupby(['Fecha', 'Principio activo', 'Problema de salud', 'Estado', 'Región de Origen', 'Establecimiento Origen'])['Número de registros'].sum().reset_index()

    #Realizar un Merge para unificar los dataframes y calcular la diferencia
    informe_diferencias_df = pd.merge(fonasa_master_actualizada_df, fonasa_master_anterior, on =['Fecha', 'Principio activo', 'Problema de salud', 'Estado', 'Región de Origen', 'Establecimiento Origen'], how = 'left')
    informe_diferencias_df = informe_diferencias_df.rename(columns = {'Número de registros_x':'Registros ultimo mes', 'Número de registros_y':'Registros mes anterior'})
    informe_diferencias_df['Diferencia'] = informe_diferencias_df['Registros ultimo mes'] - informe_diferencias_df['Registros mes anterior']

    #Filtrar para que se muestren principios activos de inmunologia y diferencias diferentes de cero y vacios
    informe_diferencias_filt = informe_diferencias_df[informe_diferencias_df['Principio activo'].isin(principios_activos_inmunologia)]
    informe_diferencias_filt = informe_diferencias_filt[(informe_diferencias_filt['Diferencia'] != 0) & informe_diferencias_filt['Diferencia'].notna()]

    #Exportar la db maestra actualizada 
    informe_diferencias_filt.to_csv(folder_diferencias, index=False, encoding='latin1')
    
    #REALIZAR EL SPLIT DE PROBLEMA DE SALUD POR PRINCIPIO ACTIVO
    logging.info("Realizando el split de problema de salud por principio activo")
    
    #Variables generales
    folder_split = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "splitproblemasalud.csv")
    
    # Filtrar el DataFrame para los principios activos de Inmunologia y calcular el split de problema de salud por principio activo
    fonasa_master_inmunologiaa = fonasa_master_actualizada_df[fonasa_master_actualizada_df['Principio activo'].isin(principios_activos_inmunologia)]
    registros_totales_df = fonasa_master_inmunologiaa.groupby(['Principio activo', 'Problema de salud'])['Número de registros'].sum().reset_index()
    registros_principioactivo_df = fonasa_master_inmunologiaa.groupby(['Principio activo'])['Número de registros'].sum().reset_index()
    split_df = pd.merge(registros_totales_df, registros_principioactivo_df, on='Principio activo', suffixes=('', '_total'))
    split_df['Indice'] = split_df['Número de registros']/split_df['Número de registros_total']
    split_df = split_df.drop(['Número de registros', 'Número de registros_total'], axis=1)
    
    #Exportar el archivo de split
    split_df.to_csv(folder_split, index=False, encoding='latin1')
    
    #ACTUALIZAR LA CARPETA DE HISTORICOS
    logging.info("Actualizando la carpeta de historicos")
    
    #Variables generales
    folder_historico_actu = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "fonasa_historico")
    
    #Dejar la carpeta de histico de fonasa solo los 12 archivos más recientes
    files = [os.path.join(folder_historico_actu,files) for files in os.listdir(folder_historico_actu) if os.path.isfile(os.path.join(folder_historico_actu,files))]
    files.sort(key=lambda x: os.path.getmtime(x))
    if len(files) > 12:
        files_to_delete = files[:len(files-12)]
        for file in files_to_delete:
            os.remove(file)
            logging.info(f"Archivo eliminado: {file}")
    else:
        logging.info("No se requiere eliminacion de archivos, hay 12 o menos archivos en la carpeta de historicos")
    
    #Mensaje final
    logging.info("Database FONASA generada con exito")
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)