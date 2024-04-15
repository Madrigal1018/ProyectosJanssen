import pandas as pd
import numpy as np
import sys
import os
import logging

from os import getenv
from sqlalchemy import create_engine
from dotenv import load_dotenv

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

#Funciones
def read_sql_file(path) -> str:
    '''Recibe un archivo SQL (path) y devuelve su contenido como una cadena'''
    with open(path, 'r') as query_file:
        query = query_file.read()
    return query

def get_lake_df(origin_path: str, dynamic_columns: dict) -> pd.DataFrame:
    '''Limpia el archivo obtenido mediante una Query en SQL. Requiere como 
    parametros; el path del archivo SQL a utilizar y las columnas que varian
    entre lsa diferentes fuentes de información'''
    #Construye la cadena de conexión, crea el motor y lee el contenido del archivo SQL
    connection_string = f'denodo://{os.getenv("DENODO_USERNAME")}:{os.getenv("DENODO_PASSWORD")}@{os.getenv("DENODO_HOST")}:{os.getenv("DENODO_PORT")}/{os.getenv("DENODO_DABATASE")}'
    engine = create_engine(connection_string)
    sql_query = read_sql_file(f"./sql/{origin_path}")
    denodo_df = pd.read_sql(sql_query, con=engine)
    
    # Cambiar tipos de datos a date
    denodo_df['date'] = pd.to_datetime(denodo_df['date'])
    # Cambiar nombre de columnas en función del SQL Query path
    common_columns = {
        'date': 'DATE',
        'max_date': 'MAX_DATE',  
        'fuente':'FUENTE',
        'marca': 'MARCA', 
        'institucion': 'INSTITUCION'
        }
    column_rename = {**common_columns, **dynamic_columns}
    denodo_df = denodo_df.rename(columns=column_rename)
    return denodo_df

def prepare_df(file_name: str, origin: str, columns: dict) -> pd.DataFrame:
    '''Agrupa y normaliza los diferentes dataframes para facilitar su unificación.
    Requiere de el path de sql, el origen de información y las columnas a utilizar'''
    logging.info(f'Realizando la consulta SQL de {origin}')
    df = get_lake_df(file_name, columns)
    df = df.groupby(['DATE', 'MAX_DATE', 'FUENTE', 'MARCA', 'INSTITUCION'], as_index=False).sum(numeric_only=True)
    df = df.melt(id_vars=['DATE', 'MAX_DATE', 'FUENTE', 'MARCA', 'INSTITUCION'], value_vars=columns.values(), var_name='VARIABLE', value_name='TOTAL')
    df['ORIGEN'] = origin
    df['TOTAL'] = df['TOTAL'].round(2)
    return df

def normalized_df()-> pd.DataFrame:
    '''Importa todos los dataframes de las diferentes fuentes de datos y los unifica en un solo dataframe normalizado'''
    data_sources = [
        ('altas_normales.sql', 'ALTAS NORMALES', {'piezas': 'PIEZAS', 'importe': 'IMPORTE', 'precio': 'PRECIO'}),
        ('altas_imss.sql', 'ALTAS IMSS', {'piezas': 'PIEZAS', 'importe': 'IMPORTE', 'precio': 'PRECIO'}),
        ('altas_issste.sql', 'ALTAS ISSSTE', {'piezas': 'PIEZAS', 'importe': 'IMPORTE', 'precio': 'PRECIO'}),
        ('fallos.sql', 'FALLOS', {'piezas': 'PIEZAS', 'importe': 'IMPORTE', 'precio': 'PRECIO'}),
        ('consumos_imss.sql', 'CONSUMOS IMSS', {'inventario': 'INVENTARIO', 'cpm_r': 'CPM R', 'cpm_v': 'CPM V', 'piezas': 'PIEZAS'}),
        ('consumos_issste.sql', 'CONSUMOS ISSSTE', {'inventario': 'INVENTARIO', 'consumo_autorizado': 'CONSUMO AUTORIZADO', 'dpn': 'DPN', 'piezas': 'PIEZAS'}),
        ('consumos_censida.sql', 'CONSUMOS CENSIDA', {'inventario': 'INVENTARIO', 'consumo_autorizado': 'CONSUMO AUTORIZADO', 'numero_pacientes': 'NUMERO PACIENTES', 'piezas': 'PIEZAS'})
    ]
    dfs = [prepare_df(file_name, origin, columns) for file_name, origin, columns in data_sources]    
    full_db = pd.concat(dfs, ignore_index=True)
    full_db = pd.pivot_table(full_db, values=['TOTAL', 'MAX_DATE'],
                            index=['DATE', 'ORIGEN', 'INSTITUCION', 'MARCA', 'VARIABLE'],
                            columns=['FUENTE'], aggfunc={'TOTAL': 'sum', 'MAX_DATE': 'max'}).reset_index()
    full_db.columns = ['DATE', 'ORIGEN', 'INSTITUCION', 'MARCA', 'VARIABLE',
                    'DATE_STANDARD', 'DATE_FREEZE', 'TOTAL_STANDARD', 'TOTAL_FREEZE']
    return full_db

def differences_unified_df(total_validation_df: pd.DataFrame) -> pd.DataFrame:
    '''Importa el dataframe que contiene todos los totales unificados y desnormaliza la información para mostrar
    la diferencia absoluta y la diferencia relativa para las dos variables respectivas. Requiere como 
    parametro el dataframe normalizado para evitar llamarlo dos veces'''
    #Cargar el dataframe a utilizar
    differences_validation_df = total_validation_df
    #Realizar calculos
    differences_validation_df['DIFF ABS'] = (differences_validation_df['TOTAL_STANDARD'] - differences_validation_df['TOTAL_FREEZE']).abs().round(2)
    differences_validation_df['DIFF%'] = ((differences_validation_df['TOTAL_FREEZE'] / differences_validation_df['TOTAL_STANDARD'])-1).round(10)
    differences_validation_df['MOTIVO'] = np.where(differences_validation_df['TOTAL_FREEZE'].isna() | 
                                                    differences_validation_df['TOTAL_STANDARD'].isna(), 
                                                    'Un origen no tiene información', 'Sin identificar')
    differences_validation_df['FECHA IDENTIFICACION'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    differences_validation_df = differences_validation_df.loc[differences_validation_df['DIFF ABS'] != 0,
                                ['DATE', 'ORIGEN', 'INSTITUCION', 'MARCA',
                                'VARIABLE', 'TOTAL_STANDARD', 'TOTAL_FREEZE', 'DIFF ABS', 
                                'DIFF%', 'MOTIVO', 'FECHA IDENTIFICACION']]
    return differences_validation_df

def historical_differences_df(differences_united_df: pd.DataFrame) -> pd.DataFrame:
    '''Obtiene un acumulado de las diferencias historicas de los dataframes'''
    last_differences_df = differences_united_df
    historical_diff_path = os.path.join(getenv("DOWNLOAD_DIRECTORY"), "output", "INEFAM_HISTORICAL_DIFFERENCES.csv")
    # Verificar si el archivo existe
    if os.path.exists(historical_diff_path):
        historical_differences_df = pd.read_csv(historical_diff_path, encoding='latin1')
    else:
        # Crear DataFrame vacío con las columnas requeridas
        columns = ['DATE', 'ORIGEN', 'INSTITUCION', 'MARCA', 'VARIABLE', 'TOTAL_STANDARD',
                   'TOTAL_FREEZE', 'DIFF ABS', 'DIFF%', 'MOTIVO', 'FECHA IDENTIFICACION']
        historical_differences_df = pd.DataFrame(columns=columns)    
    historical_differences_df = pd.concat([last_differences_df, historical_differences_df], ignore_index=True)
    historical_differences_df['DATE'] = pd.to_datetime(historical_differences_df['DATE'])
    cols_to_round = ['TOTAL_STANDARD', 'TOTAL_FREEZE', 'DIFF ABS', 'DIFF%']
    historical_differences_df[cols_to_round] = historical_differences_df[cols_to_round].astype(float).round({'TOTAL_STANDARD': 2, 'TOTAL_FREEZE': 2, 'DIFF ABS': 2, 'DIFF%': 10})
    historical_differences_df = historical_differences_df.drop_duplicates(subset=['DATE', 'VALIDACION', 'INSTITUCION', 'VARIABLE', 'TOTAL_STANDARD', 'TOTAL_FREEZE', 'DIFF ABS', 'DIFF%'])
    return historical_differences_df

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    logging.info('Generando archivo de validación historica')
    database_hist = normalized_df()
    database_hist.to_csv("./output/INEFAM_TOTALS_COMPARISION.csv", index=False, encoding='utf-8-sig')
    
    logging.info('Generando archivo de diferencias historicas')
    differences_df = differences_unified_df(database_hist)
    differences_df.to_csv("./output/INEFAM_DIFFERENCES_COMPARISION.csv", index=False, encoding='utf-8-sig')
    
    logging.info('Generando archivo de historico de diferencias')
    historical_diff_df = historical_differences_df(differences_df)
    historical_diff_df.to_csv("./output/INEFAM_HISTORICAL_DIFFERENCES.csv", index=False, encoding='utf-8-sig')
    logging.info('Archivos generado con exito')

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)