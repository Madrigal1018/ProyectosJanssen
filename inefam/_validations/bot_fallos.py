import pandas as pd
import numpy as np
import sys
import os
import logging

from os import getenv
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timedelta

# PROGRAM Config
# Load enviromental variables
load_dotenv()

# Config the logging envents
logging.basicConfig(level=logging.INFO)

# Constants 
PROJECT_FOLDER = getenv("DOWNLOAD_DIRECTORY")

#Funciones
def read_sql_file(path) -> str:
    '''Get a SQL (path) and return its content in a string'''
    with open(path, 'r') as query_file:
        query = query_file.read()
    return query

def dim_institution_id() -> pd.DataFrame:
    """ Improt the dimention file"""
    # Relevant columns 
    COLUMNS = ['CLAVE_CLUES', 'INSTITUCION']
    # Import dimention file
    file_path = os.path.join(PROJECT_FOLDER, 'input', 'JANSSEN_CAT_INSTITUCION.txt')
    df = pd.read_csv(file_path, delimiter='|', usecols=COLUMNS)
    # Rename columns
    columns_rename = {
        'CLAVE_CLUES': 'Institucion_ID',
        'INSTITUCION': 'Institucion'
    }
    df = df.rename(columns=columns_rename)
    return df

def clean_ff() -> pd.DataFrame:
    """ Clean the flat file in one same structure"""
    # Relevant columns and data types
    COLUMNS = ['ANNUAL', 'MES', 'CLAVE_CLUES', 'CLAVE_CByCM', 'PIEZAS', 'IMPORTE_MAX']
    DATA_TYPE = {columna: str if columna in COLUMNS[:4] else float for columna in COLUMNS}
    # File pahts
    file_path = os.path.join(PROJECT_FOLDER, 'input', 'JANSSEN_INEFAM-FALLOS.txt')
    # Import file
    df = pd.read_csv(file_path, delimiter='|', usecols=COLUMNS, dtype=DATA_TYPE)
    # Rename Columns
    columns_rename = {
        'ANNUAL': 'Ano',
        'MES': 'Mes',
        'CLAVE_CLUES': 'Institucion_ID',
        'CLAVE_CByCM': 'Product_ID',
        'FCC_CD': 'Product_ID',
        'PIEZAS': 'Units',
        'IMPORTE_MAX': 'MXN'
        }
    df = df.rename(columns=columns_rename)
    # Correct format
    df['Date'] = pd.to_datetime(df['Ano'].astype(str)\
                                + '-' + df['Mes'].astype(str).str.zfill(2)\
                                + '-01')
    # Join with institutions
    institution = dim_institution_id()
    df = pd.merge(df, institution, on=['Institucion_ID'], how='left')
    # FIlter for relevant institutions
    df = df[df['Institucion'].isin(['IMSS', 'ISSSTE', 'CENSIDA'])]
    # Filter the data for relevant years
    max_year = df['Date'].dt.year.max()
    filter_year = max_year - 4
    df = df[df['Date'].dt.year >= filter_year]
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df.loc[:, ['Date', 'Institucion', 'Product_ID', 'Metric', 'Value']]

def clean_lake2() -> pd.DataFrame:
    """ Clean the lake2 information in one same structure"""
    # Construct the engine and read the sql
    connection_string=f'denodo://{os.getenv("DENODO_USERNAME")}:{os.getenv("DENODO_PASSWORD")}@{os.getenv("DENODO_HOST")}:{os.getenv("DENODO_PORT")}/{os.getenv("DENODO_DABATASE")}'
    engine = create_engine(connection_string)
    df = pd.read_sql(read_sql_file(f"./sql/fallos_lk2.sql"), con=engine)
    # Rename columns
    columns_rename = {
        'date': 'Date',
        'institucion': 'Institucion',
        'product_id': 'Product_ID',
        'lk2_units': 'Units',
        'lk2_mxn': 'MXN'
        }
    df = df.rename(columns=columns_rename)
    # Change data types
    df['Date'] = pd.to_datetime(df['Date'])
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df.loc[:, ['Date', 'Institucion', 'Product_ID', 'Metric', 'Value']]

def clean_qlik() -> pd.DataFrame:
    """ Clean the qlik information in one same structure"""
    file_path = os.path.join(PROJECT_FOLDER, 'output', 'scrapping_fallos', 'dash_qlik.xlsx')
    df = pd.read_excel(file_path)
    # Rename columns
    columns_rename = {
        'Institution': 'Institucion',
        'CByCM': 'Product_ID'
        }
    df = df.rename(columns=columns_rename)
    # Change data types
    df['Date'] = pd.to_datetime(df['Date'])
    df['Product_ID'] = df['Product_ID'].str.lstrip('0')
    # Filter for relevant information
    df = df[df['Institucion'].isin(['IMSS', 'ISSSTE', 'CENSIDA'])]
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df.loc[:, ['Date', 'Institucion', 'Product_ID', 'Metric', 'Value']]

def clean_tableau() -> pd.DataFrame:
    """ Clean the tableau file in one same structure"""   
    # Relevant columns and month mapping
    COLUMNS = ['AÑO', 'MES ', 'INSTITUCIÓN', 'CLAVE CNIS_anterior', 'PIEZAS', 'IMPORTE']
    MONTH_MAPPING = {
        'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril': 4,
        'Mayo': 5, 'Junio': 6, 'Julio': 7, 'Agosto': 8,
        'Septiembre': 9, 'Octubre': 10, 'Noviembre': 11, 'Diciembre': 12
        }
    # Import file
    file = os.path.join(PROJECT_FOLDER, "output", "scrapping_fallos", 'dash_tableau.csv')
    df = pd.read_csv(file, encoding='utf16', sep= '\t', usecols=COLUMNS)
    columns_rename = {
        'INSTITUCIÓN':'Institucion',
        'CLAVE CNIS_anterior': 'Product_ID', 
        'PIEZAS': 'Units',
        'IMPORTE' : 'MXN'
        }
    df = df.rename(columns=columns_rename)
    # Correct the formats
    df['MXN'] = df['MXN'].str.replace('[$,]', '', regex=True).astype(float)
    df['Units'] = df['Units'].str.replace(',','').str.replace('.',',').astype(float)
    df['MES '] = df['MES '].str.title().map(MONTH_MAPPING)
    df['Date'] = pd.to_datetime(df['AÑO'].astype(str)\
                                + '-' + df['MES '].astype(str).str.zfill(2)\
                                + '-01')
    # Filter the relevant institutions
    df = df[df['Institucion'].isin(['IMSS', 'ISSSTE', 'CENSIDA'])]
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df.loc[:, ["Date", "Institucion", "Product_ID", "Metric", "Value"]]

def merge_data_sources_to_validate(comparison: str, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    "Make the required validations for the dashboard"
    df = pd.merge(df1, df2, on = ['Date', 'Institucion', 'Product_ID', 'Metric'], how = 'outer')
    df['Comparativa'] = comparison
    return df   

def concat_validations(tableau_df, ff_df, lk2_df, qlik_df) -> pd.DataFrame:
    """ Concat all the validations in one data source"""
    info_list = [
        ('Tableau vs FF', tableau_df, ff_df),
        ('FF vs LK2', ff_df, lk2_df),
        ('LK2 vs Qlik', lk2_df, qlik_df)
    ]
    df_list = []
    df_list = [merge_data_sources_to_validate(comparison, source1, source2) for comparison, source1, source2 in info_list]
    df = pd.concat(df_list, axis=0)
    return df

def dim_cbycm_product() -> pd.DataFrame:
    """ Relate the cbycm with the product"""
    COLUMNS = ['CLAVE_CBYCM', 'PRINC_ACTIVO']
    file = os.path.join(PROJECT_FOLDER, 'input', 'JANSSEN_CAT_PRODUCTO.txt')
    df = pd.read_csv(file, delimiter = '|', usecols=COLUMNS)
    # Rename columns
    columns_rename = {
        'CLAVE_CBYCM': 'Product_ID',
        'PRINC_ACTIVO': 'Product'
    }
    df = df.rename(columns=columns_rename)
    # Correct the format
    df['Product'] = df['Product'].str.title()
    return df

def add_relevant_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add relevant columns to the validation"""
    df['Value_x'] = df['Value_x'].round(2)
    df['Value_y'] = df['Value_y'].round(2)
    df['Diff Abs'] = np.where(df['Value_x'].notnull() & df['Value_y'].notnull(),
                              (df['Value_x'] - df['Value_y']).abs(),
                              df[['Value_x', 'Value_y']].max(axis=1))
    df['Diff %'] = np.where(df['Value_y'] == 0,
                            -1,
                            np.where(df['Value_y'].notnull() & df['Value_x'].notnull(),
                                     (df['Value_x'] / df['Value_y']) - 1,
                                     -1))
    df['Motivo'] = np.where(df['Value_y'].isna() | df['Value_x'].isna(), 
                            'Un origen no tiene información',
                            np.where(df['Diff Abs'] == 0, 'Sin diferencias', 'Sin identificar'))
    df['Fecha extracción'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    # Merge with the product description
    products = dim_cbycm_product()
    df = pd.merge(df, products, on = ['Product_ID'], how='left')
    # Change 'Product' to 'Sin Identificar' if 'Product_ID' is -1
    df['Product'] = np.where(df['Product_ID'] == -1, 'Sin Identificar', df['Product'])
    return df.loc[:, ['Date', 'Institucion', 'Product', 'Metric', 'Value_x', 
                      'Value_y', 'Comparativa', 'Diff Abs', 'Diff %', 'Motivo', 'Fecha extracción']]

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    #Consultado diferencias totales 
    logging.info('Consultando archivo Tableau')
    tableau_df = clean_tableau()
    logging.info('Consultando archivo FF')
    ff_df = clean_ff()
    logging.info('Consultando Lake 2')
    lk2_df = clean_lake2()
    logging.info('Consultando Qlik')
    qlik_df = clean_qlik()
    logging.info('Generando validación de altas')
    full_df = concat_validations(tableau_df, ff_df, lk2_df, qlik_df)
    final_df = add_relevant_columns(full_df)
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "INEFAM_FALLOS_VALIDATION.txt")
    final_df.to_csv(output_filepath, encoding='utf-16', index=False)
    logging.info('Archivo generado con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)