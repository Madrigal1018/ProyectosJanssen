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

def clean_ff (ff_path: str, relevant_column: str, institution: str, products: pd.DataFrame) -> pd.DataFrame: 
    """ Clean the flat file in one same structure"""
    # Chunksize
    CHUNKSIZE = 1000000
    # Relevant Columns and data types
    COLUMNS = ['ANNUAL', 'MES', 'CLAVE_CByCM', 'INVENTARIO', relevant_column]
    DATA_TYPE = {columna: str if columna in COLUMNS[:3] else float for columna in COLUMNS}
    # Import the file
    file_path = os.path.join(PROJECT_FOLDER, 'input', ff_path)
    chunk_list = []
    for chunk in  pd.read_csv(file_path, delimiter = '|', chunksize=CHUNKSIZE, usecols=COLUMNS, dtype=DATA_TYPE):
        chunk = chunk.groupby(['ANNUAL', 'MES', 'CLAVE_CByCM']).sum().reset_index()
        chunk_list.append(chunk)
    df = pd.concat(chunk_list, ignore_index=True)
    # Rename columns
    columns_rename = {
        'ANNUAL': 'Ano',
        'MES': 'Mes',
        'CLAVE_CByCM': 'Product_ID',
        'INVENTARIO': 'Inventario',
        relevant_column: 'ConsAut' if institution == 'CENSIDA' else (
                         'CPMR' if institution == 'IMSS' else relevant_column
                        )
    }
    df = df.rename(columns=columns_rename)
    # Create date column
    df['Date'] = pd.to_datetime(df['Ano'] + '-' + df['Mes'].str.zfill(2) + '-01')
    # Define  the institution
    df['Institucion'] = institution
    # Filter  the year
    max_year = df['Date'].dt.year.max()
    filter_year = max_year - 4
    df = df[df['Date'].dt.year >= filter_year]
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars =  ['Inventario', 'ConsAut' if institution == 'CENSIDA' else (
                                              'CPMR' if institution == 'IMSS' else relevant_column)],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Merge with the product description
    products = products
    df = pd.merge(df, products, on = ['Product_ID'], how='left')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Institucion', 'Product', 'Metric', 'Value']]

def unified_all_ff(products: pd.DataFrame) -> pd.DataFrame:
    """ Concat all flatfiles for altas"""
    info_list = [
        ('JANSSEN_INEFAM-CONSUMOS_ISSSTE.txt', 'DPN', 'ISSSTE'),
        ('JANSSEN_INEFAM-CONSUMOS_IMSS.txt', 'CPM_R', 'IMSS'),
        ('JANSSEN_INEFAM-CONSUMOS_CENSIDA.txt', 'CONSUMO_AUTORIZADO', 'CENSIDA')
        ]
    df_list = []
    for file, column, category in info_list:
        inst_df = clean_ff(file, column, category, products)
        df_list.append(inst_df)
    df = pd.concat(df_list, axis=0)
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product', 'Metric'])
    return df

def clean_lake2(products: pd.DataFrame) -> pd.DataFrame: 
    """ Clean the lake2 information in one same structure"""
    # Construct the engine and read the sql
    connection_string=f'denodo://{os.getenv("DENODO_USERNAME")}:{os.getenv("DENODO_PASSWORD")}@{os.getenv("DENODO_HOST")}:{os.getenv("DENODO_PORT")}/{os.getenv("DENODO_DABATASE")}'
    engine = create_engine(connection_string)
    df = pd.read_sql(read_sql_file(f"./sql/consumos_lk2.sql"), con=engine)
    # Rename columns
    columns_rename = {
        'date': 'Date',
        'institucion': 'Institucion',
        'product_id': 'Product_ID',
        'lk2_inventario': 'Inventario',
        'lk2_var': 'VAR'
        }
    df = df.rename(columns=columns_rename)
    # Change data types
    df['Date'] = pd.to_datetime(df['Date'])
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Inventario', 'VAR'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Replace Metric values based on 'Institucion'
    institution_mapping = {
        'IMSS': 'DPN',
        'ISSSTE': 'CPMR',
        'CENSIDA': 'ConsAut'
    }
    df['Metric'] = df['Institucion'].map(institution_mapping).fillna(df['Metric'])
    # Merge with the product description
    products = products
    df = pd.merge(df, products, on = ['Product_ID'], how='left')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df.loc[:, ['Date', 'Institucion', 'Product', 'Metric', 'Value']]

def clean_qlik(institution: str, relevant_column: str) -> pd.DataFrame:
    """ Clean the qlik information in one same structure"""
    # Access the file
    file_var = os.path.join(PROJECT_FOLDER, 'output', 'scrapping_consumos', f'dash_qlik_{institution}_var.xlsx')
    file_stock = os.path.join(PROJECT_FOLDER, 'output', 'scrapping_consumos', f'dash_qlik_{institution}_stock.xlsx')
    df_1 = pd.read_excel(file_var)
    df_2 = pd.read_excel(file_stock)
    # Merge the files in one same table
    df = pd.merge(df_1,df_2, on=['Month', 'Molecule'], how= 'outer')
    #Rename column
    columns_rename = {
        'Molecule': 'Product',
        'Stock': 'Inventario',
        relevant_column: 'ConsAut' if institution == 'CENSIDA' else (
                         'CPMR' if institution == 'IMSS' else relevant_column
                        )
        }   
    df = df.rename(columns=columns_rename)
    # Create date column
    df['Date'] = pd.to_datetime(df['Month'])
    # Filter the date
    df = df[df['Date'].dt.year > (max(df['Month'].dt.year)-5)]
    # Define  the institution
    df['Institucion'] = institution
    # Correct the format
    df['Product'] = df['Product'].str.title()
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product'],
                 value_vars =  ['Inventario', 'ConsAut' if institution == 'CENSIDA' else (
                                              'CPMR' if institution == 'IMSS' else relevant_column)],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Institucion', 'Product', 'Metric', 'Value']]

def unified_all_qlik() -> pd.DataFrame:
    """ Concat all qlik files for altas"""
    info_list = [
        ('ISSSTE', 'DPN'),
        ('IMSS', 'CPM R'),
        ('CENSIDA', 'Consumption')
        ]
    df_list = []
    for institution, column in info_list:
        inst_df = clean_qlik(institution, column)
        df_list.append(inst_df)
    df = pd.concat(df_list, axis=0)
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product', 'Metric'])
    return df

def clean_tableau(institution: str, first_column_keyword: str, second_column_keyword: str) -> pd.DataFrame:
    """ Clean Tableau information in one same structure"""
    # Import the file 
    file = os.path.join(PROJECT_FOLDER, "output", "scrapping_consumos", f'dash_tableau_{institution.upper()}.xlsx')
    df = pd.read_excel(file)
    # Repeat the years each 12 months
    years_series = pd.Series([int(year) for year in df.columns.to_numpy() if str(year).isdigit()])
    years_repeated = years_series.repeat(12)
    # Get the stock and variable
    file = os.path.join(PROJECT_FOLDER, "output", "scrapping_consumos", f'dash_tableau_{institution.upper()}.xlsx')
    df = pd.read_excel(file, skiprows=[0, 2])
    var_series = pd.melt(df.filter(regex=first_column_keyword, axis=1), value_name="VAR")["VAR"]
    consumption_series = pd.melt(df.filter(regex=second_column_keyword, axis=1), value_name=f"_{second_column_keyword}")[f"_{second_column_keyword}"]
    stock_series = pd.melt(df.filter(regex='INVENTARIO', axis=1), value_name="_INVENTARIO")["_INVENTARIO"]
    # Repeath the month to match the lenght of years_repeated
    months_cycle = pd.concat([pd.Series(range(1, 13))] * len(years_series), ignore_index=True)
    # Create the dataframe and reorderit
    df = pd.DataFrame({
        "Ano": years_repeated.reset_index(drop=True),
        "Mes": months_cycle,
        f"VAR": var_series,
        "Inventario": stock_series
    })
    # Correct the format
    df['Date'] = pd.to_datetime(df['Ano'].astype(str)\
                                + '-' + df['Mes'].astype(str).str.zfill(2)\
                                + '-01')
    df['Institucion'] = institution
    # Add the product column with a dummy value
    df['Product'] = 'Sin Identificar'
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product'],
                 value_vars = ['VAR', 'Inventario'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product', 'Metric']).sum().reset_index()
    # Replace Metric values based on 'Institucion'
    institution_mapping = {
        'IMSS': 'DPN',
        'ISSSTE': 'CPMR',
        'CENSIDA': 'ConsAut'
    }
    df['Metric'] = df['Institucion'].map(institution_mapping).fillna(df['Metric'])
    return df.loc[:, ["Date", "Institucion", "Product", "Metric", "Value"]]

def unified_all_tableau() -> pd.DataFrame:
    """ Concat all flatfiles for altas"""
    info_list = [
        ('ISSSTE', 'DPN', 'CONSUMO AUTORIZADO'),
        ('IMSS', 'CPM R', 'CANTIDAD ALTA'),
        ('CENSIDA', 'CONSUMO AUTORIZADO', "No. DE PACIENTES")
        ]
    df_list = []
    for file, column_1, column_2 in info_list:
        inst_df = clean_tableau(file, column_1, column_2)
        df_list.append(inst_df)
    df = pd.concat(df_list, axis=0)
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product', 'Metric'])
    return df

def merge_data_sources_to_validate(comparison: str, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    "Make the required validations for the dashboard"
    if comparison == 'Tableau vs FF':
        df2 = df2.drop('Product', axis=1)
        df =  pd.merge(df1, df2.groupby(['Date', 'Institucion', 'Metric']).sum().reset_index(),
                       on = ['Date', 'Institucion', 'Metric'], how = 'outer')
    else:
        df = pd.merge(df1, df2, on = ['Date', 'Institucion', 'Product', 'Metric'], how = 'outer')
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

def add_relevant_columns(df: pd.DataFrame) -> pd.DataFrame:
    """ Add relevant columns to the validation"""
    # Add the columns required
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
    return df

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    #Consultado diferencias totales 
    products = dim_cbycm_product()
    logging.info('Consultando archivo Tableau')
    tableau_df = unified_all_tableau()
    logging.info('Consultando archivo FF')
    ff_df = unified_all_ff(products)
    logging.info('Consultando Lake 2')
    lk2_df = clean_lake2(products)
    logging.info('Consultando Qlik')
    qlik_df = unified_all_qlik()
    logging.info('Generando validación de altas')
    full_df = concat_validations(tableau_df, ff_df, lk2_df, qlik_df)
    final_df = add_relevant_columns(full_df)
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "INEFAM_CONSUMOS_VALIDATION.txt")
    final_df.to_csv(output_filepath, encoding='utf-16', index=False)
    logging.info('Archivo generado con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)