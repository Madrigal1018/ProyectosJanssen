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

def clean_ff (ff_path: str, relevant_column: str, institution: str) -> pd.DataFrame: 
    """ Clean the flat file in one same structure"""
    # Chunksize
    CHUNKSIZE = 1000000
    # Relevant Columns and data types
    COLUMNS = ['ANNUAL', 'MES', 'CLAVE_CByCM', relevant_column, 'IMPORTE']
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
        relevant_column: 'Units',
        'IMPORTE': 'MXN'
        }
    df = df.rename(columns=columns_rename)
    # Create date column
    df['Date'] = pd.to_datetime(df['Ano'] + '-' + df['Mes'].str.zfill(2) + '-01')
    # Define  the institution
    df['Institucion'] = institution
    # Calculate the year to filter the data in function of the institution
    max_year = df['Date'].dt.year.max()
    if institution == 'Normales':
        year_filter = max_year - 4
    else:
        year_filter = max_year - 2
    # Filter the date
    df = df[df['Date'].dt.year >= year_filter]
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Institucion', 'Product_ID', 'Metric', 'Value']]

def unified_all_ff() -> pd.DataFrame:
    """ Concat all flatfiles for altas"""
    info_list = [
        ('JANSSEN_INEFAM-ALTAS.txt', 'PIEZAS', 'Normales'),
        ('JANSSEN_INEFAM-ALTAS IMSS-UNOPS.txt', 'PIEZAS ALTA IMSS', 'IMSS'),
        ('JANSSEN_INEFAM-ALTAS_ISSSTE-UNOPS.txt', 'PIEZAS', 'ISSSTE')
        ]
    df_list = []
    for file, column, category in info_list:
        inst_df = clean_ff(file, column, category)
        df_list.append(inst_df)
    df = pd.concat(df_list, axis=0)
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df

def clean_lake2() -> pd.DataFrame: 
    """ Clean the lake2 information in one same structure"""
    # Construct the engine and read the sql
    connection_string=f'denodo://{os.getenv("DENODO_USERNAME")}:{os.getenv("DENODO_PASSWORD")}@{os.getenv("DENODO_HOST")}:{os.getenv("DENODO_PORT")}/{os.getenv("DENODO_DABATASE")}'
    engine = create_engine(connection_string)
    df = pd.read_sql(read_sql_file(f"./sql/altas_lk2.sql"), con=engine)
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
    file_path = os.path.join(PROJECT_FOLDER, 'output', 'scrapping_altas', 'dash_qlik.xlsx')
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
    filter = ((df["Sub Channel"] == "Altas INEFAM") |
              ((df['Sub Channel'] == 'Altas UNOPS') & (df['Institucion'].isin(['IMSS', 'ISSSTE']))))
    df = df[filter].reset_index(drop=True)
    # Reeplace INEFAM Sub Channel for "Normales"
    df.loc[df['Sub Channel'] == 'Altas INEFAM', 'Institucion'] = 'Normales'
    # Drop irrelevant columns
    df = df.drop('Sub Channel', axis=1)
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
    
def clean_tableau(file_path: str, institution: str) -> pd.DataFrame:
    """ Clean the tableau files in one same structure"""
    # Repeat the years each 12 months
    file = os.path.join(PROJECT_FOLDER, "output", "scrapping_altas", file_path)
    df = pd.read_excel(file)
    years_series = pd.Series([int(year) for year in df.columns.to_numpy() if str(year).isdigit()])
    years_repeated = years_series.repeat(12)
     # Get the units and the MXN
    df = pd.read_excel(file, skiprows=[0, 2])
    if institution in ('IMSS'):
        df = df.iloc[:,4:]
    units_series = pd.melt(df.filter(regex="PIEZAS", axis=1), value_name="Units")["Units"]
    mxn_series = pd.melt(df.filter(regex="IMPORTE", axis=1), value_name="MXN")["MXN"]
    # Repeat each month to match the lenght of years_repeated
    months_cycle = pd.concat([pd.Series(range(1, 13))] * len(years_series), ignore_index=True)
    # Create the final dataframe
    df = pd.DataFrame({
        "Year": years_repeated.reset_index(drop=True),
        "Month": months_cycle,
        "Institucion": institution,
        "Units": units_series,
        "MXN": mxn_series
    })
    # Correct the format 
    df['Date'] = pd.to_datetime(df['Year'].astype(str)\
                                + '-' + df['Month'].astype(str).str.zfill(2)\
                                + '-01') 
    # Add the product column with a dummy value
    df['Product_ID'] = -1
    # Unpivot table
    df = pd.melt(df, id_vars = ['Date', 'Institucion', 'Product_ID'],
                 value_vars = ['Units', 'MXN'],
                 var_name = 'Metric',
                 value_name = 'Value')
    # Group by the dataframe
    df = df.groupby(['Date', 'Institucion', 'Product_ID', 'Metric']).sum().reset_index()
    return df.loc[:, ["Date", "Institucion", "Product_ID", "Metric", "Value"]]

def unified_all_tableau() -> pd.DataFrame:
    """ Concat all flatfiles for altas"""
    info_list = [
        ('dash_tableau_ALTAS.xlsx', 'Normales'),
        ('dash_tableau_IMSS.xlsx', 'IMSS'),
        ('dash_tableau_ISSSTE.xlsx', 'ISSSTE')
        ]
    df_list = []
    for file, category in info_list:
        inst_df = clean_tableau(file, category)
        df_list.append(inst_df)
    df = pd.concat(df_list, axis=0)
    # Sort the dataframe
    df = df.sort_values(by=['Date', 'Institucion', 'Product_ID', 'Metric'])
    return df

def merge_data_sources_to_validate(comparison: str, df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    "Make the required validations for the dashboard"
    # Merge the data sources
    if comparison == 'Tableau vs FF':
        df2 = df2.drop('Product_ID', axis=1)
        df =  pd.merge(df1, df2.groupby(['Date', 'Institucion', 'Metric']).sum().reset_index(),
                       on = ['Date', 'Institucion', 'Metric'], how = 'outer')
    else:
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
    tableau_df = unified_all_tableau()
    logging.info('Consultando archivo FF')
    ff_df = unified_all_ff()
    logging.info('Consultando Lake 2')
    lk2_df = clean_lake2()
    logging.info('Consultando Qlik')
    qlik_df = clean_qlik()
    logging.info('Generando validación de altas')
    full_df = concat_validations(tableau_df, ff_df, lk2_df, qlik_df)
    final_df = add_relevant_columns(full_df)
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "INEFAM_ALTAS_VALIDATION.txt")
    final_df.to_csv(output_filepath, encoding='utf-16', index=False)
    logging.info('Archivo generado con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)