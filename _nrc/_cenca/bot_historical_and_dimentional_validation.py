import pandas as pd
import numpy as np
import os
import logging
import sys

from os import getenv
from dotenv import load_dotenv

# Load enviroment variables and configure loggin
load_dotenv()
logging.basicConfig(level=logging.INFO)

# Poryect folder variable
PROJECT_FOLDER = getenv('DOWNLOAD_DIRECTORY')

#Establish the CENCA countries in a variables to future use
CENCA_COUNTRIES = ['CRI','DOM','GTM','HND','NIC','PAN','SLV','ABW','BHS','BRB','CUW','HTI','JAM','TTO']
COUNTRY_REPLICEMENTS = {'COS': 'CRI', 'SAL': 'SLV', 'HON': 'HND', 'GUA': 'GTM'}

#Functions
def read_file_excel(*args: str, sheet) -> pd.DataFrame:
    """Reads an Excel file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_excel(folder, sheet_name=sheet)
    return df

def read_file_csv(*args: str) -> pd.DataFrame:
    """Reads a CSV file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_csv(folder, sep=";")
    return df

def relate_fcc_with_product_and_manufacturer() -> pd.DataFrame:
    """ Create a table that relate the fcc code with the product and manufacturer"""
    df_prod = read_file_csv('input', 'dim_product.txt')
    df_man = read_file_csv('input', 'dim_manufacturer.txt')
    df = pd.merge(df_prod, df_man, on = ['COUNTRY_ABV_CD', 'MANUFACTURER_CD'], how = 'left')
    # Rename Columns
    columns_rename = {
        'COUNTRY_ABV_CD': 'Country_ID',
        'PACK_DESC': 'Presentation',
        'PRODUCT_DESC': 'Product',
        'FCC_CD': 'Product_ID',
        'MANUFACTURER_DESC': 'Manufacturer',
        'CORP_DESC': 'Corporation',
        }
    df = df.rename(columns=columns_rename)
    # Filter the table for the cenca countries
    df = df[df['Country_ID'].isin(CENCA_COUNTRIES)]
    # Correct the format of the columns
    df['Product'] = df['Product'].astype(str).str.title()
    df['Manufacturer'] = df['Manufacturer'].astype(str).str.title()
    df['Corporation'] = df['Corporation'].astype(str).str.title()
    df['Presentation'] = df['Presentation'].astype(str).str.title()
    return df

def relate_molecule() -> pd.DataFrame:
    """ Create a table that relate the fcc code with the molecule"""
    df = read_file_csv('input', 'rel_molecule.txt')
    # Rename Columns
    columns_rename = {
        'COUNTRY_ABV_CD': 'Country_ID',
        'FCC_CD': 'Product_ID',
        'MLCL_DESC': 'Molecule',
        }
    df = df.rename(columns=columns_rename)
    # Filter the table for the cenca countries
    df = df[df['Country_ID'].isin(CENCA_COUNTRIES)]
    # Correct the format of the columns
    df['Molecule'] = df['Molecule'].astype(str).str.title()
    # Drop duplicates
    df = df.drop_duplicates(subset=['Country_ID', 'Product_ID'])
    return df.loc[:, ['Country_ID', 'Product_ID', 'Molecule']]

def clean_fact_nrc_farma() -> pd.DataFrame:
    """Clean the file of the previous time"""
    df = read_file_csv('input', 'fact_nrc_pharma.txt')
    # Rename columns
    columns_rename = {
        'PERIOD_CD': 'Date',
        'COUNTRY_ABV_CD': 'Country_ID',
        'CHANNEL_DESC': 'Channel',
        'FCC_CD': 'Product_ID',
        'UNITS_QTY': 'QTY',
        'LIST_VALUES_USD_AMT': 'USD'
        }
    df = df.rename(columns=columns_rename)
    # Filter the dataframe for the CENCA Countries
    df = df[df['Country_ID'].isin(CENCA_COUNTRIES)]
    # Correct the format of USD values
    df['USD'] = df['USD'].str.replace(',', '')
    df['USD'] = df['USD'].astype(float)/100000
    df['USD'] = df['USD'].round(2)
    # Correct channel names
    df['Channel'] = df['Channel'].astype(str).str.title()
    # Identify the product, manufacturer, corporation and presentation
    detail_product_df = relate_fcc_with_product_and_manufacturer()
    df = pd.merge(df, detail_product_df, on = ['Product_ID', 'Country_ID'], how = 'left')
    # Identify the molecule
    molecule = relate_molecule()
    df = pd.merge(df, molecule, on = ['Product_ID', 'Country_ID'], how = 'left')
    # Correct the date format
    df['Date'] = df['Date'].astype(str) + '01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    # Melt the quantity and the usd in one column
    df = pd.melt(df, id_vars=['Country_ID', 'Date', 'Channel','Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation'],
                    value_vars=['QTY', 'USD'], 
                    var_name='Metric', value_name='FF Value')
    # Group by to let just one row per relevant combination 
    df = df.groupby(['Date', 'Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation', 'Country_ID', 'Channel', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation', 'Country_ID', 'Channel', 'Metric', 'FF Value']]

def clean_flexview() -> pd.DataFrame:
    """ Clean the flex view souce."""
    df = read_file_excel('input', 'nrc_flexview.xlsx', sheet='NRC_CEA_M')
    # Rename Columns
    columns_rename = {
        'Period': 'Date',
        'Product Description2' : 'Product',
        'Country Code' : 'Country_ID',
        'Units': 'QTY',
        'Values Usd': 'USD',
        'Manufacturer Description': 'Manufacturer',
        'Corporation Description': 'Corporation',
        'Pack Description': 'Presentation'
    }
    df = df.rename(columns=columns_rename)
    # Replace the country codes to unifed towards the data sets
    df['Country_ID'] = df['Country_ID'].astype(str).replace(COUNTRY_REPLICEMENTS)
    # Correct the format of USD Values
    df['USD'] = df['USD'].round(2)
    # Correct formats
    df['Channel'] = df['Channel'].astype(str).str.title()
    df['Product'] = df['Product'].astype(str).str.title()
    df['Manufacturer'] = df['Manufacturer'].astype(str).str.title()
    df['Corporation'] = df['Corporation'].astype(str).str.title()
    df['Presentation'] = df['Presentation'].astype(str).str.title()
    df['Molecule'] = df['Molecule'].astype(str).str.title()
    # Ignore the last row "Grand Total"
    df = df.iloc[:-1]
    # Correct the date format
    df['Date'] = df['Date'].astype(str).str.replace('-', '').str[:6]
    df['Date'] = df['Date'].astype(str) + '01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    #Melt the quantity and the usd in one column
    df = pd.melt(df, id_vars=['Country_ID', 'Date', 'Channel','Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation'],
                    value_vars=['QTY', 'USD'], 
                    var_name='Metric', value_name='IQVIA Value')
    # Group by to let just one row per relevant combination 
    df = df.groupby(['Date', 'Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation', 'Country_ID', 'Channel', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation', 'Country_ID', 'Channel', 'Metric', 'IQVIA Value']]

def merge_data_sources(fact_nrc_farma_df: pd.DataFrame) -> pd.DataFrame:
    """ Merge the two dataset in one table"""
    df1 = fact_nrc_farma_df
    df2 = clean_flexview()
    df = pd.merge(df1, df2, on=['Date', 'Product', 'Molecule', 'Manufacturer', 'Corporation', 'Presentation', 'Country_ID', 'Channel', 'Metric'], how = 'outer')
    return df

def historical_validation(fact_nrc_farma_df: pd.DataFrame) -> pd.DataFrame:
    """ Calculate the relevant information for the validation"""
    df = merge_data_sources(fact_nrc_farma_df)
    df['Diff Abs'] = np.where(df['FF Value'].notnull() & df['IQVIA Value'].notnull(),
                              (df['FF Value'] - df['IQVIA Value']).abs(),
                              df[['FF Value', 'IQVIA Value']].max(axis=1))
    df['Diff %'] = np.where(df['FF Value'] == 0,
                            -1,
                            np.where(df['FF Value'].notnull() & df['IQVIA Value'].notnull(),
                                     (df['IQVIA Value'] / df['FF Value']) - 1,
                                     -1))
    df['Motivo'] = np.where(df['FF Value'].isna() | df['IQVIA Value'].isna(), 
                            'Un origen no tiene información',
                            np.where(df['Diff Abs'] == 0, 'Sin diferencias', 'Sin identificar'))
    df['Fecha extracción'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    return df

def dimention_analysis(fact_nrc_farma_df: pd.DataFrame, columns: list) -> pd.DataFrame:
    """
    Identify the dimensition that have information in the previos month,
    but dont have information in the current mont
    """
    df = fact_nrc_farma_df
    # Identify the previos dimentions
    df_prev = df[df['Date'] == df['Date'].max() - pd.DateOffset(months=1)].groupby(
                    columns).size().reset_index(
                    name='Prev_Count')
    # Identify the current dimentions
    df_curr = df[df['Date'] == df['Date'].max()].groupby(
                    columns).size().reset_index(
                    name='Curr_Count')
    #Identify previos dimentions that disapear, and dimentions that appear              
    df = pd.merge(df_prev, df_curr, on=columns, how='outer')
    no_report_df = df[df['Curr_Count'].isnull()].copy()
    no_report_df['Status'] = 'No Report'
    new_report_df = df[df['Prev_Count'].isnull()].copy()
    new_report_df['Status'] = 'New Report'
    df = pd.concat([no_report_df,new_report_df],ignore_index=True)
    to_delete_columns = ['Curr_Count', 'Prev_Count']
    df = df.drop(to_delete_columns, axis=1)
    return df

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    clean_fact_nrc_df = clean_fact_nrc_farma()
    hist_validation_df = historical_validation(clean_fact_nrc_df)
    product_analysis = dimention_analysis(clean_fact_nrc_df, ['Product'])
    country_analysis = dimention_analysis(clean_fact_nrc_df, ['Country_ID'])
    product_granularity_analysis = dimention_analysis(clean_fact_nrc_df, ['Country_ID', 'Channel', 'Product'])
    country_granularity_analysis = dimention_analysis(clean_fact_nrc_df, ['Country_ID', 'Channel'])
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "NRC_HISTORICAL_VALIDATION_AND_DIMENTIONAL_ANALYSIS.xlsx")
    with pd.ExcelWriter(output_filepath) as writer:
        hist_validation_df.to_excel(writer, sheet_name='historic_validation', index=False)
        product_analysis.to_excel(writer, sheet_name='product_dimention_analysis', index=False)
        country_analysis.to_excel(writer, sheet_name='country_dimention_analysis', index=False)
        product_granularity_analysis.to_excel(writer, sheet_name='producto_analysis_dim_granular', index=False)
        country_granularity_analysis.to_excel(writer, sheet_name='country_analysis_dim_granular', index=False)
    logging.info('Validación Historica y Analisis dimensional generado con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)
