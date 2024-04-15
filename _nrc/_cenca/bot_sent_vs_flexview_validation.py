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

# Dictionary mappin country names to country codes
COUNTRY_CODES = {
        'El Salvador': 'SAL',
        'Nicaragua': 'NIC',
        'Panama': 'PAN',
        'Aruba': 'ABW',
        'Guatemala': 'GUA',
        'Curacao': 'CUW',
        'Barbados': 'BRB',
        'Trinidad & Tobago': 'TTO',
        'Haiti': 'HTI',
        'Honduras': 'HON',
        'Jamaica': 'JAM',
        'Costa Rica': 'COS',
        'Dominicana': 'DOM',
        'Bermuda': 'BER',
        'Usa': 'USA',
        'Bahamas': 'BHS',
        'Antigua/Barbuda': 'ATG',
        'Islas Caiman': 'CYM'
    }

# Janssen products list
JANSSEN_PRODUCTS_LIST = [
    'DARZALEX', 'VELCADE', 'ZYTIGA', 'SIMPONI', 'STELARA', 
    'IMBRUVICA', 'DARZALEX FASPRO', 'REMICADE', 'ERLEADA', 
    'RIBOMUSTIN'
    ]

# Darzalex Faspro special cases
DARZALEX_FASPRO_PRESENTATION = [
    "DARZALEX 1X1800MG VIAL SUBC. BNLUX", "DARZALEX 1X1800MG VIAL SUBC. CENCA"
    ]

#Functions
def file_extraction() -> list:
    """Extracts the list of files from a specific folder."""
    folder_sales = os.path.join(PROJECT_FOLDER, "input", "sent_sales")
    files = os.listdir(folder_sales)
    return files

def read_file_csv(*args: str) -> pd.DataFrame:
    """Reads a CSV file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_csv(folder, sep="|")
    return df

def read_file_excel(*args: str, sheet) -> pd.DataFrame:
    """Reads an Excel file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_excel(folder, sheet_name=sheet)
    return df

def unified_sent_sales() -> pd.DataFrame:
    """Unifies the different sales files into one table."""
    df_list = []
    for file in file_extraction():
        df = read_file_csv('input', 'sent_sales', file)
        try:
            df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
        except ValueError:
            df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
        if file == 'VxxxxM05.D24':
            df = df[~((df['Date'].dt.year == 2023) & (df['Date'].dt.month == 5))]
        df_list.append(df)
    df = pd.concat(df_list, ignore_index=True)
    return df

def janssen_products() -> pd.DataFrame:
    """Identifies the relevant products for Janssen from presentation."""
    df = read_file_csv('input', 'historic_sent_products.txt')
    # Identify the Jannsen products from presentation colum
    regex = '|'.join(JANSSEN_PRODUCTS_LIST)
    df['Product'] = df['SKU Description'].str.extract('(' + regex + ')', expand=False)
    df = df.dropna(subset=['Product'], ignore_index=True)
    # Identify the DARZALEX FASPRO products
    condition = df['SKU Description'].isin(DARZALEX_FASPRO_PRESENTATION)
    df['Product'] = df.apply(lambda x: "DARZALEX FASPRO" if condition[x.name] else x['Product'], axis=1)
    # Rename columns
    columns_rename = {
        'Product Internal Code': 'Product_ID'
        }
    df = df.rename(columns=columns_rename)
    # Put the columns in the right format
    df['Product'] = df['Product'].astype(str).str.title()
    df['SKU Description'] = df['SKU Description'].astype(str).str.title()
    df['Product_ID'] = df['Product_ID'].astype(str)
    return df.loc[:,['Product_ID', 'Product']]

def relate_client_and_country() -> pd.DataFrame:
    """Creates a table that relates the client code with the country code."""
    df = read_file_csv('input', 'historic_sent_clients.txt')
    # Put the columns in the right format
    df['Country'] = df['Country'].str.strip().str.title()
    df['Client Internal Code'] = df['Client Internal Code'].astype(str).str[-8:]
    # Correct country names
    df['Country'] = df['Country'].replace(
                    {'Cayman Islands': 'Islas Caiman', 
                    'Dominican Rep.': 'Dominicana',
                    'Trinidad,Tobago' : 'Trinidad & Tobago'})
    # Map the country codes with the country names
    df['Country_ID'] = df['Country'].map(COUNTRY_CODES)
    return df.loc[:, ['Country_ID', 'Client Internal Code']]

def clean_sent_sales(janssen_pdts: pd.DataFrame) -> pd.DataFrame:
    """ Clean the sales sales data. """
    df = unified_sent_sales()
    # Rename the columns
    columns_rename = {
        'Product Internal Code': 'Product_ID',
        'Market Type / Outlet Canal' : 'Channel',
        'Units of SKU' : 'Sent Units'
    }
    df = df.rename(columns=columns_rename)
    # Change data types and correct format
    df['Product_ID'] = df['Product_ID'].astype(str)
    df['Client Internal Code'] = df['Client Internal Code'].astype(str).str[-8:]
    df['Sent Units'] = df['Sent Units'].astype(int)    
    # Correct channel names
    df['Channel'] = df['Channel'].astype(str).str.strip()
    df['Channel'] = df['Channel'].replace(
                                    {'Publico': 'Gobierno'
                                    }) 
    df['Date'] = df['Date'].astype(str).str.replace('-', '').str[:6]
    df['Date'] = df['Date'].astype(str) + '01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    # Identify country codes based on customer codes
    country_df = relate_client_and_country()
    df = pd.merge(df, country_df, on=['Client Internal Code'], how='left')
    # Specify the country name
    df['Country_Name'] = df['Country_ID'].map({v: k for k, v in COUNTRY_CODES.items()})
    # FIlter the dataframe for Janssen Products
    products_df = janssen_pdts 
    products = products_df['Product_ID'].astype(str).tolist()
    df = df[df['Product_ID'].isin(products)]
    df.reset_index(drop=True, inplace=True)
    # Identify Product Name codes based on Products ID
    df = pd.merge(df, products_df, on=['Product_ID'], how='left')
    # Group by to let just one row per relevant combination 
    df = df.groupby(['Date', 'Product', 'Country_ID', 'Country_Name', 'Channel']).sum().reset_index()
    return df.loc[:, ['Date', 'Product', 'Country_ID', 'Country_Name', 'Channel', 'Sent Units']]

def clean_flexview() -> pd.DataFrame:
    """ Clean the flex view souce."""
    df = read_file_excel('input', 'nrc_flexview.xlsx', sheet='NRC_CEA_M')
    # Rename Columns
    columns_rename = {
        'Period': 'Date',
        'Product Description2' : 'Product',
        'Country Code' : 'Country_ID',
        'Units': 'FlexView Units'
    }
    df = df.rename(columns=columns_rename)
    # FIlter for Jansen products
    df = df[df['Manufacturer Description'] == 'JANSSEN']
    # Correct formats
    df['Channel'] = df['Channel'].astype(str).str.title()
    df['Product'] = df['Product'].astype(str).str.title()
    df['Date'] = df['Date'].astype(str).str.replace('-', '').str[:6]
    df['Date'] = df['Date'].astype(str) + '01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    # Specify the country name
    df['Country_Name'] = df['Country_ID'].map({v: k for k, v in COUNTRY_CODES.items()})
    # Ignore the last row "Grand Total"
    df = df.iloc[:-1]
    # Group by to let just one row per relevant combination 
    df = df.groupby(['Date', 'Product', 'Country_ID', 'Country_Name', 'Channel']).sum().reset_index()
    return df.loc[:,['Date', 'Product', 'Country_ID', 'Country_Name', 'Channel', 'FlexView Units']]

def merge_data_sources() -> pd.DataFrame:
    """ Merge the two dataset in one table"""
    df1 = clean_sent_sales(janssen_products())
    df2 = clean_flexview()
    df = pd.merge(df1, df2, on=['Date', 'Product', 'Country_ID', 'Country_Name','Channel'], how = 'outer')
    return df

def final_validation() -> pd.DataFrame:
    """ Calculate the relevant information for the validation"""
    df = merge_data_sources()
    df['Diff Abs'] = np.where(df['Sent Units'].notnull() & df['FlexView Units'].notnull(),
                              (df['Sent Units'] - df['FlexView Units']),
                              df[['Sent Units', 'FlexView Units']].max(axis=1))
    df['Diff %'] = np.where(df['Sent Units'] == 0,
                            -1,
                            np.where(df['Sent Units'].notnull() & df['FlexView Units'].notnull(),
                                     (df['FlexView Units'] / df['Sent Units']) - 1,
                                     -1))
    df['Motivo'] = np.where(df['Sent Units'].isna() | df['FlexView Units'].isna(), 
                            'Un origen no tiene información',
                            np.where(df['Diff Abs'] == 0, 'Sin diferencias', 'Sin identificar'))
    df['Fecha extracción'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
    return df

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    #Consultado diferencias totales 
    final = final_validation()
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "NRC_SENT_VS_FLEXVIEW_VALIDATION.xlsx")
    with pd.ExcelWriter(output_filepath) as writer:
        final.to_excel(writer, sheet_name='sent_vs_flexview_validation', index=False)
    logging.info('Validación Ventas Enviadas vs Ventas FlexView generada con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)

