import pandas as pd
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

#Functions
def read_file_excel(*args: str, sheet, cols=None) -> pd.DataFrame:
    """Reads an Excel file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_excel(folder, sheet_name=sheet, usecols=cols)
    return df

def read_file_csv(*args: str) -> pd.DataFrame:
    """Reads a CSV file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_csv(folder, sep=";")
    return df

def title_after_dash(text):
    """ Title format just for the ATC Desciption, not the code"""
    parts = text.split('-', 1) 
    if len(parts) == 2:  
        return parts[0] + '-' + parts[1].title() 
    else:
        return text

def clean_jnj_hs_cenca() -> pd.DataFrame:
    """ Clean the jnj hs cenca file."""
    df = read_file_excel('input', 'jnj_hs_cenca.xlsx', sheet='HS JnJ CenCa')
    #Rename columns
    columns_rename = {
        'OVERALL TA ': 'Overall TA',
        'HS/OTHER': 'Category'
        }
    df = df.rename(columns=columns_rename)
    # Correct the format
    df['Overall TA'] = df['Overall TA'].astype(str).str.title()
    df['Category'] = df['Category'].astype(str).str.title()
    df['ATC IV'] = df['ATC IV'].astype(str).apply(title_after_dash)
    return df.loc[:,['ATC IV', 'Overall TA', 'Category']]

def clean_rpt_di_cea_janssen() -> pd.DataFrame:
    """ Clean the rpt di cea janssen file."""
    df = read_file_excel('input', 'rpt_di_cea_janssen.xlsx', sheet='DATA', cols=['PERIOD', 'PACK_DESC', 'MOLECULE', 'ATC4'])
    #Rename columns and select the relevant column
    columns_rename = {
        'ATC4': 'ATC IV', 
        'MOLECULE': 'Molecule',
        'PERIOD': 'Date',
        'PACK_DESC': 'Presentation'
        }
    df = df.rename(columns=columns_rename)
    df = df.loc[:, ['Date', 'Molecule', 'ATC IV', 'Presentation']]
    # Correct the format
    df['Date'] = df['Date'].astype(str) + '-01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y-%m-%d')
    df['Molecule'] = df['Molecule'].astype(str).str.title()
    df['Presentation'] = df['Presentation'].astype(str).str.title()
    df['ATC IV'] = df['ATC IV'].astype(str).apply(title_after_dash)
    return df

def clean_flexview() -> pd.DataFrame:
    """ Clean the flex view souce."""
    df = read_file_excel('input', 'nrc_flexview.xlsx', sheet='NRC_CEA_M')
    #Rename columns and select the relevant column
    columns_rename = {
        'Atc 4': 'ATC IV',
        'Period': 'Date',
        'Product Description2': 'Product',
        'Product Description': 'Product v2',
        'Pack Description': 'Presentation'
        }
    df = df.rename(columns=columns_rename)
    df = df.loc[:, ['Date', 'Product', 'Product v2','Molecule', 'Presentation', 'ATC IV']]
    # Ignore the last row "Grand Total"
    df = df.iloc[:-1]
    # Correct the format
    df['Molecule'] = df['Molecule'].astype(str).str.strip().str.title()
    df['Product'] = df['Product'].astype(str).str.strip().str.title()
    df['Product v2'] = df['Product v2'].astype(str).str.strip().str.title()
    df['Presentation'] = df['Presentation'].astype(str).str.title()
    df['ATC IV'] = df['ATC IV'].astype(str).apply(title_after_dash)
    return df

def atc_iv_validation_market_vs_nrc(flex_df: pd.DataFrame) -> pd.DataFrame:
    """ Make the validation of act iv between market and nrc"""
    # Import the data sources
    jnj_hs_df = clean_jnj_hs_cenca()
    flexview_df = flex_df['ATC IV']
    # Merge the data sources
    df = pd.merge(jnj_hs_df, flexview_df, on=['ATC IV'], how='outer', indicator=True)
    # Replace the values to clarify to the final user
    merge_mapping = {
        'both': 'En ambas fuentes de datos',
        'left_only': 'En mercado, pero no en NRC',
        'right_only': 'En NRC, pero no en mercado'
    }
    df['_merge'] = df['_merge'].map(merge_mapping)
    df = df.rename(columns={'_merge': 'Status'})
    # Drop duplicates
    df = df.drop_duplicates(subset=['ATC IV'], ignore_index=True)
    return df

def atc_iv_validation_dim_vs_nrc(flex_df: pd.DataFrame, dimentional_df: pd.DataFrame) -> pd.DataFrame:
    """ Make the validation of act iv between dim and nrc"""
    # Import the data sources
    rpt_di_cea_janssen = dimentional_df.loc[:,['ATC IV', 'Presentation']]
    flexview_df = flex_df.loc[:,['ATC IV']]
    # Merge the data sources
    df = pd.merge(rpt_di_cea_janssen, flexview_df, on=['ATC IV'], how='outer', indicator=True)
    merge_mapping = {
        'both': 'En ambas fuentes de datos',
        'left_only': 'En DIM, pero no en NRC',
        'right_only': 'En NRC, pero no en DIM'
    }
    df['_merge'] = df['_merge'].map(merge_mapping)
    df = df.rename(columns={'_merge': 'Status'})
    # Delete duplicates
    df = df.drop_duplicates(subset=['ATC IV', 'Presentation'], ignore_index=True)
    return df

def double_molecules_by_product(flex_df: pd.DataFrame) -> pd.DataFrame:
    """ Check if there exists cases when one product description contains more than one molecule"""
    df = flex_df.loc[:,['Product v2', 'Molecule']]
    molecule_count = df.groupby('Product v2')['Molecule'].nunique()
    df['Status'] = 'Okay' 
    df.loc[df['Product v2'].isin(molecule_count[molecule_count > 1].index), 'Status'] = 'Más de 1 molecula asociada'
    return df

def mol_validation_dim_vs_nrc(flex_df: pd.DataFrame, dimentional_df: pd.DataFrame) -> pd.DataFrame:
    rpt_di_cea_janssen = dimentional_df['Molecule']
    flexview_df = flex_df.loc[:,['Molecule', 'Product', 'Presentation', 'ATC IV']]
    # Merge the data sources
    df = pd.merge(rpt_di_cea_janssen, flexview_df, on=['Molecule'], how='outer', indicator=True)
    merge_mapping = {
        'both': 'En ambas fuentes de datos',
        'left_only': 'En DIM, pero no en NRC',
        'right_only': 'En NRC, pero no en DIM'
    }
    df['_merge'] = df['_merge'].map(merge_mapping)
    df['_merge']
    df = df.rename(columns={'_merge': 'Status'})
    # Delete duplicates
    df = df.drop_duplicates(subset=['Molecule', 'Product', 'Presentation', 'ATC IV'], ignore_index=True)
    return df

def run():
    '''Generar el archivo excel final para utilizar en el dashboard'''
    clean_flex_df = clean_flexview()
    clean_dim_df = clean_rpt_di_cea_janssen()
    atciv_market_vs_flex_df = atc_iv_validation_market_vs_nrc(clean_flex_df)
    atciv_dim_vs_nrc_df = atc_iv_validation_dim_vs_nrc(clean_flex_df, clean_dim_df)
    double_molecules_df = double_molecules_by_product(clean_flex_df)
    mol_dim_vs_nrc_df = mol_validation_dim_vs_nrc(clean_flex_df, clean_dim_df)
    output_filepath = os.path.join(PROJECT_FOLDER, "output", "NRC_ATCIV_AND_MOL_VALIDATION.xlsx")
    with pd.ExcelWriter(output_filepath) as writer:
        atciv_market_vs_flex_df.to_excel(writer, sheet_name='atciv_market_vs_flex', index=False)
        atciv_dim_vs_nrc_df.to_excel(writer, sheet_name='atciv_dim_vs_nrc', index=False)
        double_molecules_df.to_excel(writer, sheet_name='double_molecules', index=False)
        mol_dim_vs_nrc_df.to_excel(writer, sheet_name='mol_dim_vs_nrc', index=False)
    logging.info('Validación ATC IV y Molecula generado con exito')
    
if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)