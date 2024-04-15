import pandas as pd
import logging
import os
import sys
from os import getenv
from dotenv import load_dotenv

#CONFIGURACIÓN DEL PROGRAMA
#Cargar variables de entorno con credenciales
load_dotenv()

#Configuración del registro de eventos
logging.basicConfig(level=logging.INFO)

PROJECT_FOLDER = getenv("DOWNLOAD_DIRECTORY")

def consolidate_table() -> pd.DataFrame:
    """ Consolidate the different validations of INEFAM"""
    # Define the folders
    file_altas = os.path.join(PROJECT_FOLDER, "_validations", "output", "INEFAM_ALTAS_VALIDATION.txt")
    file_consumos = os.path.join(PROJECT_FOLDER, "_validations", "output", "INEFAM_CONSUMOS_VALIDATION.txt")
    file_fallos = os.path.join(PROJECT_FOLDER, "_validations", "output", "INEFAM_FALLOS_VALIDATION.txt")    
    # Read files
    altas = pd.read_csv(file_altas, encoding='utf-16')
    consumos = pd.read_csv(file_consumos, encoding='utf-16')
    fallos = pd.read_csv(file_fallos, encoding='utf-16')
    # Define the validations
    altas['Validation'] = 'Altas'
    consumos['Validation'] = 'Consumos'
    fallos['Validation'] = 'Fallos'
    # Concat the files in one file unified
    df = pd.concat([altas, consumos, fallos], ignore_index=True)
    return df

def manage_differences(df: pd.DataFrame) -> pd.DataFrame:
    """ Just left the rows that have differences"""
    df = df[df['Motivo'] != 'Sin diferencias']
    return df
    
def run():
    """ Generate the final excel"""
    logging.info('Generando archivo unificado de INEFAM')
    final_df = consolidate_table()
    logging.info('Generando archivo para gestionar diferencias')
    manage_diff = manage_differences(final_df)
    logging.info('Exportando archivos')
    output_filepath_final = os.path.join(PROJECT_FOLDER, "_dashboard", "output", "INEFAM_UNIFIED_VALIDATION.txt")
    output_filepath_diff = os.path.join(PROJECT_FOLDER, "_dashboard", "output", "INEFAM_MANAGE_DIFF.xlsx")
    final_df.to_csv(output_filepath_final, index=False, encoding='utf-16')
    manage_diff.to_excel(output_filepath_diff, index=False)
    logging.info('Archivos generados con exito')


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.info(e)
        sys.exit(1)