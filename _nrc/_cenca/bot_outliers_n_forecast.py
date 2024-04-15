import pandas as pd
import numpy as np
import os
import logging
import matplotlib.pyplot as plt
import seaborn as sns
import xgboost as xgb
import math
import sys

plt.style.use('fivethirtyeight')

from sklearn.metrics import mean_squared_error
from os import getenv
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

CENCA_COUNTRIES = ['CRI','DOM','GTM','HND','NIC','PAN','SLV','ABW','BHS','BRB','CUW','HTI','JAM','TTO']
PROJECT_FOLDER = getenv('DOWNLOAD_DIRECTORY')

#Functions
def read_file_csv(*args: str) -> pd.DataFrame:
    """Reads a CSV file from a specific folder."""
    folder = os.path.join(PROJECT_FOLDER, *[arg.strip() for arg in ",".join(args).split(',')])
    df = pd.read_csv(folder, sep=";")
    return df

def relate_fcc_with_product() -> pd.DataFrame:
    """ Create a table that relate the fcc code with the product and manufacturer"""
    df = read_file_csv('input', 'dim_product.txt')
    # Rename Columns
    columns_rename = {
        'COUNTRY_ABV_CD': 'Country_ID',
        'PRODUCT_DESC': 'Product',
        'FCC_CD': 'Product_ID',
        }
    df = df.rename(columns=columns_rename)
    # Filter the table for the cenca countries
    df = df[df['Country_ID'].isin(CENCA_COUNTRIES)]
    # Correct the format of the columns
    df['Product'] = df['Product'].astype(str).str.title()
    return df


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
    detail_product_df = relate_fcc_with_product()
    df = pd.merge(df, detail_product_df, on = ['Product_ID', 'Country_ID'], how = 'left')
    # Correct the date format
    df['Date'] = df['Date'].astype(str) + '01'
    df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
    #Melt the quantity and the usd in one column
    df = pd.melt(df, id_vars=['Country_ID', 'Date', 'Channel','Product'],
                    value_vars=['QTY', 'USD'], 
                    var_name='Metric', value_name='Value')
    # Group by to let just one row per relevant combination 
    df = df.groupby(['Date', 'Product', 'Country_ID', 'Channel', 'Metric']).sum().reset_index()
    return df.loc[:, ['Date', 'Product', 'Country_ID', 'Channel', 'Metric', 'Value']]

def filt_fact_nrc_farma(df:pd.DataFrame, metric:str = None, channel:str = None, country:str = None, product:str = None) -> pd.DataFrame:
    """Filt the fact nrc farma table"""
    df = df
    if metric is not None:
        df = df[df['Metric'] == metric]
    if channel is not None:
        df = df[df['Channel'] == channel]
    if country  is not None:
        df = df[df['Country_ID'] == country]
    if product is not None:
        df = df[df['Product'] == product]
    return df

def unique_list_nrc_farma(df:pd.DataFrame, column_unique:str) -> list:
    """Identify a list of unique values for a specific column"""
    df = df
    df = df[column_unique].unique()
    return df

def grouped_ff(df: pd.DataFrame) -> pd.DataFrame:
    """ Group the file an set the date as index"""
    df = df
    df = df.set_index('Date')
    df = df.loc[:, 'Value']
    df = df.resample('MS').sum().to_frame()
    return df

def create_features(df):
    """ Create time series features based on time series index. """
    df = df
    df['quarter'] = df.index.quarter
    df['month'] = df.index.month
    df['year'] = df.index.year
    return df

def list_dataframe_to_forecast(df: pd.DataFrame) -> list:
    """Generate a list of all the combinations dataframes that we should forecast"""
    df_list = []
    clean_df = df
    for metric in ['QTY', 'USD']:
        filt_j1 = filt_fact_nrc_farma(clean_df, metric=metric)
        unique_channels = unique_list_nrc_farma(filt_j1, 'Channel')
        for channel in unique_channels:
            filt_j2 = filt_fact_nrc_farma(filt_j1, metric=metric, channel=channel)
            unique_countries = unique_list_nrc_farma(filt_j2, 'Country_ID')
            for country in unique_countries:
                filt_j3 = filt_fact_nrc_farma(filt_j2, metric=metric, channel=channel, country=country)
                unique_products = unique_list_nrc_farma(filt_j3, 'Product')
                for product in unique_products:
                    filt_j4 = filt_fact_nrc_farma(filt_j3, metric=metric, channel=channel, country=country, product=product)
                    df_list.append(filt_j4)
    return df_list

def list_dataframes_to_forecast_same_date(clean_df: pd.DataFrame, df_list: list) -> list:
    max_date = clean_df['Date'].max()
    clean_list = []
    for df_temp in df_list:
        max_date_df = df_temp['Date'].max()
        if max_date_df != max_date:
            # Create a new row with NaN values for other columns
            new_row = {'Date': max_date}
            for column in df_temp.columns:
                if column != 'Date':
                    new_row[column] = np.nan
            # Append the new row to the DataFrame
            new_row_df = pd.DataFrame(new_row, index=[0])
            df_temp = pd.concat([df_temp, new_row_df], ignore_index=True)
            clean_list.append(df_temp)
        else:
            clean_list.append(df_temp)
    return clean_list

def is_outlier(row):
        if row['Prediction'] > 1.10 * row['Value'] or row['Prediction'] < 0.9 * row['Value']:
                return True
        else:
                return False

def forecast_model_three_months(df_list: list) -> pd.DataFrame:
    df_forecast = []
    for df in df_list:
        # Save the relevant information 
        channel = unique_list_nrc_farma(df,'Channel')[0]
        country = unique_list_nrc_farma(df,'Country_ID')[0]
        product = unique_list_nrc_farma(df,'Product')[0]
        metric = unique_list_nrc_farma(df,'Metric')[0]
        # Create the required format to forecast
        df_current = grouped_ff(df)
        df_current = create_features(df_current)
        # Define the forecast for the last month
        LENGHT_DF = len(df_current)-1
        train_df = df_current.iloc[:LENGHT_DF]
        test_df = df_current.iloc[LENGHT_DF:]
        # Define the correct format for the train and test
        train_df = create_features(train_df)
        test_df = create_features(test_df)
        # Define constants
        FEATURES = ['quarter', 'month', 'year']
        TARGET = 'Value'
        # Variables for the model
        X_train = train_df[FEATURES]
        y_train = train_df[TARGET]
        X_test = test_df[FEATURES]
        y_test = test_df[TARGET]
        # Use the model to forecast
        reg = xgb.XGBRegressor(base_score=0.5, booster='gbtree',    
                            n_estimators=1000,
                            early_stopping_rounds=50,
                            objective='reg:linear',
                            max_depth=3,
                            learning_rate=0.01)
        reg.fit(X_train, y_train,
            eval_set=[(X_train, y_train), (X_test, y_test)],
            verbose=100)
        test_df['Prediction'] = reg.predict(X_test)
        test_df['Prediction'] = test_df['Prediction'].apply(lambda x: math.ceil(x))
        # Merge the prediction
        df_fcst = df_current.merge(test_df[['Prediction']], how='left', left_index=True, right_index=True)  
        
        # Create the required format to forecast
        df_future = grouped_ff(df)
        df_future = create_features(df_future)
        # Define constants
        FEATURES = ['quarter', 'month', 'year']
        TARGET = 'Value'
        # Variables for the model
        X_all = df_future[FEATURES]
        y_all = df_future[TARGET] 
        # Use the model to forecast
        reg = xgb.XGBRegressor(base_score=0.5, booster='gbtree',    
                                n_estimators=1000,
                                early_stopping_rounds=50,
                                objective='reg:linear',
                                max_depth=3,
                                learning_rate=0.01)
        reg.fit(X_all, y_all,
                eval_set=[(X_all, y_all)],
                verbose=100)
        #Future dataframe
        last_date = df_future.index[-1]
        future_dates = pd.date_range(last_date + pd.DateOffset(months=1), last_date + pd.DateOffset(months=3), freq='MS')
        df_future_dates = pd.DataFrame(index=future_dates)
        df_future_dates['isFuture'] = True
        df_future['isFuture'] = False
        df_fsct_future = pd.concat([df_future, df_future_dates])
        # Create features to feed the model
        df_fsct_future = create_features(df_fsct_future)  
        # Make the prediction
        future_w_features = df_fsct_future.query('isFuture').copy()
        future_w_features['Prediction'] = reg.predict(future_w_features[FEATURES])
        # Concat the dataframes
        full_df = pd.concat([df_fcst, future_w_features], axis=0)
        # Establish the format of the dataframe
        full_df['Channel'] = channel
        full_df['Product'] = product
        full_df['Country_ID'] = country
        full_df['Metric'] = metric
        full_df['isOutlier'] = full_df.apply(lambda row: is_outlier(row), axis=1)
        df_forecast.append(full_df)
    final_df = pd.concat(df_forecast, ignore_index=False)
    final_df = final_df.loc[:, ['Metric', 'Channel', 'Country_ID', 'Product', 'Value', 'Prediction', 'isOutlier']]
    return final_df

if __name__ == "__main__":
    try:
        clean_df = clean_fact_nrc_farma()
        df_list = list_dataframe_to_forecast(clean_df)
        df_list_clean = list_dataframes_to_forecast_same_date(clean_df, df_list)
        forecast_df = forecast_model_three_months(df_list_clean)
        output_filepath = os.path.join(PROJECT_FOLDER, "output", "NRC_OUTLIERS_AND_FORECAST.xlsx")
        with pd.ExcelWriter(output_filepath) as writer:
            forecast_df.to_excel(writer, sheet_name='outliers_n_forecast_nrc', index=True)
        logging.info('Validación Outliers & Forecast generado con exito')
    except Exception as e:
        logging.error(e)
        sys.exit(1)