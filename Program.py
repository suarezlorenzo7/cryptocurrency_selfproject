"""These are functions to be used exclusively in the dag."""

import pandas as pd
from src.fetch_and_load import fetch_and_load
from src.generate_data_frame import generate_dataframe
from utils.get_keys import get_keys

def fetch_data():
    api_key = get_keys("binance", "api")
    signature_key = get_keys("binance", "signature")
    return generate_dataframe(api_key, signature_key)

def process_data(dataframe):
    datetime_col = pd.to_datetime(dataframe['datetime'].iloc[0]).isoformat()
    btcusdt_rate = float(dataframe['BTCUSDT_rate'].iloc[0])
    ethusdt_rate = float(dataframe['ETHUSDT_rate'].iloc[0])
    solusdt_rate = float(dataframe['SOLUSDT_rate'].iloc[0])
    btcbrl_rate = float(dataframe['BTCBRL_rate'].iloc[0])
    ethbrl_rate = float(dataframe['ETHBRL_rate'].iloc[0])
    solbrl_rate = float(dataframe['SOLBRL_rate'].iloc[0])
    btcusdt_total = float(dataframe['BTCUSDT_total'].iloc[0])
    ethusdt_total = float(dataframe['ETHUSDT_total'].iloc[0])
    solusdt_total = float(dataframe['SOLUSDT_total'].iloc[0])
    btcbrl_total = float(dataframe['BTCBRL_total'].iloc[0])
    ethbrl_total = float(dataframe['ETHBRL_total'].iloc[0])
    solbrl_total = float(dataframe['SOLBRL_total'].iloc[0])
    usdt_total = float(dataframe['usdt_total'].iloc[0])
    brl_total = float(dataframe['brl_total'].iloc[0])
    
    return {
        'datetime_col': datetime_col,
        'btcusdt_rate': btcusdt_rate,
        'ethusdt_rate': ethusdt_rate,
        'solusdt_rate': solusdt_rate,
        'btcbrl_rate': btcbrl_rate,
        'ethbrl_rate': ethbrl_rate,
        'solbrl_rate': solbrl_rate,
        'btcusdt_total': btcusdt_total,
        'ethusdt_total': ethusdt_total,
        'solusdt_total': solusdt_total,
        'btcbrl_total': btcbrl_total,
        'ethbrl_total': ethbrl_total,
        'solbrl_total': solbrl_total,
        'usdt_total': usdt_total,
        'brl_total': brl_total
    }

def load_data(datetime,
        btcusdt_rate,
        ethusdt_rate,
        solusdt_rate,
        btcbrl_rate,
        ethbrl_rate,
        solbrl_rate,
        btcusdt_total,
        ethusdt_total,
        solusdt_total,
        btcbrl_total,
        ethbrl_total,
        solbrl_total,
        brl_total,
        usdt_total):        

    host = get_keys("gcp", "host")
    port = get_keys("gcp", "port")
    database = get_keys("gcp", "database")
    user = get_keys("gcp", "user")
    password = get_keys("gcp", "password")

    fetch_and_load(host, port, database, user, password, 
        datetime,
        btcusdt_rate,
        ethusdt_rate,
        solusdt_rate,
        btcbrl_rate,
        ethbrl_rate,
        solbrl_rate,
        btcusdt_total,
        ethusdt_total,
        solusdt_total,
        btcbrl_total,
        ethbrl_total,
        solbrl_total,
        brl_total,
        usdt_total)