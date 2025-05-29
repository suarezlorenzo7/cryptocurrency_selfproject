import pandas as pd
import time
from datetime import datetime

from .extract_crypto_amounts import get_crypto_amounts
from .extract_crypto_rates import get_crypto_rates
from utils.get_keys import get_keys

def generate_dataframe(api_key, signature_key) -> pd.DataFrame: 
    
    """Expects both an API and a Signature key and returns a dataframe containing crypto, USD and BRL amounts (converted)"""

    real_currencies = ['BRL', 'USDT']

    symbols_list = []

    crypto_amounts = get_crypto_amounts(api_key, signature_key)

    if crypto_amounts is None:
        
            # Handle the case where get_crypto_amounts returns None
            raise ValueError("Failed to fetch crypto amounts. Received None.")

    df = pd.DataFrame(columns=['datetime'])

    # Get the current datetime

    now = datetime.now()
    formatted_datetime = now.strftime("%Y-%m-%d %H:%M")

    data_dict = {'datetime': formatted_datetime}

    for currency, amount in crypto_amounts.items():
        
        for real_currency in real_currencies:

            symbol = currency + real_currency

            exchange_rate = get_crypto_rates(symbol=symbol)

            total_value = float(amount) * float(exchange_rate)
                
            # Add the exchange rate and total value as new columns
            data_dict[symbol + '_rate'] = round(float(exchange_rate), 2)
            data_dict[symbol + '_total'] = round(float(total_value), 2)

    df = pd.concat([df, pd.DataFrame(data_dict, index=[0])], ignore_index=True)

    usdt_columns = [col for col in df.columns if 'USDT' in col and 'total' in col]
    brl_columns = [col for col in df.columns if 'BRL' in col and 'total' in col]

    df['usdt_total'] = round(df[usdt_columns].sum(axis=1), 2)
    df['brl_total'] = round(df[brl_columns].sum(axis=1), 2)

    return df


