import json
import requests


def get_crypto_rates(symbol: str) -> float:
    
    """Expects uppercase symbol containing 2 concatenated currency codes and returns the exchange rate between them."""
    
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    response = requests.get(url)

    if response.status_code == 200: 
        
        data = response.json()
        return data['price']

    else:

        print(f'Error: {response.status_code}')
        print(response.text)
        return None
