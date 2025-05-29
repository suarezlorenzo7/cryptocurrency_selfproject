import hashlib
import hmac
import requests
import time


def get_crypto_amounts(api_key, signature_key) -> dict:

    """Expects an API key and a signature (secret) key from a Binance account and returns a dict of coins and free amounts in that account."""

    timestamp = int(time.time() * 1000)         # time in miliseconds
    timestamp_string = 'timestamp={}'.format(timestamp)
    signature = hmac.new(bytes(signature_key , 'latin-1'), msg = bytes(timestamp_string, 'latin-1'), digestmod = hashlib.sha256).hexdigest()

    headers = {
        "X-MBX-APIKEY": api_key
    }

    url = "https://api.binance.com/api/v3/account?" + timestamp_string + "&signature=" + signature
    response = requests.get(url, headers=headers)

    if response.status_code == 200: 
        data = response.json()
        balances = data.get('balances', [])
        crypto_amounts = {item['asset']: item['free'] for item in balances if float(item['free']) > 0 and not item['asset'] =='BRL'}
        
        return crypto_amounts

    else:
        print(f'Error: {response.status_code}')
        print(response.text)
    
        return None



