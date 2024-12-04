import requests
from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO

def get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    res = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(res.json()['chart']['result'][0])


def store_prices(stock):
    minio = BaseHook.get_connection('minio')
    print('minio', minio)
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    bucket_name = 'stock-market'  # Use a valid bucket name
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    objw = client.put_object(
        bucket_name=bucket_name,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    
    path = f'{objw.bucket_name}/{symbol}'
    print(f"Storing path to XCom: {path}")  # Add this line for logging
    return path