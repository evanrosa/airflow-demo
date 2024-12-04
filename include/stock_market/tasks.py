import requests
from airflow.hooks.base import BaseHook
import json
from minio import Minio
from io import BytesIO
from airflow.exceptions import AirflowNotFoundException

BUCKET_NAME = 'stock-market'  # Use a valid bucket name

def get_minio_client():
    minio = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
        access_key=minio.login,
        secret_key=minio.password,
        secure=False
    )
    return client   

def get_stock_prices(url, symbol):
    url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
    api = BaseHook.get_connection('stock_api')
    res = requests.get(url, headers=api.extra_dejson['headers'])
    return json.dumps(res.json()['chart']['result'][0])


def store_prices(stock):
    client = get_minio_client()
    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)
    stock = json.loads(stock)
    symbol = stock['meta']['symbol']
    data = json.dumps(stock, ensure_ascii=False).encode('utf8')
    client.put_object(
        bucket_name=BUCKET_NAME,
        object_name=f'{symbol}/prices.json',
        data=BytesIO(data),
        length=len(data)
    )
    
    path = f'{BUCKET_NAME}/{symbol}'
    print(f"Storing path to XCom: {path}")  # Add this line for logging
    return path

def get_formatted_csv(path):
    path = 'stock-market/AAPL'
    client = get_minio_client()
    prefix_name = f"{path.split('/')[1]}/formatted_prices/"
    objects = client.list_objects(BUCKET_NAME, prefix=prefix_name, recursive=True)
    for obj in objects:
        if obj.object_name.endswith('.csv'):
            return  obj.object_name
    raise AirflowNotFoundException('The csv file does not exist.')
    