from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from datetime import datetime, timedelta 
import requests
from include.stock_market.tasks import get_stock_prices, store_prices
from airflow.providers.docker.operators.docker import DockerOperator

SYMBOL = 'AAPL'

dag_owner = ''

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='stock_market',
        default_args=default_args,
        description='stock_market',
        start_date=datetime(2023,1,1),
        schedule_interval='@daily',
        catchup=False,
        tags=['stock_market']
):

    start = EmptyOperator(task_id='start')

    @task.sensor(poke_interval=30, timeout=300, mode='poke')    
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        print('api', api)
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print("url", url)
        res = requests.get(url, headers=api.extra_dejson['headers'])
        print('json', res.json())
        condition = res.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id="get_stock_prices",
        python_callable=get_stock_prices,
        op_kwargs={'url': '{{ task_instance.xcom_pull(task_ids="is_api_available") }}', 'symbol': SYMBOL}
        # op_args: Optional[List] = None,
        # templates_dict: Optional[Dict] = None
        # templates_exts: Optional[List] = None
    )       
    
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=store_prices,
        op_kwargs={'stock': '{{ task_instance.xcom_pull(task_ids="get_stock_prices") }}'}
    )
    
    format_prices = DockerOperator(
        task_id='format_prices',
        image='airflow/stock-app',
        container_name='format_prices',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        environment={
            'SPARK_APPLICATION_ARGS': '{{ task_instance.xcom_pull(task_ids="store_prices") }}'
        }
    )
        

    end = EmptyOperator(task_id='end')

    start >> is_api_available() >> get_stock_prices >> store_prices >> format_prices >> end