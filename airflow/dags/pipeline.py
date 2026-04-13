# airflow/dags/pipeline.py
from airflow.decorators import dag, task
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/usr/local/airflow/modules')

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(seconds=30),
}

@dag(
    dag_id='crypto_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule='0 8 * * *',
    catchup=False,
)
def crypto_pipeline():

    @task()
    def submit_trade_job(execution_timeout=timedelta(minutes=9)):
        import requests
        res = requests.post(
            'http://jobmanager:8081/jars/upload',
            files={'jarfile': open('/opt/src/job/trade_aggregate_tumbling_window.py', 'rb')}
        )
        res.raise_for_status()

    @task()
    def submit_ob_job(execution_timeout=timedelta(minutes=9)):
        import requests
        res = requests.post(
            'http://jobmanager:8081/jars/upload',
            files={'jarfile': open('/opt/src/job/ob_aggregate_tumbling_window.py', 'rb')}
        )
        res.raise_for_status()


    submit_trade_job()
    submit_ob_job()

crypto_pipeline()