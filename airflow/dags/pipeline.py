from airflow.sdk import dag, task
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

    @task.bash()
    def submit_trade_job():
        return """
        docker exec jobmanager \
        ./bin/flink run \
        -py /opt/src/job/trade_aggregate_tumbling_window.py \
        --pyFiles /opt/src -d
        """

    @task.bash()
    def submit_ob_job():
        return """
        docker exec jobmanager \
        ./bin/flink run \
        -py /opt/src/job/ob_aggregate_tumbling_window.py \
        --pyFiles /opt/src -d
        """

    submit_trade_job()
    submit_ob_job()

crypto_pipeline()