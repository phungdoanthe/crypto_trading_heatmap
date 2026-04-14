FROM apache/airflow:3.2.0-python3.11

ENV AIRFLOW_HOME=/opt/airflow
WORKDIR $AIRFLOW_HOME

COPY requirements.txt .
RUN pip install --user -r requirements.txt

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/modules"
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=false
ENV AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=15