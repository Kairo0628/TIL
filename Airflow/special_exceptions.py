from airflow.sdk import DAG, TriggerRule
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
import logging

def skip_task():
    logging.info('Skip Task Start')

    raise AirflowSkipException

def fail_task():
    logging.info('Fail Task Start')

    raise AirflowFailException

with DAG(
    dag_id = 'special_exceptions_dag',
    start_date = datetime(2026, 4, 1)
) as dag:
    
    t1 = PythonOperator(
        python_callable = skip_task,
        task_id = 'skip_task'
    )

    t2 = PythonOperator(
        python_callable = fail_task,
        task_id = 'fail_task',
        retries = 3,
        trigger_rule = TriggerRule.ALWAYS
    )

    t1 >> t2
