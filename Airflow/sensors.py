from airflow.sdk import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.time_delta import TimeDeltaSensor

import time
from datetime import datetime

def return_true():
    time.sleep(3)

    return True

with DAG(
    dag_id = 'airflow_sensors_dag',
    start_date = datetime(2026, 4, 1),
    schedule = None
) as dag:
    
    t1 = FileSensor(
        task_id = 'file_sensor_task',
        fs_conn_id = 'fs_conn_id',
        filepath = 'plugins/file_sensor_testing.txt'
    )

    t2 = PythonSensor(
        task_id = 'python_sensor_task',
        python_callable = return_true
    )

    t3 = TimeDeltaSensor(
        task_id = 'time_delta_sensor_task',
        delta = 10
    )

    t1 >> t2 >> t3
