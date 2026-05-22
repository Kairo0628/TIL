from airflow.sdk import DAG, task, get_current_context
from airflow.providers.standard.operators.empty import EmptyOperator

import time
from datetime import datetime

@task.branch
def choose_task():
    context = get_current_context()
    run_type = context['dag_run'].conf.get('run_type', 't1')

    if run_type == 't1':
        return 'max_active_dag_run_test'
    else:
        return 'max_active_tasks_test'

@task
def max_active_dag_run_test():
    time.sleep(180)

    print('Done')

@task
def parallel_task(num):
    print(num)
    time.sleep(180)

with DAG(
    dag_id = 'dag_concurrency_test_dag',
    start_date = datetime(2026, 5, 1),
    schedule = None,
    max_active_runs = 2,
    max_active_tasks = 2
) as dag:
    
    branch = choose_task()

    t1 = max_active_dag_run_test()
    t2 = EmptyOperator(task_id = 'max_active_tasks_test')

    p1 = parallel_task.override(task_id = 'p1')(num = 1)
    p2 = parallel_task.override(task_id = 'p2')(num = 2)
    p3 = parallel_task.override(task_id = 'p3')(num = 3)

    branch >> [t1, t2]
    t2 >> [p1, p2, p3]
