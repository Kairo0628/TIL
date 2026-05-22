from airflow.sdk import DAG, task, get_current_context

from datetime import datetime
import time

@task
def pool_test():
    print('Pool test Task Start')

    time.sleep(180)
    print('End')

@task(pool_slots = 3)
def pool_slot_test():
    print('Pool Slot test Task Start')

    time.sleep(180)
    print('End')

@task.branch
def choose_task():
    context = get_current_context()
    run_type = context['dag_run'].conf.get('run_type', 't1')

    if run_type == 't1':
        return 'pool_test'
    else:
        return 'pool_slot_test'


with DAG(
    dag_id = 'pool_test_dag',
    start_date = datetime(2026, 5, 1),
    schedule = None,
    default_args = {
        'pool': 'test_pool'
    }
)as dag:
    
    t1 = pool_test()
    t2 = pool_slot_test()

    branch = choose_task()

    branch >> [t1, t2]
