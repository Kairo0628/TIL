from airflow.sdk import DAG, task, get_current_context
from airflow.providers.standard.operators.bash import BashOperator

@task
def xcom_push_task():
    value = {'a': 123, 'b': 345, 'c': 567}

    context = get_current_context()
    ti = context['ti']
    ti.xcom_push(key = 'xcom_push', value = value)

@task
def xcom_pull_task():
    context = get_current_context()
    ti = context['ti']
    value = ti.xcom_pull(key = 'xcom_push', task_ids = 'xcom_push_task')

    print(value)

    print(value['a'])
    print(value['b'])
    print(value['c'])

@task(do_xcom_push = True)
def do_xcom_push_task():
    value = {1: 'a', 2: 'b', 3: 'c'}
    
    return value

@task
def do_xcom_pull_task():
    context = get_current_context()
    ti = context['ti']

    value = ti.xcom_pull(task_ids = 'do_xcom_push_task')

    print(value)

    print(value['1'])
    print(value['2'])
    print(value['3'])

with DAG(
    dag_id = 'xcoms_test',
    start_date = None,
    schedule = None
) as dag:
    
    t1 = xcom_push_task()
    t2 = xcom_pull_task()

    t1 >> t2

    t3 = do_xcom_push_task()
    t4 = do_xcom_pull_task()

    t3 >> t4
    
    t5 = BashOperator(
        task_id = 'bash_xcom_push',
        bash_command = 'echo "BashOperator XCom Push" && '
        'echo "{{ ti.xcom_push(key = "bash_operator_push", value = 3) }}" ',
    )

    t6 = BashOperator(
        task_id = 'bash_xcom_pull',
        bash_command = 'echo "BashOperator XCom Pull" && '
        'echo "XCom pushed value: {{ ti.xcom_pull(key = "bash_operator_push", task_ids = "bash_xcom_push") }}"',
    )

    t5 >> t6
