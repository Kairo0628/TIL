from airflow.sdk import DAG, dag
from airflow.providers.standard.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    dag_id = 'context_manager_dag',
    start_date = datetime(2026, 4, 1),
    schedule = None
) as dag:
    
    t1 = EmptyOperator(
        task_id = 'empty_task_1'
    )

    t2 = EmptyOperator(
        task_id = 'empty_task_2'
    )

    t3 = EmptyOperator(
        task_id = 'empty_task_3'
    )


# 하나의 파일에 여러개의 DAG를 선언할 수 있음, 하지만 맨 위의 DAG만 로드됨.
standard_dag = DAG(
    dag_id = 'standard_constructor_dag',
    start_date = datetime(2026, 4, 1),
    shedule = None
)

t1 = EmptyOperator(
    task_id = 'empty_task',
    dag = standard_dag
)

# 하나의 파이썬 파일에 여러개의 DAG를 만들어 둔 뒤, 실제 DAG 파일에서 하나만 가져오는 방식을 사용할 수도 있음.
# 이 경우, my_dag = decorator_dag() 선언 뒤 import my_dag를 통해 가져와야 함.
@dag(
    dag_id = 'decorator_dag',
    start_date = datetime(2026, 4, 1),
    schedule = None
)
def decorator_dag():
    t1 = EmptyOperator(
        task_id = 'empty_task'
    )

decorator_dag()
