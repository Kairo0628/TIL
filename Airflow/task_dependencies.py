from airflow.sdk import DAG, chain, cross_downstream
from airflow.providers.standard.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    dag_id = 'task_dependencies_dag',
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

    t4 = EmptyOperator(
        task_id = 'empty_task_4'
    )

    t5 = EmptyOperator(
        task_id = 'empty_task_5'
    )

    t6 = EmptyOperator(
        task_id = 'empty_task_6'
    )

    t1 >> t2 >> t3
    t3 << t2 << t1
    t1 >> [t2, t3]

    # t1 >> t2 >> t3
    chain(t1, t2, t3)

    # [t1, t2] >> [t3, t4] 실제로는 리스트끼리 연결하는 방식은 불가능
    # t1 >> t3
    # t2 >> t4
    chain([t1, t2], [t3, t4])

    # t1 >> t3
    # t1 >> t4
    # t2 >> t3
    # t2 >> t4
    cross_downstream([t1, t3], [t3, t4])
