from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.empty import EmptyOperator

from datetime import datetime

with DAG(
    dag_id = 'task_group_dag',
    start_date = datetime(2026, 4, 1)
) as dag:
    
    start = EmptyOperator(
        task_id = 'start'
    )

    # Task Group 1
    with TaskGroup(
        group_id = 'group1'
    ) as group1:
        t1_1 = EmptyOperator(
            task_id = 'group1_task1'
        )

        t1_2 = EmptyOperator(
            task_id = 'group1_task2'
        )

        t1_3 = EmptyOperator(
            task_id = 'group1_task3'
        )

        t1_1 >> [t1_2, t1_3]
    # Group 1 End

    # Task Group 2
    with TaskGroup(
        group_id = 'group2'
    ) as group2:
        t2_1 = EmptyOperator(
            task_id = 'group2_task1'
        )

        # Task Group 3
        with TaskGroup(
            group_id = 'group2_1'
        ) as group2_1:
            t2_2_1 = EmptyOperator(
                task_id = 'group2_1_task_1'
            )

            t2_2_2 = EmptyOperator(
                task_id = 'group2_1_task_2'
            )

            t2_2_1 >> t2_2_2
        # Group 3 End

        t2_1 >> group2_1
    # Group 2 End

    end = EmptyOperator(
        task_id = 'end'
    )

    start >> group1 >> group2 >> end
