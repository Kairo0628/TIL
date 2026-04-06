from airflow.sdk import DAG, task, TriggerRule
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator

from datetime import datetime

# Branching
@task.branch(
        task_id = 'branch_task'
)
def branch_func(ti):
    xcom_val = ti.xcom_pull(task_ids = 'empty_task1')
    if xcom_val == None:
        return 'empty_task2-1'
    else:
        return 'empty_task2-2'

with DAG(
    dag_id = 'branch_dag',
    start_date = datetime(2026, 4, 1)
) as dag:

    t1 = EmptyOperator(
        task_id = 'empty_task1'
    )

    t2_1 = EmptyOperator(
        task_id = 'empty_task2-1'
    )

    t2_2 = EmptyOperator(
        task_id = 'empty_task2-2'
    )

    branch_task = branch_func()

    t1 >> branch_task >> [t2_1, t2_2]

# Latest Only
with DAG(
    dag_id = 'latest_only_dag',
    start_date = datetime(2026, 4, 1)
) as dag:
    
    t1 = EmptyOperator(
        task_id = 'empty_task1'
    )

    lt = LatestOnlyOperator(
        task_id = 'latest_only_task'
    )

    t2 = EmptyOperator(
        task_id = 'follow_latest_task'
    )

    t1 >> lt >> t2

# Depends On Past
t1 = EmptyOperator(
    task_id = 'empty_task1',
    depends_on_past = True
)

# Trigger Rule
t1 = EmptyOperator(
    task_id = 'empty_task1'
)

t2 = EmptyOperator(
    task_id = 'empty_task2',
    trigger_rule = TriggerRule.ALL_SUCCESS
)

t1 >> t2
