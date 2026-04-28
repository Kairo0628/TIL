from airflow.sdk import dag, task

from datetime import datetime

@dag(
    dag_id = 'multi_output_test_dag',
    start_date = datetime(2026, 4, 1),
    schedule = None
)
def multi_output_test_dag():

    @task
    def non_multi_output():
        return {
            'A': 'this is A',
            'B': 'this is B'
        }
    
    @task(multiple_outputs = True)
    def multi_output():
        return {
            'A': 'this is multi A',
            'B': 'this is multi B'
        }
    
    @task
    def consume(output):
        print(output['A'])
        print(output['B'])

    t1 = consume(non_multi_output())
    t2 = consume(multi_output())

multi_output_test_dag()
