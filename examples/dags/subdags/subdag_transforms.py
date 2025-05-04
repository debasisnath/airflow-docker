
from airflow import DAG
from airflow.operators.bash import BashOperator

def subdag_transforms (parent_dag_id, child_dag_id, args):
    with DAG(f" {parent_dag_id}.{child_dag_id}",
                start_date=args['start_date'],
                schedule_interval=args['schedule_interval'], 
                catchup=args['catchup']) as dag:
        
        transforms_a = BashOperator(
            task_id='transforms_a', 
            bash_command= 'sleep 10'
        )

        transforms_b = BashOperator(
            task_id='transforms_b',
            bash_command= 'sleep 10'
        )

        transforms_c = BashOperator(
            task_id='transforms_c',
            bash_command= 'sleep 10'
        )

        return dag
