from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

def transform_tasks():

    with TaskGroup("transforms", tooltip="Transform tasks") as group:

        transform_a = BashOperator(
            task_id='transform_a',
            bash_command='sleep 10'
        )

        transform_b = BashOperator(
            task_id='transform_b',
            bash_command='sleep 10'
        )

        transform_c = BashOperator(
            task_id='transform_c',
            bash_command='sleep 10'
        )
        return group



def downloads_tasks():

    with TaskGroup("downloadss", tooltip="downloads tasks") as group:

        downloads_a = BashOperator(
            task_id='downloads_a',
            bash_command='sleep 10'
        )

        downloads_b = BashOperator(
            task_id='downloads_b',
            bash_command='sleep 10'
        )

        downloads_c = BashOperator(
            task_id='downloads_c',
            bash_command='sleep 10'
        )
        return group
