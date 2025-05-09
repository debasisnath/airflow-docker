# docker_dag.py

from airflow.decorators import task, dag
from airflow.provider.docker.operators.docker import DockerOperator 
from docker.types import Mount

from datetime import datetime

@dag (start_date=datetime(2021, 1, 1), schedule_interval='@daily', catchup=False)
def docker_dag():

    @task()
    def t1():
        pass

    t2 = DockerOperator(
        task_id='t2',
        container_name='task_t2',
        api_version='auto',
        image='stock_image:v1.0.0',
        command='bash /tmp/scripts/output.sh  ',  # 👈 
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        xcom_all=True,
        retrieve_output=True,
        retrieve_output_path='/tmp/script.out',
        mem_limit='512m',
        auto_remove=True,
        mounts=[
            Mount(source='/Users/marclamberti/sandbox/includes/scripts', target='/tmp/sc', type="bind")
        ]
    )


    t1() >> t2

dag=docker_dag()


