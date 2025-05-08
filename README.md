.env
```env
AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
AIRFLOW_UID=50000
```

```bash
mkdir -p ./logs
sudo chown -R 50000:0 ./logs
chmod -R 755 ./logs
docker compose up --build
```

- ERROR: You need to upgrade the database. Please run `airflow db upgrade`. Make sure the command is run using Airflow version 2.4.2.

```bash
docker compose down --volumes --remove-orphans
docker compose up --build


docker compose ps

docker cp materials_airflow-scheduler_1:/opt/airflow/airflow.cfg  .

```


#### The Celery Executor settings
```bash

executor=CeleryExecutor
sql_alchemy_conn=postgresql+psycopg2://<user><password>@<host>/<db>
celery_result_backend postgresql+psycopg2://<user><password>@<host>/<db>
celery_broker_url=redis://@redis:6379/0
```


### Tipical containers set of Aitflow

| NAME                                     | IMAGE                  | COMMAND                     | SERVICE             | CREATED     | STATUS              | PORTS                                           |
|------------------------------------------|-------------------------|------------------------------|---------------------|-------------|---------------------|--------------------------------------------------|
| apache-airflow-251-airflow-scheduler-1   | apache/airflow:2.4.2   | "/usr/bin/dumb-init …"      | airflow-scheduler   | 3 days ago  | Up 3 hours (healthy) | 8080/tcp                                        |
| apache-airflow-251-airflow-triggerer-1   | apache/airflow:2.4.2   | "/usr/bin/dumb-init …"      | airflow-triggerer   | 3 days ago  | Up 3 hours (healthy) | 8080/tcp                                        |
| apache-airflow-251-airflow-webserver-1   | apache/airflow:2.4.2   | "/usr/bin/dumb-init …"      | airflow-webserver   | 3 days ago  | Up 3 hours (healthy) | 0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp     |
| apache-airflow-251-airflow-worker-1      | apache/airflow:2.4.2   | "/usr/bin/dumb-init …"      | airflow-worker      | 3 days ago  | Up 3 hours (healthy) | 8080/tcp                                        |
| apache-airflow-251-postgres-1            | postgres:13            | "docker-entrypoint.s…"      | postgres            | 3 days ago  | Up 3 hours (healthy) | 5432/tcp                                        |
| apache-airflow-251-redis-1               | redis:latest           | "docker-entrypoint.s…"      | redis               | 3 days ago  | Up 3 hours (healthy) | 6379/tcp                                        |


#### Restart Airflow docker (with CeleryExecutor)

```bash

docker compose down && docker compose --profile flower up

```


#### Remove All DAG examples

- Open the file docker-compose.yaml
- Replace the value 'true' by 'false' for the AIRFLOW__CORE__LOAD_EXAMPLES environment variables
- Restart Airflow by typing `docker-compose down && docker-compose up -d `


####  Concurrency, the parameters you must know!

parallelism / AIRFLOW__CORE__PARALELISM

max_active_tasks_per_dag / AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG

max_active_runs_per_dag / AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG


-  https://marclamberti.com/blog/airflow-kubernetes-executor/
-  https://marclamberti.com/blog/how-to-use-timezones-in-apache-airflow/
-  https://marclamberti.com/blog/airflow-bashoperator/ 
-  https://marclamberti.com/blog/templates-macros-apache-airflow/
-  https://marclamberti.com/blog/variables-with-apache-airflow/
-  https://marclamberti.com/blog/apache-airflow-best-practices-1/
-  https://marclamberti.com/blog/running-apache-airflow-locally-on-kubernetes/
-  https://marclamberti.com/blog/the-postgresoperator-all-you-need-to-know/


###  apache-airflow-providers-docker

```bash 

pip install apache-airflow-providers-docker
```


