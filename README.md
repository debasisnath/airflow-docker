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

#### MAC 
...
Astro cli  for mac...

Use gRPC FUSE for file sharing
...


#### Extre videos:
- 1: Airflow Data Pipeline with AWS and Snowflake for Beginners | Project
   - https://www.youtube.com/watch?v=wT67h9qDl1o

```text
You can find the text version of that video and orignal DAG here:
https://www.youtube.com/redirect?event=video_description&redir_token=QUFFLUhqbmZjZzdEYzNZYlA0YUVqQkUwVXVYaEpaeWJwd3xBQ3Jtc0tuOVNESkxORzA1eFBTa1ItOGd2bkRzS05lTVFONEFoekxKRlhGX2VOWTgxaDZEUkdSS3dsOXJOcVJ0dWFqc2dWRkhiQW1XWDZGUEN6NTZDeW5Ud1YzUTB1T3I1NXRPUGFXd21xY21pWFI3d1dldkR3NA&q=https%3A%2F%2Fastro-sdk-python.readthedocs.io%2Fen%2Fstable%2Fgetting-started%2FGETTING_STARTED.html&v=wT67h9qDl1o

➡️ Astro CLI
docs.astronomer.io/astro/cli/install-cli

[
curl -sSL install.astronomer.io | sudo bash -s
]

[
astro dev init
astro dev start  

]



Materials:
➡️ orders_data_header.csv
order_id,customer_id,purchase_date,amount
ORDER1,CUST1,1/1/2021,100
ORDER2,CUST2,2/2/2022,200
ORDER3,CUST3,3/3/2023,300

➡️ Env vars

AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
AIRFLOW__ASTRO_SDK__SQL_SCHEMA=ASTRO_SDK_SCHEMA

➡️ SQL requests

CREATE DATABASE ASTRO_SDK_DB;
CREATE WAREHOUSE ASTRO_SDK_DW;
CREATE SCHEMA ASTRO_SDK_SCHEMA;

CREATE OR REPLACE TABLE customers_table (customer_id CHAR(10), customer_name VARCHAR(100), type VARCHAR(10) );

INSERT INTO customers_table (CUSTOMER_ID, CUSTOMER_NAME,TYPE) VALUES     ('CUST1','NAME1','TYPE1'),('CUST2','NAME2','TYPE1'),('CUST3','NAME3','TYPE2');

CREATE OR REPLACE TABLE reporting_table (
    CUSTOMER_ID CHAR(30), CUSTOMER_NAME VARCHAR(100), ORDER_ID CHAR(10), PURCHASE_DATE DATE, AMOUNT FLOAT, TYPE CHAR(10));

INSERT INTO reporting_table (CUSTOMER_ID, CUSTOMER_NAME, ORDER_ID, PURCHASE_DATE, AMOUNT, TYPE) VALUES
('INCORRECT_CUSTOMER_ID','INCORRECT_CUSTOMER_NAME','ORDER2','2/2/2022',200,'TYPE1'),
('CUST3','NAME3','ORDER3','3/3/2023',300,'TYPE2'),
('CUST4','NAME4','ORDER4','4/4/2022',400,'TYPE2');

 

```
