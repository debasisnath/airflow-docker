.env

AIRFLOW_IMAGE_NAME=apache/airflow:2.4.2
AIRFLOW_UID=50000


mkdir -p ./logs


sudo chown -R 50000:0 ./logs


chmod -R 755 ./logs



docker compose up --build


ERROR: You need to upgrade the database. Please run `airflow db upgrade`. Make sure the command is run using Airflow version 2.4.2.


docker-compose down --volumes --remove-orphans
docker-compose up --build
