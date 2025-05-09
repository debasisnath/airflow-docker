# airflow_dqi_micro_dags.py

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import pendulum

# Global Configs
local_tz = pendulum.timezone("Asia/Kolkata")
sshHook = SSHHook(ssh_conn_id='conn_edlpsbbl_120', cmd_timeout=None)




# Static Configs
RPT_FILE_NAME = "micro_DQ_Rule_template_vw_test_"
XLSX_PATH = "/edl/NathDebasis/DQI_scripts/reports/"

def get_month_last_date(frmt='%Y-%m-%d', lag=2):
    current_date = datetime.now()
    # current_date = datetime(2025, 4, 24)  # testing
    
    # Calculate the first day of the current month
    first_day_of_current_month = current_date.replace(day=1)
    
    # Calculate the first day of the month 'lag' months before
    for _ in range(lag):
        first_day_of_current_month = first_day_of_current_month.replace(day=1) - timedelta(days=1)
        first_day_of_current_month = first_day_of_current_month.replace(day=1)
    
    # Calculate the last day of the month 'lag' months before
    last_day_of_lag_month = (first_day_of_current_month + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    return last_day_of_lag_month.strftime(frmt)

POST_TXT = get_month_last_date("%b_%Y", 2).casefold()  # eg. feb_2025



# Use a wildcard-style pattern (note: this is just a naming convention, not actual globbing)
excel_dataset = Dataset(uri=f"file://{XLSX_PATH}{RPT_FILE_NAME}*.xlsx")

# Email content
success_text = f"""<h3>The DQI_MICRO_Automation DAG has completed successfully.</h3> 
<div>
<p>File name : `{XLSX_PATH}{RPT_FILE_NAME}_{POST_TXT}.xlsx` </p>
</div>
"""

# Default args
default_args = {
    'email': ['xxxx.xxxx@bbbbb.com', 'aaaa.bbbb@bbbbb.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ------------------ PRODUCER DAG ------------------

@dag(
    dag_id='DQI_MICRO_Producer',
    default_args=default_args,
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 4, 25, tz="Asia/Kolkata"),
    catchup=False,
    tags=["Env:Production", "Project:DQI"],
)
def producer_dag():
    start = DummyOperator(task_id="start")

    run_script = SSHOperator(
        ssh_hook=sshHook,
        task_id='run_dqi_micro_automation',
        command='python3 /edl/NathDebasis/DQI_scripts/DQI_MICRO_Automation_vw_v02.py',
        do_xcom_push=False,
    )

    @task(outlets=[excel_dataset])
    def mark_dataset_updated():
        return "Excel file generated."

    start >> run_script >> mark_dataset_updated()

producer_dag = producer_dag()

# ------------------ CONSUMER DAG ------------------

@dag(
    dag_id='DQI_MICRO_Consumer',
    default_args=default_args,
    schedule=[excel_dataset],  # trigger using dataset 
    start_date=pendulum.datetime(2025, 4, 25, tz="Asia/Kolkata"),
    catchup=False,
    tags=["Env:Production", "Project:DQI"],
)
def consumer_dag():
    @task
    def notify_success():
        return "Excel file detected. Sending email..."

    send_email = EmailOperator(
        task_id='send_success_email',
        to=['xxxx.nnn@bbbb.com', 'aaaaa.bbbbbb@bbbb.com'],
        subject='DQI_MICRO_Automation DAG Success',
        html_content=success_text,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_success() >> send_email

consumer_dag = consumer_dag()


