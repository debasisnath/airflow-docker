# DQI_config_vw.py

from datetime import datetime, timedelta

# Hive Connection Settings
HIVE_HOST = "10.1.22.123"
HIVE_PORT = 10000
HIVE_DATABASE = "edl_metadata_db"
HIVE_USERNAME = "edlpbbl"
# Additional Hive configuration settings if needed
HIVE_CONFIG = {"hive.execution.engine": "tez"}

# Impala Connection Settings
IMPALA_HOST = "10.1.22.127"
IMPALA_PORT = 21050

# Paths for SQL and reports
SQL_FILE_PATH = "/edl/NathDebasis/DQI_scripts/SQL_VW/"
XLSX_PATH = "/edl/NathDebasis/DQI_scripts/reports/"
RPT_FILE_NAME = "micro_DQ_Rule_template_vw_test_"

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

POST_TXT = get_month_last_date("%b_%Y", 2).casefold()  # feb_2025

# SQL Context Dictionary
SQL_CONTEXT = {
    "table_name_curated": f"default.cust_loan_micro_curated",
    "table_name": f"default.cust_loan_micro",
    "full_dte": f"{get_month_last_date('%Y-%m-%d', 2)}"
}

THRESHOLD = 0.95
EXPECTED_SCORE_CUSTOMER_NAME = 10
EXPECTED_SCORE_AGE_DOB = 7
