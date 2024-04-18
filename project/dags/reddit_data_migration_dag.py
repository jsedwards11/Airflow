"""Schedule a single task to execute the reddit_data_migration.py script. Configured to run once, and retrieves the
database connection string from an Airflow variable."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'reddit_data_migration',
    default_args=default_args,
    description='Reddit Data Migration',
    schedule_interval="@once",
    start_date=datetime.strptime("2024-01-01", '%Y-%m-%d'),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='reddit_data_migration',
        bash_command='python /opt/airflow/dags/reddit_data_migration.py --connection %s' % Variable.get("data_dev_connection")
    )
