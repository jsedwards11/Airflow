"""Overall, this DAG schedules a single task to execute the tomtom_data_migration.py script. The DAG is configured
to run once, and it retrieves the database connection string from an Airflow variable."""

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
    'tomtom_data_migration',
    default_args=default_args,
    description='Tomtom Data Migration',
    schedule_interval="@once",
    start_date=datetime.strptime("2024-01-01", '%Y-%m-%d'),
    catchup=False
) as dag:

    t1 = BashOperator(
        task_id='tomtom_data_migration',
        bash_command='python /opt/airflow/dags/tomtom_data_migration.py --connection %s' % Variable.get("data_dev_connection")
    )
