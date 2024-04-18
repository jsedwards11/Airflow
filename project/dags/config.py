import os

REDDIT_API = os.getenv("REDDIT_API", "https://www.reddit.com/r/Cars/top.json?limit=10")
CSV_FILE_DIR = os.getenv("CSV_FILE_DIR", "/opt/airflow/dags/datasets/reddit")

PSQL_DB = os.getenv("PSQL_DB", "airflow")
PSQL_USER = os.getenv("PSQL_USER", "airflow")
PSQL_PASSWORD = os.getenv("PSQL_PASSWORD", "airflow")
PSQL_PORT = os.getenv("PSQL_PORT", "5432")
PSQL_HOST = os.getenv("PSQL_HOST", "localhost")
