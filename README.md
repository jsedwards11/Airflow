# Creating an ETL Pipeline in Python Using Docker and Airflow.


## Key Points and Overview

+ The goal of this project is to create an ETL pipeline extracting data for top posts from the cars subreddit of the Reddit API.
+
+ Two dags are created:
+ 1) reddit_data_migration_dag to schedule a single task to execute reddit_data_migration.py. Configured to run once, and retrieves the database connection string from an Airflow variable.
+ 2) reddit_dag to schedule two tasks: reddit_ingestion.py and reddit_to_db.py. Tasks are scheduled to run daily, and t2 depends on t1.
  

## Files

reddit_data_migration.py: creates the reddit database if it does not already exist.
reddit_ingestion.py: import data from Reddit to a CSV file.
reddit_to_db.py: export data from the CSV file and insert data to a database, if the data does not already exist.
query_db.py: example script to query our PostgreSQL 'reddit' table in a docker container.


## Running the code
Run in a local environment by going into the root directory and execute:
```console
docker-compose up --build
```
After Airflow webserver is running, open Airflow in your local browser and go to:
```
localhost:8080
```
with username: `admin`
and password: `admin1234`  
### Set Airflow Variable
Set Variable for connection to Postgresql database in Airflow by going to Admin > Variable with:  
```
Key: data_dev_connection
Value: "postgresql+psycopg2://airflow:airflow@postgres/airflow"
```
