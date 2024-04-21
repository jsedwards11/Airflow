import psycopg2
from psycopg2 import OperationalError


def connect_to_database():
    """Connect to the PostgreSQL database."""
    try:
        # Establish a connection to the database
        connection = psycopg2.connect(
            user="airflow",
            password="airflow",
            host="localhost",
            port="5432",
            database="airflow"
        )
        print("Connected to the database successfully!")
        return connection
    except OperationalError as e:
        print(f"The error '{e}' occurred.")


def execute_query(connection, query):
    """Execute a query on the database."""
    try:
        # Create a cursor object to execute queries
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch all the results
        records = cursor.fetchall()

        # Print the results
        for record in records:
            print(record)

        # Close the cursor
        cursor.close()
    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)


if __name__ == "__main__":
    # Connect to the database
    connection = connect_to_database()

    # Define your query
    query = "SELECT * FROM reddit;"

    # Execute the query
    execute_query(connection, query)

    # Close the database connection
    connection.close()
