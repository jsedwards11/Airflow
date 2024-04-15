import psycopg2
import config

# Utilize the database settings from config.py
connection_config = "dbname='{}' user='{}' password='{}' host='{}' port='{}'".format(
    config.PSQL_DB,
    config.PSQL_USER,
    config.PSQL_PASSWORD,
    config.PSQL_HOST,
    config.PSQL_PORT
)


def query_data(sql_query):
    """
    Query data from the PostgreSQL database and print the results.
    :param sql_query: SQL query string
    """
    try:
        # Connect to the database
        conn = psycopg2.connect(connection_config)
        cur = conn.cursor()

        # Execute the query
        cur.execute(sql_query)

        # Fetch all rows from the last executed query
        rows = cur.fetchall()

        # Print each row
        for row in rows:
            print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error: {error}")
    finally:
        # Close the communication with the database
        if conn is not None:
            cur.close()
            conn.close()
            print("Database connection closed.")


# Example SQL query
sql_query = "SELECT * FROM tomtom LIMIT 10;"

if __name__ == "__main__":
    query_data(sql_query)
