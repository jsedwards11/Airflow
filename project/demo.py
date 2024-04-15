import psycopg2

conn = psycopg2.connect(
    database="airflow",
    user="airflow",
    password="airflow",
    host="localhost"
)

cur = conn.cursor()

cur.execute("SELECT * FROM tomtom;")
rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()
