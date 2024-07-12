import psycopg2
import main
from main import Reader

# potem dodaj to do pliku konfiguracyjnego
hostname = 'staz-wasko.postgres.database.azure.com'
database = 'postgres'
username = "staz"
pwd = "WaskoCoigGliwiceKatowice1"
port_id = 5432
conn = None
cur = None
try:
    conn = psycopg2.connect(
        host = hostname,
        dbname = database,
        user = username,
        password = pwd,
        port = port_id)

    cur = conn.cursor()

    reader = Reader(main.path)
    df = reader.read()

    for index, row in df.iterrows():
        insert_script = 'INSERT INTO test (id, name) VALUES (%s, %s)'
        insert_values = (row['id'], row['name'])  # Adjust column names as per your DataFrame
        cur.execute(insert_script, insert_values)
        conn.commit()


except Exception as error:
    print(error)
finally:
    if cur is not None:
        cur.close()
    if conn is not None:
        conn.close()