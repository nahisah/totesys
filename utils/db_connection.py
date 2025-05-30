from pg8000.native import Connection
import dotenv
import os



def create_conn():
    dotenv.load_dotenv()

    user = os.environ['DBUSER']
    database = os.environ["DBNAME"]
    password = os.environ["DBPASSWORD"]

    return Connection(
        database=database,
        user=user,
        password=password
    )
    

def close_conn(conn):
    conn.close()