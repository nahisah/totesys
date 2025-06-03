from pg8000.native import Connection
import dotenv
import os



def create_conn():

    """
    This function creates a connection to PostgreSQL database using credentials from environment variables. 
    Loads environment variables using dotenv retrieves the database the credentials and returns a connection object.

    Returns:
        Connection: Database connection to PostgreSQL database
    
    """
    dotenv.load_dotenv()

    user = os.environ['DBUSER']
    database = os.environ["DBNAME"]
    password = os.environ["DBPASSWORD"]
    host = os.environ["HOST"]

    return Connection(
        database=database,
        user=user,
        password=password,
        host = host
    )
    

def close_conn(conn):

    """
    Closes connection to PostgreSQL database.

    Arguments:
        conn: Live PostgreSQL connection object that is to be closed.  

    Returns:
        None

    """
    conn.close()