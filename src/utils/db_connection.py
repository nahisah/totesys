"""
Contains the utility functions for opening and closing connections to a Postgres database.
"""

import os

import dotenv
from pg8000.native import Connection


def create_conn():
    """
    This function creates a connection to a PostgreSQL database using credentials from environment variables.

    # Returns:
        A pg8000 database Connection object.
    """
    dotenv.load_dotenv()

    user = os.environ["DBUSER"]
    database = os.environ["DBNAME"]
    password = os.environ["DBPASSWORD"]
    host = os.environ["HOST"]

    return Connection(database=database, user=user, password=password, host=host)


def close_conn(conn):
    """
    This function closes the connection to a PostgreSQL database.

    # Arguments:
        conn: a pg8000 Connection object that is to be closed.

    # Returns:
        None.
    """
    conn.close()
