# DBModule.py

import sqlite3
import pandas as pd


def connect_and_create_db(database_location, database_name):
    """
    Connects to SQLite server and creates a new database space.

    Parameters:
    - database_location: Location where the database will be stored.
    - database_name: Name of the database.

    Returns:
    - Connection object to the created database.
    """
    connection = sqlite3.connect(f'{database_location}/{database_name}.db')
    print(f"Connected to SQLite database: {database_name}")
    return connection


def read_csv(csv_location, csv_filename):
    """
    Reads data from a CSV file.

    Parameters:
    - csv_location: Location of the CSV file.
    - csv_filename: Name of the CSV file.

    Returns:
    - Pandas DataFrame containing the CSV data.
    """
    csv_path = f'{csv_location}/{csv_filename}.csv'
    df = pd.read_csv(csv_path)
    print(f"Read data from CSV file: {csv_filename}")
    return df


def write_to_table(dataframe, table_name, conn):
    """
    Writes CSV data to a table in the SQLite database.

    Parameters:
    - dataframe: Pandas DataFrame containing the data to be stored.
    - table_name: Name of the table in the database.
    """
    dataframe.to_sql(table_name, conn, index=False, if_exists='replace')
    print(f"Data written to table: {table_name}")
