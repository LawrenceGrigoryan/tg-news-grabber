"""
Connection utilities for MySQL server
"""
import os
from typing import NoReturn
import sqlalchemy
import pandas as pd
from mysql.connector import connect


def get_connection_mysql():
    """
    Get connection to MySQL
    """
    connection = connect(
        host=os.getenv("MYSQL_HOST"),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD")
    )
    return connection


def save_df_mysql(
        df: pd.DataFrame,
        user: str,
        password: str,
        host: str,
        database: str,
        table_name: str
        ) -> NoReturn:
    """
    Saves pandas dataframe to the given table

    Args:
        data (pd.DataFrame): _description_
        conf (dict): Database config
    """
    try:
        connection_str = 'mysql+mysqlconnector://{}:{}@{}/{}'. \
            format(user, password, host, database)
        connection = sqlalchemy.create_engine(connection_str)
        df.to_sql(
            con=connection, 
            name=table_name, 
            if_exists='append', 
            index=False
        )
    except Exception as e:
        print(e)
