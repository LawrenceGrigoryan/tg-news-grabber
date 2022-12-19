"""Connection utilities for MySQL server"""
from typing import NoReturn
import omegaconf
import sqlalchemy
import pandas as pd
# from mysql.connector import connect, Error


def save_table_mysql(
        df: pd.DataFrame,
        conf: omegaconf.dictconfig.DictConfig,
        ) -> NoReturn:
    """
    Saves pandas dataframe to the given table

    Args:
        data (pd.DataFrame): _description_
        conf (dict): Database config
    """
    try:
        connection_str = 'mysql+mysqlconnector://{}:{}@{}/{}'. \
            format(conf.user, conf.password, conf.host, conf.database)
        connection = sqlalchemy.create_engine(connection_str)
        df.to_sql(
            con=connection, 
            name=conf.table_name, 
            if_exists='append', 
            index=False
        )
    except Exception as e:
        print(e)
