import sqlalchemy
import urllib
import pyodbc
import pandas as pd
import utils.config as cf


def create_engine(config_name):
    '''
    Parameters
    ----------
    config_name : str
        Section headers in config file

    Returns
    -------
    sqlalchemy engine
    '''

    config_string = cf.get_sql_credentials(config_name)
    params = urllib.parse.quote(config_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine


def connect_to_db(config_name):
    config_string = cf.get_sql_credentials(config_name)
    conn = pyodbc.connect(config_string)
    return conn


def execute(query, conn):
    '''Wrapper to execute SQL queries'''
    cursor = conn.cursor().execute(query)

    columns = [column[0] for column in cursor.description]

    result = pd.DataFrame(columns=columns)
    for row in cursor.fetchall():
        result = result.append(pd.DataFrame(list(row), index=columns).T)

    return result
