import sqlalchemy
import os
import configparser
import urllib
import pyodbc


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

    config_string = get_config_string(config_name)
    params = urllib.parse.quote(config_string)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    return engine


def get_config_string(config_name):
    config = configparser.ConfigParser()

    # read more about working directory at https://stackoverflow.com/questions/13800515/cant-load-relative-config-file-using-configparser-from-sub-directory
    config.read(os.path.join(os.path.abspath(os.path.dirname('__file__')), 'conf', 'config.ini'))

    server = config[config_name]['SERVER']
    database = config[config_name]['DB']
    username = config[config_name]['USERNAME']
    password = config[config_name]['PASSWORD']
    driver = config[config_name]['DRIVER']
    port = config[config_name]['PORT']

    config_string = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=' + port + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    return config_string


def connect_to_db(config_name):
    config_string = get_config_string(config_name)
    conn = pyodbc.connect(config_string)
    return conn
