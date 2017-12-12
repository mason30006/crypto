import os
import configparser


def set_config():
    '''Small wrapper to set config parser'''
    config = configparser.ConfigParser()

    # read more about working directory at https://stackoverflow.com/questions/13800515/cant-load-relative-config-file-using-configparser-from-sub-directory
    config.read(os.path.join(os.path.abspath(os.path.dirname('__file__')), 'conf', 'config.ini'))
    return config


def get_sql_credentials(config_name):
    '''Grab Azure server credentials'''
    config = set_config()

    server = config[config_name]['SERVER']
    database = config[config_name]['DB']
    username = config[config_name]['USERNAME']
    password = config[config_name]['PASSWORD']
    driver = config[config_name]['DRIVER']
    port = config[config_name]['PORT']

    config_string = 'DRIVER=' + driver + ';SERVER=' + server + ';PORT=' + port + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    return config_string


def get_gdax_credentials(config_name):
    '''Grab Gdax credentials'''
    config = set_config()

    secret = config[config_name]['SECRET']
    key = config[config_name]['KEY']
    password = config[config_name]['PASS']

    return {'key': key, 'secret': secret, 'password': password}
