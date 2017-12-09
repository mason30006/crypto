import pyodbc
import os, configparser

config = configparser.ConfigParser()
config.read(os.path.join(os.path.abspath(os.path.dirname('__file__')), 'conf', 'config.ini')) # read more about working directory at https://stackoverflow.com/questions/13800515/cant-load-relative-config-file-using-configparser-from-sub-directory

server = config['DB_CONFIG']['SERVER']
database = config['DB_CONFIG']['DB']
username = config['DB_CONFIG']['USERNAME']
password = config['DB_CONFIG']['PASSWORD']
driver = config['DB_CONFIG']['DRIVER']
port = config['DB_CONFIG']['PORT']

cnxn = pyodbc.connect('DRIVER='+driver+';PORT='+port+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

