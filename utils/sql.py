import pyodbc
import os, configparser

def connect_to_db(db_name):
	'''db_name needs to be one of the section headers in config'''
	config = configparser.ConfigParser()
	config.read(os.path.join(os.path.abspath(os.path.dirname('__file__')), 'conf', 'config.ini')) # read more about working directory at https://stackoverflow.com/questions/13800515/cant-load-relative-config-file-using-configparser-from-sub-directory

	server = config['db_name']['SERVER']
	database = config['db_name']['DB']
	username = config['db_name']['USERNAME']
	password = config['db_name']['PASSWORD']
	driver = config['db_name']['DRIVER']
	port = config['db_name']['PORT']

	cnxn = pyodbc.connect('DRIVER='+driver+';PORT='+port+';SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
	cursor = cnxn.cursor()

