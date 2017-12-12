import gdax
import utils.config as cf


def get_auth_client():
    '''Small wrapper to get GDAX credentials and return authentication client'''
    api_dict = cf.get_gdax_credentials('GDAX')
    auth_client = gdax.AuthenticatedClient(api_dict['key'], api_dict['secret'], api_dict['password'])
    return auth_client

auth_client = get_auth_client()
auth_client.get_accounts()
