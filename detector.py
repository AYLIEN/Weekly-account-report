from module import date_operations, file_operations
import os, shutil
import json
import time
import requests
import glob
import json
import xmltodict
import copy
from pprint import pprint
from tqdm import tqdm
from datetime import datetime
from datetime import timedelta

to_exclude= ['@aylien', 'aylien.com']

def main():
   
    ###
    # 1) Detect internal account and append if not lsited
    # {'account_id': '', 'email': 'sasa@rerer.com', 'account_propose': 'internal'}
    
    accounts_file = file_operations(folder='data_retrieved', file_name='accounts', extension='', content='', date='')
    
    accounts= accounts_file.load_json()
    
    listed_accounts = []
    for account in accounts:
        
        if isinstance(account['users']['user'], list):
            for user in account['users']['user']:
                if any(substring in user['email'] for substring in to_exclude):
                    listed_accounts.append({'account_id': account['id'], 'email': account['users']['user']['email'], 'account_propose': 'internal'})
            
        elif isinstance(account['users']['user'], dict):
            if any(substring in account['users']['user']['email'] for substring in to_exclude):
                listed_accounts.append({'account_id': account['id'], 'email': account['users']['user']['email'], 'account_propose': 'internal'})
            else:
                listed_accounts.append({'account_id': account['id'], 'email': account['users']['user']['email'], 'account_propose': 'external'})

        
    #load the file
    accounts_know_file = file_operations(folder='input_data', file_name='accounts_type', extension='', content='', date='')
    
    accounts_know = accounts_know_file.load_json()
    
    #### check accounts haven't been detected and append it:  

    accounts_know_ls =[]        
    for each in accounts_know:
        accounts_know_ls.append(each['email'])

    accounts_know_new=accounts_know
    for account in tqdm(listed_accounts):
        
        if account['email'] not in  accounts_know_ls:
            
            accounts_know_new.append(account )
    
    # move file
    accounts_old_file = file_operations(folder='input_data', file_name='accounts_type', extension='', content=accounts_know_new, date='')

    accounts_old_file.move_files()
    
    
    # Save to a file
    accounts_new_file = file_operations(folder='input_data', file_name='accounts_type', extension='', content=accounts_know_new, date='')
    
    accounts_know_new = accounts_new_file.save_to_json()
                

    # 2) Scan for new accounts and append the new ones. List with News APIaccounts accounts
    # {'app_id': '', 'plan_name': '',  'application_product': 'RADAR' or 'NEWS API'}
    
    applications_file = file_operations(folder='data_retrieved', file_name='applications', extension='', content='', date='')
    
    applications= applications_file.load_json()
    
    listed_apps = []
    for app in applications:

        if isinstance(app['plan'], dict):
            
            if 'radar' in str(app['plan']['name']).lower():
            
                listed_apps.append({'application_id': app['application_id'], 'plan_name': app['plan']['name'],  'application_product': 'RADAR'})
                
            else:
                listed_apps.append({'application_id': app['application_id'], 'plan_name': app['plan']['name'],  'application_product': 'News API'})
        
    
    #load the file with know accounts
    applications_know_file = file_operations(folder='input_data', file_name='applications_product_type', extension='', content='', date='')
    
    applications_know = applications_know_file.load_json()
    
    #### check accounts haven't applications_know detected and append it:
        
    applications_know_list = [app_id['application_id'] for app_id in applications_know]
    
    applications_know_new=applications_know
    for app in tqdm(listed_apps):
        
        if app['application_id'] not in applications_know_list:
            
            applications_know_new.append(app )
    
    # move file
    accounts_old_file = file_operations(folder='input_data', file_name='applications_product_type', extension='', content=accounts_know_new, date='')

    accounts_old_file.move_files()
    
    
    # Save to a file
    applications_new_file = file_operations(folder='input_data', file_name='applications_product_type', extension='', content=applications_know_new, date='')
    
    applications_know_new = applications_new_file.save_to_json()
                
    
if __name__ == "__main__":
    main()