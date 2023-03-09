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
import sys

# Reload from disk.
## if True it will hit the API, if false it will relaod from the disk.
hit_api = {'accounts': False,
           'applications': False,
           'hits': True,
           'stories_volumes': True,
           'autocompletes_volumes': True,
           'coverages_volumes': True,
           'related_stories_volumes': True,
           'time_series_volumes': True, 
           'trends_volumes': True,
           'histograms_volumes': True,
           'clusters_volumes': True,
           'total_stories_volumes': True,
           'features': True,
           'limits': True}

# delta = 16
# search_period_type ="delta"
# # search_period_type ="date"

# date_since= '2022-11-05 00:00:00'
# date_until= '2022-11-20 23:59:59'

class collector:
  def __init__(self, access_token, api_base_url='', api_url_path='', max_per_page=1, page=1):
      self.access_token = access_token
      self.api_base_url = api_base_url
      self.api_url_path = api_url_path
      self.max_per_page = max_per_page
      self.page = page

  def call_accounts_endpoint(self):
      
      total_pages = 0
      
      params = {'access_token': self.access_token,
                'page': self.page,
                'per_page': self.max_per_page}
      
      url = self.api_base_url + self.api_url_path
      
      listed_itens =[]
      
      def interact_itens(itens_response):
          for item in itens_response:
              temp_item = copy.deepcopy(item)
              listed_itens.append(temp_item)
      
      while params['page'] <= total_pages or params['page'] == 1:
          response_raw = requests.get(url=url, params=params)
          response = response_raw.content
          response = response.decode('utf8')
          response = dict(xmltodict.parse(response))
          response = json.loads(json.dumps(response))

          if response_raw.status_code == 200:
              
              # First if will capture the total of pages to loop through 
              if int(response['accounts']['@current_page']) == 1:
                  # Captures the the total of pages at the first interaction.
                  total_pages = int(response['accounts']['@total_pages'])
                  
                  # Call the function that loop through each iten
                  interact_itens(response['accounts']['account'])
                  
                  
              # From second page ownards the number of pages is already know and will be incremental
              elif int(response['accounts']['@current_page']) > 1:

                 # Call the function that loop through each item
                interact_itens(response['accounts']['account'])    
                
          pprint('Retrieving page %s from a total of %s page(s)' %(str(params['page']), str(total_pages)))
          params['page'] += 1
          
      return listed_itens

  def call_applications_endpoint(self):
      
      total_pages = 0
      
      params = {'access_token': self.access_token,
                'page': self.page,
                'per_page': self.max_per_page}
      
      url = self.api_base_url + self.api_url_path
      
      listed_itens =[]
      
      def interact_itens(itens_response):
          for item in itens_response:
              temp_item = copy.deepcopy(item)
              listed_itens.append(temp_item)
      
      while params['page'] <= total_pages or params['page'] == 1:
          response_raw = requests.get(url=url, params=params)
          response = response_raw.content
          response = response.decode('utf8')
          response = dict(xmltodict.parse(response))
          response = json.loads(json.dumps(response))

          if response_raw.status_code == 200:

              # First if will capture the total of pages to loop through 
              if int(response['applications']['@current_page']) == 1:
                  # Captures the the total of pages at the first interaction.
                  total_pages = int(response['applications']['@total_pages'])
                  
                  # Call the function that loop through each item
                  interact_itens(response['applications']['application'])
                  
               # From second page ownards the number of pages is already know and will be incremental
              elif int(response['applications']['@current_page']) > 1:
                   # Call the function that loop through each item
                   interact_itens(response['applications']['application'])    
                
          pprint('Retrieving page %s from a total of %s page(s)' %(str(params['page']), str(total_pages)))
          params['page'] += 1
          
      return listed_itens

class analytics:
  def __init__(self, formats="json", access_token='', api_base_url='', api_url_path='', application_id='', metric_name='', since='', until='', granuraty='', timezone='', skip_change="true"):
      self.formats = formats
      self.access_token = access_token
      self.api_base_url = api_base_url
      self.api_url_path = api_url_path
      self.application_id = application_id
      self.metric_name = metric_name
      self.until = until
      self.since = since
      self.granuraty = granuraty
      self.timezone = timezone
      self.skip_change = skip_change

  def call_analytics_endpoint(self):
      
      
      path =self.api_url_path.replace('app_id', self.application_id)
      
      url = self.api_base_url + path
      
      params = {'access_token': self.access_token,
                'metric_name': self.metric_name,
                'since': self.since,
                'until': self.until,
                'granularity': self.granuraty,
                'timezone': self.timezone,
                'skip_change': self.skip_change}
      
      
      
      response_raw = requests.get(url=url, params=params)
      
      if response_raw.status_code == 200:
      
          response = response_raw.json()
          
          
          return response

class features:
  def __init__(self, api_base_url="", api_url_path="", access_token='', application_plan_id=""):
      self.api_base_url=api_base_url,
      self.api_url_path= api_url_path,
      self.access_token = access_token
      self.application_plan_id = application_plan_id

  def call_features_per_app(self):
      
      
      path =self.api_url_path[0].replace('application_plan_id', self.application_plan_id)
      
      url = self.api_base_url[0] + path
      
      params = {'access_token': self.access_token}
      
      
      
      response_raw = requests.get(url=url, params=params)
      
      if response_raw.status_code == 200:
          response = response_raw.content
          response = response.decode('utf8')
          response = dict(xmltodict.parse(response))
          response = json.loads(json.dumps(response))
          
          response['application_plan_id'] = self.application_plan_id
          
          return response
      
  def call_limits_per_app(self):
      
      
      path =self.api_url_path[0].replace('application_plan_id', self.application_plan_id)
      
      url = self.api_base_url[0] + path
      
      params = {'access_token': self.access_token,
                'page': 1,
                'per_page': 50}
      
      
      
      response_raw = requests.get(url=url, params=params)
      
      if response_raw.status_code == 200:
          response = response_raw.content
          response = response.decode('utf8')
          response = dict(xmltodict.parse(response))
          response = json.loads(json.dumps(response))
          
          response['application_plan_id'] = self.application_plan_id
          
          return response


class file_operations:
  def __init__(self, folder, file_name, date, extension, content):
      self.folder = folder
      self.file_name = file_name
      self.date = str(datetime.now().strftime('%Y%m%d_%H%M%S'))
      self.extension = extension
      self.content = content
      
  def save_to_json(self):
      
      dir_path = os.getcwd()
      
      pathanme = dir_path + '/' + self.folder + '/' + self.file_name + '_' + self.date + self.extension   
      
      with open(pathanme, 'w') as fp:
          json.dump(self.content, fp)
          
      fp.close()
    
  def load_json(self):
      dir_path = os.getcwd()
      file = dir_path + '/' + self.folder + '/' + '*' + self.file_name + '*'
      
      files = glob.glob(file)

      f = open(files[0])
      
      data = json.load(f)
      
      f.close()
      
      return data
      
          
  def check_file_exists(self):
      
     dir_path = os.getcwd() 
     file = dir_path + '/' + self.folder + '/' + '*' + self.file_name + '*'
     
     files = glob.glob(file)
     
     if len(files) > 0:
         return True
     else:
         return False
     
  def move_files(self):
      
     dir_path = os.getcwd() 
     file = dir_path + '/' + self.folder + '/' + '*' + self.file_name + '*'

     
     files = glob.glob(file)
     
     for file in files:
         file_name = file.split("/")[-1]
         src = dir_path + '/' + self.folder + '/' + file_name
         dst = dir_path + '/' + self.folder + '/' + 'archive/' + file_name
         
         shutil.move(src,dst)

  def delete_files(self):
        
     dir_path = os.getcwd() 
     file = dir_path + '/' + self.folder + '/' + '*' + self.file_name + '*'
    
       
     files = glob.glob(file)
       
     for file in files:
         file_name = file.split("/")[-1]
         file_path_name = dir_path + '/' + self.folder + '/' + file_name

         os.remove(file_path_name)

      
     

class date_operations:
    def get_since_date(today, number_days):
        
        since =  today - timedelta(days=number_days)
        since = since.replace(second=0, microsecond=0, minute=0, hour=0)
        since = since.strftime('%Y-%m-%d %H:%M:%S')
        
        return since
        
    def get_until_date(today, number_days=1):
        
        until = today  - timedelta(days=number_days)
        until = until.replace(second=59, microsecond=59, minute=59, hour=23)
        until = until.strftime('%Y-%m-%d %H:%M:%S')
        
        return until
    
    def get_delta(date):
        
        
        date = datetime.strptime(date, '%Y-%m-%dT%H:%M:%SZ')
        today = datetime.today()
        
        delta = today - date
        
        return delta.days


def main(search_period_type_input="delta", delta_input=16,  date_since_input='2099-11-01 00:00:00', date_until_input='2099-11-15 23:59:59'):

    # Sample command line
    # 'delta', 16 
    # 'date', '2022-11-05 00:00:00', '2022-11-20 23:59:59'

    if search_period_type_input == "delta":
        search_period_type ="delta"
        delta = delta_input

    elif search_period_type_input == "date":
        search_period_type ="date"
        date_since = date_since_input
        date_until = date_until_input
        delta = 0
        
    
    if search_period_type == 'delta':
        # Date pattern returned '2022-11-01 00:00:00'
        date_since= date_operations.get_since_date(datetime.now(), delta)
        # Date pattern returned '2022-11-15 23:59:59'
        date_until= date_operations.get_until_date(datetime.now(), 1)  
        
    elif search_period_type == 'date':
        # [] Build a function to validate the date foramt
        date_since= date_since_input
        date_until= date_until_input

    pprint(date_since)
    pprint(date_until)
    
    # Pull data from accounts: 
    accounts_file = file_operations('data_retrieved', 'accounts', '', '', '')
        
    # check = accounts_file.check_file_exists()

    if hit_api.get('accounts') is True:
        
        print("#################################")
        print("#### Retrieving all accounts ####")
        print("#################################")
    
        accounts_file = file_operations('data_retrieved', 'accounts', '', '', '')
        
        accounts_file.move_files()
        
        accounts_endpoint = collector('217a5119fec38a58befee4681edaf321653088a4a3b8638b26cb3e429885bfda',
                             'https://aylien-news-api-admin.3scale.net',
                             '/admin/api/accounts.xml',
                             500)
        
        accounts = accounts_endpoint.call_accounts_endpoint()
        
        if accounts is not None:
        
            accounts_file = file_operations('data_retrieved', 'accounts', '',  '.json', accounts )
            
            accounts_file.save_to_json()
    
    
    # Pull data from applications:

    applications_file = file_operations('data_retrieved', 'applications', '', '', '')
    
    # check = applications_file.check_file_exists()
    
    if hit_api.get('applications') is True:
        
        print("#####################################")
        print("#### Retrieving all applications ####")
        print("#####################################")
        
        applications_file = file_operations('data_retrieved', 'applications', '', '', '')
        
        applications_file.move_files()
        
        applications_endpoint = collector('217a5119fec38a58befee4681edaf321653088a4a3b8638b26cb3e429885bfda',
                             'https://aylien-news-api-admin.3scale.net',
                             '/admin/api/applications.xml',
                             500)
    
        applications = applications_endpoint.call_applications_endpoint()
        
        if applications is not None:
    
            applications_file = file_operations('data_retrieved', 'applications', '',  '.json', applications )
            
            applications_file.save_to_json()
        
        
    # Pull data from invoices:
    # [ ] For future 
    
    # Pull analytics:
    anlytic_endpoints=[]
    for i in hit_api:
        if i not in ['accounts', 'applications', 'features']:
            anlytic_endpoints.append(hit_api.get(i))
    
    if any(anlytic_endpoints):
        
        print("##############################################")
        print("#### Retrieving all anallytics per APP ID ####")
        print("##############################################")
        print()
        print()

        
        applications_file = file_operations('data_retrieved', 'applications', '',  '.json', '' )
        applications = applications_file.load_json()

        ### For IBM ww will retrieve a specific set of APP IDs, no need to load from file.
        ## List of IBM applications copied from 3Scale
        IBM_applications = ['433a6774',
                            '930f4c51',
                            '60eaa1cb',
                            '1681e6c4',
                            '9fc0e0dd']
        
        ## Loop through apps and leave behind only the ones from IBM
        apps_temp =[]
        for app in applications:
            if str(app['application_id']) in str(IBM_applications):
                apps_temp.append(app)

        applications = apps_temp
        pprint("Total apps to be retrieved:")
        pprint(len(applications))
        
        endpoint_analytics_metrics = ['hits',
                                      'stories_volumes',
                                      'autocompletes_volumes',
                                      'coverages_volumes',
                                      'related_stories_volumes',
                                      'time_series_volumes', 
                                      'trends_volumes',
                                      'histograms_volumes',
                                      'clusters_volumes',
                                      'total_stories_volumes']        
        
        for metric in endpoint_analytics_metrics:
            
            if hit_api.get(metric) is True:
            
                message = "Retrive metrics for endpoint %s." %(metric)
                print("#"*len(message))
                print(message)
                print("#"*len(message))
                
                analytics_file = file_operations('data_retrieved', metric, '', '', '')
        
                analytics_file.move_files() 
                
                total_anl = []
                
                for app in tqdm(applications):
                    
                    modified_delta = date_operations.get_delta(app['updated_at']) 
                    
                    if app['state'] != 'suspended' or (app['state'] == 'suspended' and modified_delta <= delta +1):
            
                        analytics_file = analytics(formats='json',
                                                    access_token= '217a5119fec38a58befee4681edaf321653088a4a3b8638b26cb3e429885bfda',
                                                    api_base_url='https://aylien-news-api-admin.3scale.net',
                                                    api_url_path= '/stats/applications/app_id/usage.json',
                                                    application_id = app['id'],
                                                    metric_name= metric,
                                                    since= date_since,
                                                    until= date_until,
                                                    granuraty='day',
                                                    timezone='london',
                                                    skip_change='true'
                                                   
                                                    ) 
                    
                        data_anl = analytics_file.call_analytics_endpoint()
                        
                        if data_anl is not None:                    
                            total_anl.append(data_anl)
                        
                metrics_file = file_operations('data_retrieved', metric, '',  '.json', total_anl )
            
                metrics_file.save_to_json()


    #####
    # Retrieve Feature for each application    
    if hit_api.get('features') is True:
        
        print("############################################")
        print("#### Retrieving all features per APP ID ####")
        print("############################################")
        
        features_file = file_operations('data_retrieved', 'feature_flags', '', '', '')
    
        features_file.move_files()
        
        applications_file = file_operations('data_retrieved', 'applications', '',  '.json', '' )
        
        applications = applications_file.load_json()

        ### For IBM ww will retrieve a specific set of APP IDs, no need to load from file.
        ## List of IBM applications copied from 3Scale
        IBM_applications = ['433a6774',
                            '930f4c51',
                            '60eaa1cb',
                            '1681e6c4',
                            '9fc0e0dd']
        
        ## Loop through apps and leave behind only the ones from IBM
        apps_temp =[]
        for app in applications:
            if str(app['application_id']) in str(IBM_applications):
                apps_temp.append(app)

        applications = apps_temp
        pprint("Total apps to be retrieved:")
        pprint(len(applications))
       
        application_plan_ids = []
        for app in applications:
            application_plan_ids.append( {'application_plan_id':app['plan']['id'], 'updated_at':app['updated_at'], 'state':app['state']  })
            
        #Unique app pan ids
        application_plan_ids= list({v['application_plan_id']:v for v in application_plan_ids}.values())    
        
        total_features = []
        
        for app in tqdm(application_plan_ids):
            
            modified_delta = date_operations.get_delta(app['updated_at']) 
                
            # if app['state'] != 'suspended' or (app['state'] == 'suspended' and modified_delta <= delta+1):
            
            feature_file = features(access_token= '217a5119fec38a58befee4681edaf321653088a4a3b8638b26cb3e429885bfda',

                                     api_base_url='https://aylien-news-api-admin.3scale.net',
                                     api_url_path= "/admin/api/application_plans/application_plan_id/features.xml",
                                     application_plan_id=app['application_plan_id'] ) 
            
            data_features = feature_file.call_features_per_app()
            
            if data_features is not None:
                total_features.append(data_features)
                
                
        features_file = file_operations('data_retrieved', 'feature_flags', '',  '.json', total_features )
                
        features_file.save_to_json()
        
    ###############################################################################################################
    # Retrieve Limits for each application    
    if hit_api.get('limits') is True:
        
        print("##########################################")
        print("#### Retrieving all Limits per APP ID ####")
        print("##########################################")
        
        # Find and move to archive and previous file that mathc the name
        features_file = file_operations('data_retrieved', 'limits', '', '', '')
        features_file.move_files()
        
        # Load the file with all the applications
        applications_file = file_operations('data_retrieved', 'applications', '',  '.json', '' )
        applications = applications_file.load_json()

        ### For IBM ww will retrieve a specific set of APP IDs, no need to load from file.
        ## List of IBM applications copied from 3Scale
        IBM_applications = ['433a6774',
                            '930f4c51',
                            '60eaa1cb',
                            '1681e6c4',
                            '9fc0e0dd']
        
        ## Loop through apps and leave behind only the ones from IBM
        apps_temp =[]
        for app in applications:
            if str(app['application_id']) in str(IBM_applications):
                apps_temp.append(app)

        applications = apps_temp
        pprint("Total apps to be retrieved:")
        pprint(len(applications))
        
        # List all applications and relevant metadata
        application_plan_ids = []
        for app in applications:
            application_plan_ids.append( {'application_plan_id':app['plan']['id'], 'updated_at':app['updated_at'], 'state':app['state']  })
            
        #Unique app pan ids
        application_plan_ids= list({v['application_plan_id']:v for v in application_plan_ids}.values())    
        
        total_limits = []
        
        for app in tqdm(application_plan_ids):
            
            modified_delta = date_operations.get_delta(app['updated_at']) 
                
            
            # if app['state'] != 'suspended' or (app['state'] == 'suspended' and modified_delta <= delta+120):
            
            feature_file = features(access_token= '217a5119fec38a58befee4681edaf321653088a4a3b8638b26cb3e429885bfda',

                                     api_base_url='https://aylien-news-api-admin.3scale.net',
                                     api_url_path= "/admin/api/application_plans/application_plan_id/limits.xml",
                                     application_plan_id=app['application_plan_id'] ) 
            
            data_limits = feature_file.call_features_per_app()
            
            if data_limits is not None:
                total_limits.append(data_limits)
                
                
        limits_file = file_operations('data_retrieved', 'limits', '',  '.json', total_limits )
        limits_file.save_to_json()
        


if __name__ == "__main__":
    main()
