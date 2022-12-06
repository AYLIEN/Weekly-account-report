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
  def __init__(self, api_base_url="", api_url_path="", access_token='', service_id=""):
      self.api_base_url=api_base_url,
      self.api_url_path= api_url_path,
      self.access_token = access_token
      self.service_id = service_id

  def call_features_per_serviceid(self):
      
      
      path =self.api_url_path[0].replace('service_id', self.service_id)
      
      url = self.api_base_url[0] + path
      
      params = {'access_token': self.access_token}
      
      
      
      response_raw = requests.get(url=url, params=params)
      
      if response_raw.status_code == 200:
          response = response_raw.content
          response = response.decode('utf8')
          response = dict(xmltodict.parse(response))
          response = json.loads(json.dumps(response))
          
          return response


class file_operations:
  def __init__(self, folder, file_name, extension, content, date=''):
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
      #2022/11/14 - removed the * (wildcard) before the file name to avoid mismatch with stories_volumes and total_stories volumes
      #There will be 2 files named with *stories*. One for the stories consumed on stories endpoint and other for the total of stories consumed accross all endpoints.
      file = dir_path + '/' + self.folder + '/'  + self.file_name + '*'
     
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
    
    def get_now_timestamp():
        
        now = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        return now
