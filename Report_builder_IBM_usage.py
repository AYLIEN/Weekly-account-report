from module import file_operations, date_operations
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
import pandas as pd
import numpy as np 
from decimal import Decimal
import xlsxwriter
import math
    
pd.set_option('display.max_columns', None)
pd.set_option('max_colwidth', 400)

def main(search_period_type_input="delta"):
    
    # Load accounts
    accounts_file = file_operations(folder='data_retrieved', file_name='accounts', extension='', content='', date='')
    
    accounts = accounts_file.load_json()
    
    accounts_df = pd.DataFrame(accounts) 
    
    ## normalize users
    accounts_user = []
    for account in accounts:
        
        if isinstance(account['users']['user'], list):
            for user in account['users']['user']:
                user['account_id'] = account['id']
                accounts_user.append(user)

            
        elif isinstance(account['users']['user'], dict):
            account['users']['user']['account_id'] = account['id']
            accounts_user.append(account['users']['user'])
            

    accounts_user_df = pd.json_normalize(accounts_user).add_prefix('user_')
    
    # Merge normalized users child
    accounts_df = accounts_df.merge(accounts_user_df, left_on='id', right_on='user_account_id')
    accounts_df = accounts_df.add_prefix('account_')
    
    
    # Load applications
    applications_file = file_operations(folder='data_retrieved', file_name='applications', extension='', content='', date='')
    
    applications = applications_file.load_json()
    
    applications_df = pd.DataFrame(applications)
 
    ## normalize plan    
    applications_plan = []
    
    for app in applications:
        
        if 'plan' in app.keys():
            app['plan']['application_id'] = app['application_id']
            applications_plan.append(app['plan'])

    applications_plan_df = pd.json_normalize(applications_plan).add_prefix('plan_')
    
    # Merge normalized plan child
    applications_df = applications_df.merge(applications_plan_df, left_on='application_id', right_on='plan_application_id')
    applications_df = applications_df.add_prefix('application_')
    
    
    #####
    # Merger accounts and aplications into report
    report = accounts_df.merge(applications_df, left_on='account_id', right_on='application_user_account_id')    
    


    #####
    # flag if internal account
    accounts_type_file = file_operations(folder='input_data', file_name='accounts_type', extension='', content='', date='')
    
    accounts_type = accounts_type_file.load_json()
    
    accounts_type_df = pd.DataFrame(accounts_type)
    
    #Merge account type
    report = report.merge(accounts_type_df, how='left', left_on=['account_id', 'account_user_email'], right_on=['account_id', 'email'])
    
    
    # Flag if account_product (RADAR or NEWS API)
    applications_product_type_file = file_operations(folder='input_data', file_name='applications_product_type', extension='', content='', date='')
    
    applications_product_type = applications_product_type_file.load_json()
    
    applications_product_type_df = pd.DataFrame(applications_product_type)
    
    #Merge product type
    report = report.merge(applications_product_type_df, how='left', left_on=['application_application_id'], right_on=['application_id'])
    
    
    ######
    # Flag new accounts
    
    
    ######
    # Tag with dates range for analytics
    analytics_date_range_file = file_operations(folder='input_data', file_name='dates_searched', extension='', content='', date='')
    analytics_date_range = analytics_date_range_file.load_json()
    
    report['analytics_start_datetime'] = analytics_date_range[0]['analytics_start_datetime']
    report['analytics_end_datetime'] = analytics_date_range[0]['analytics_end_datetime']
    
    
    ######
    # Merge analytics data
    
    endpoint_analytics_metrics = ['hits',
                                  'autocompletes_volumes',
                                  'coverages_volumes',
                                  'related_stories_volumes',
                                  'time_series_volumes', 
                                  'trends_volumes',
                                  'histograms_volumes',
                                  'clusters_volumes',
                                  'stories_volumes',
                                  'total_stories_volumes'] 
    
    for metric in endpoint_analytics_metrics:
        metric_file = file_operations(folder= 'data_retrieved', file_name= metric, extension='', content='', date='')
        metric_data = metric_file.load_json()
               
        metric_data_df = pd.json_normalize(metric_data, max_level=2)

        
        metric_data_df.head(2)
        report['analytics_start_datetime'] = str(metric_data_df['period.since'].unique()[0])
        report['analytics_end_datetime'] = str(metric_data_df['period.until'].unique()[0])
        
        metric_data_df['application.id'] = metric_data_df['application.id'].astype(str)
        
        ##
        # merge
        report = report.merge(metric_data_df[["application.id","total"]]   , how='left', left_on=['application_id_x'], right_on=['application.id']).drop(columns = ["application.id"])
        report = report.rename(columns={"total": str(metric_data_df['metric.name'].unique()[0]) })
        
        
    
    ##########
    # Fill with Zeroes
    # This function defnine the column name based on unique values from metric['name']
    # If there is more than one value it can became a problem        
    report['Hits'] = report['Hits'].fillna(0)
    report['Related Stories Volumes'] = report['Related Stories Volumes'].fillna(0)
    report['Autocompletes Volumes'] = report['Autocompletes Volumes'].fillna(0)
    report['Coverages Volumes'] = report['Coverages Volumes'].fillna(0)
    report['Time Series Volumes'] = report['Time Series Volumes'].fillna(0)
    report['Trends Volumes'] = report['Trends Volumes'].fillna(0)
    report['Histograms Volumes'] = report['Histograms Volumes'].fillna(0)
    report['Clusters Volumes'] = report['Clusters Volumes'].fillna(0)
    report['Stories Volumes'] = report['Stories Volumes'].fillna(0)
    report['Total Stories Volumes'] = report['Total Stories Volumes'].fillna(0)    
    
    
    ##########################################################################################3
    # As dicussed with Omar I'll put these filds in here as placeholders until I find a way to  collect this data
    report['Autocompletes Hits'] = 'TBD'
    report['Autocompletes Hits limit'] = 'TBD'
    report['Autocompletes Hits limit period'] = 'TBD'
    report['Autocompletes Hits limit used (%)'] = 'TBD'
    # report['Autocompletes Volume'] = 'TBD'
    report['Autocompletes Volume limit'] = 'TBD'
    report['Autocompletes Volume limit period'] = 'TBD'
    report['Autocompletes Volume limit used (%)'] = 'TBD'
    report['Clusters Hits'] = 'TBD'
    report['Clusters Hits limit'] = 'TBD'
    report['Clusters Hits limit period'] = 'TBD'
    report['Clusters Hits limit used (%)'] = 'TBD'
    # report['Clusters Volume'] = 'TBD'
    report['Clusters Volume limit'] = 'TBD'
    report['Clusters Volume limit period'] = 'TBD'
    report['Clusters Volume limit used (%)'] = 'TBD'
    report['Coverages Hits'] = 'TBD'
    report['Coverages Hits limit'] = 'TBD'
    report['Coverages Hits limit period'] = 'TBD'
    report['Coverages Hits limit used (%)'] = 'TBD'
    # report['Coverages Volume'] = 'TBD'
    report['Coverages Volume limit'] = 'TBD'
    report['Coverages Volume limit period'] = 'TBD'
    report['Coverages Volume limit used (%)'] = 'TBD'
    report['Related Stories Hits'] = 'TBD'
    report['Related Stories Hits limit'] = 'TBD'
    report['Related Stories Hits limit period'] = 'TBD'
    report['Related Stories Hits limit used (%)'] = 'TBD'
    # report['Related Stories Volume'] = 'TBD'
    report['Related Stories Volume limit'] = 'TBD'
    report['Related Stories Volume limit period'] = 'TBD'
    report['Related Stories Volume limit used (%)'] = 'TBD'
    report['Stories Hits'] = 'TBD'
    report['Stories Hits limit'] = 'TBD'
    report['Stories Hits limit period'] = 'TBD'
    report['Stories Hits limit used (%)'] = 'TBD'
    # report['Stories Volume'] = 'TBD'
    report['Stories Volume limit'] = 'TBD'
    report['Stories Volume limit period'] = 'TBD'
    report['Stories Volume limit used (%)'] = 'TBD'
    report['Time Series Hits'] = 'TBD'
    report['Time Series Hits limit'] = 'TBD'
    report['Time Series Hits limit period'] = 'TBD'
    report['Time Series Hits limit used (%)'] = 'TBD'
    # report['Time Series Volume'] = 'TBD'
    report['Time Series Volume limit'] = 'TBD'
    report['Time Series Volume limit period'] = 'TBD'
    report['Time Series Volume limit used (%)'] = 'TBD'
    report['Trends Hits'] = 'TBD'
    report['Trends Hits limit'] = 'TBD'
    report['Trends Hits limit period'] = 'TBD'
    report['Trends Hits limit used (%)'] = 'TBD'
    # report['Trends Volume'] = 'TBD'
    report['Trends Volume limit'] = 'TBD'
    report['Trends Volume limit period'] = 'TBD'
    report['Trends Volume limit used (%)'] = 'TBD'
    
    
    
    #### 
    # add feature information
    feature_flags_file = file_operations(folder='data_retrieved', file_name='feature_flags', extension='', content='', date='')
     
    feature_flags = feature_flags_file.load_json()    
    
    flags_per_app = []    
    for app_id in feature_flags:
        temp_rec={'application_plan_id': app_id['application_plan_id']}
        
        for flag in app_id['features']['feature']:
            # Features enabled will appear as true. Features not enabled won't be on this metadata
            if flag['visible'] == 'true':
                temp_rec[flag['system_name']] = True
                
            elif flag['visible'] == 'false':
                temp_rec[flag['system_name']] = False
        
        flags_per_app.append(temp_rec)
            
    flags_per_app_df = pd.DataFrame(flags_per_app)
    flags_per_app_df = flags_per_app_df.fillna(value=False)
    flags_per_app_df = flags_per_app_df.add_prefix('flag_')

    

    
    report = report.merge(flags_per_app_df, how='left', left_on=['application_plan_id'], right_on=['flag_application_plan_id'])
    
    ################################################################################################################################################################### 
    # add limits information
    limits_file = file_operations(folder='data_retrieved', file_name='limits', extension='', content='', date='')
    limits_data = limits_file.load_json()    
    
    limits_per_app = []    
    for lims in limits_data:
        if lims['limits'] is not None:
            for each in lims['limits']['limit']:
                if type(lims['limits']['limit']) is list:
                    limits_per_app.append(each)
                if type(lims['limits']['limit']) is dict:
                    limits_per_app.append(lims['limits']['limit'])
                
                
    limits_per_app_filter = []             
    for each in limits_per_app:
        if each['metric_id'] == '2555418267794': 
            limits_per_app_filter.append(each)
            
    
    limits_data_df = pd.DataFrame(limits_per_app_filter)
    limits_data_df = limits_data_df[['plan_id', 'period', 'value']]
    limits_data_df = limits_data_df.add_prefix('limit_')

    # pprint(limits_data_df.head(10))
     
    report = report.merge(limits_data_df, how='left', left_on=['application_plan_id'], right_on=['limit_plan_id'])    
    ###################################################################################################################################################################
    
    ####
    # Get domain    
    domains = []
    for index, row in report.iterrows():

        domain = row['account_user_email'].split('@')
        
        if len(domain) > 1:
            
            domains.append( str( domain[-1]) )
            
        else:
            domains.append( str( domain[-1]) )
            
    report['domain'] = domains
    
        
    ####
    # Get the latest date of the range to claculate the age back in the time
    if search_period_type_input=='date':
        for index, row in report.iterrows():
            date_until_report = []
            date_until_report.append(row['analytics_end_datetime'])
            date_until_report = datetime.strptime(max(date_until_report), '%Y-%m-%dT%H:%M:%SZ')
    
    
    # application days old
    #### Remove useless columns
    application_days_old = []
    for index, row in report.iterrows():

        crreated_at = datetime.strptime(row['application_created_at'], '%Y-%m-%dT%H:%M:%SZ')
        
        now = datetime.now()
        
        
        if  search_period_type_input=='delta':
            delta_creation = now - crreated_at        
            delta_creation = delta_creation.days
            
        elif search_period_type_input=='date':
            delta_creation = date_until_report - crreated_at 
            delta_creation = delta_creation.days
            
            
        
        if type(delta_creation) is int and delta_creation> 0:
            application_days_old.append(delta_creation)
        
        else:
            application_days_old.append(0)  
            
    # old name: Application_days_old
    report['Application days since created'] = application_days_old
    
    ####
    # Flag new account
    new_application = []
    for index, row in report.iterrows():

        if row['Application days since created'] <= 14:
            new_application.append(True)
            
        else:
            new_application.append(False)
    #  old name: new_application 
    report['Application created in the last 15 days?'] = new_application
    
    #####
    # Average hits per day
    average_hits_pe_day_calc = []
    for index, row in report.iterrows():
        if row['Application days since created'] == 0:
            average_hits_pe_day_calc.append( row['Hits']/1  )
            
        if row['Application days since created'] >= 1:
            average_hits_pe_day_calc.append( row['Hits']/row['Application days since created'] )
            
    report['average_hits_pe_day'] = average_hits_pe_day_calc
        
    # report['average_hits_pe_day'] = report['Hits']/report['Application days since created']
    
    # Round numbers up
    # Fix the issue with accounts with only one hit a day
    report['average_hits_pe_day'] = report['average_hits_pe_day'].fillna(0)
    average_hits_pe_day = []
    
    for index, row in report.iterrows():
        
        try:                
            average_hits_pe_day.append(math.ceil(row['average_hits_pe_day']))
        except:
            pprint(row['application_application_id'])
            pprint(row['Hits'])
            pprint(row['Application days since created'])
            pprint(row['average_hits_pe_day'])
            # Case it fails in the try except loop it will have 0
            average_hits_pe_day.append(0)
        
    report['average_hits_pe_day'] = average_hits_pe_day
    

    
    
    ####
    # Delete and rename columns
    #### Remove useless columns
    
    # Rename the columns with period
    min_date = []
    max_date = []
    for index, row in report.iterrows():
        
        min_date.append(row['analytics_start_datetime'])
        max_date.append(row['analytics_end_datetime'])
   
    min_date_pe = datetime.strptime(min(min_date, default='5999-10-13T23:59:59Z'), '%Y-%m-%dT%H:%M:%SZ')
    max_date_pe = datetime.strptime(max(max_date, default='1501-10-13T23:59:59Z'), '%Y-%m-%dT%H:%M:%SZ')
    
    delta = max_date_pe - min_date_pe
    delta = delta.days
    
    day_str = "days"
    if delta >1:
        day_str = "days"
    if delta <= 1:
        day_str = "day"
    
    report['Usage Data Duration (Days)'] = str(delta) + ' ' + day_str
    
    #2022/11/14 - 1148 - Removed the column names with "last 99 days" pattern. Instead, I added a column with the delta (named as 'Hits and volumes collect delta') mesauring the days between "from" and "to"
    # report = report.rename(columns={"Hits": f"Hits last {delta} {day_str}",
    #                        "Autocompletes Volumes": f"Autocompletes Volumes last  {delta} {day_str}",
    #                        "Coverages Volumes": f"Coverages Volumes last  {delta} {day_str}",
    #                        "Related Stories Volumes": f"Related Stories Volumes last  {delta} {day_str}",
    #                        "Time Series Volumes": f"Time Series Volumes last  {delta} {day_str}",
    #                        "Trends Volumes": f"Trends Volumes last  {delta} {day_str}",
    #                        "Histograms Volumes": f"Histograms Volumes last  {delta} {day_str}",
    #                        "Clusters Volumes": f"Clusters Volumes last  {delta} {day_str}",
    #                        "Stories Volumes": f"Stories Volumes last  {delta} {day_str}",
    #                        "Total Stories Volumes": f"Total Stories Volumes last {delta} {day_str}"          
    #                       })
    
    #Renaming the column "Hits"
    report = report.rename(columns={"Hits": "Total Hits"})

    ########################
    ### Sort Values ########
    ########################
    
    report = report.sort_values(by='account_created_at', ascending=False)

    # renaming other fields
    report = report.rename(columns={'account_id' : 'Account ID',
                                    'account_created_at' : 'Account created (Date)',
                                    'account_updated_at' : 'Account last updated (Date)',
                                    'account_state' : 'Account state',
                                    'account_org_name' : 'Account company name',
                                    'account_monthly_billing_enabled' : 'Account monthly billing enabled',
                                    'account_monthly_charging_enabled' : 'account monthly charging enabled',
                                    'account_credit_card_stored' : 'Account credit card stored',
                                    'account_user_state' : 'Account user state',
                                    'account_user_role' : 'Account user role',
                                    'account_user_email' : 'Account user email',
                                    'account_user_username' : 'Account username',
                                    'application_name': 'Application name',
                                    'application_created_at' : 'Application created (Date)',
                                    'application_updated_at' : 'Application last updated (Date)',
                                    'application_state' : 'Application state',
                                    'application_first_traffic_at' : 'Application first traffic (Date)',
                                    'application_application_id' : 'Application ID',
                                    'application_description' : 'Application description',
                                    'application_plan_@custom' : 'Application plan custom (T/F)',
                                    'application_plan_@default' : 'Application plan default (T/F)',
                                    'application_plan_name' : 'Application plan name',
                                    'account_propose' : 'Account propose',
                                    'application_product' : 'Application product',
                                    'analytics_start_datetime' : 'Hits and volumes collected from',
                                    'analytics_end_datetime' : 'Hits and volumes collected to',
                                    'domain' : 'Account email domain',
                                    'average_hits_pe_day' : 'Average hits per day', 
                                    'limit_value': 'Total Stories Volume limit',
                                    'limit_period' : 'Total Stories Volume limit period', #Old Name: 'Allowance limit period',
                                    'Total Stories Volumes': 'Total Stories Volume',
                                    ############################################################################################
                                    #  Feature flags fields                                    
                                    'flag_historic_1month' :        'FF historic_1month',
                                    'flag_historic_3month' :        'FF historic_3month',
                                    'flag_historic_unlimited' :     'FF historic_unlimited',
                                    'flag_historic_1year' :         'FF historic_1year',
                                    'flag_historic_2year' :         'FF historic_2year',
                                    'flag_historic_3year' :         'FF historic_3year',
                                    'flag_historic_4year' :         'FF historic_4year',
                                    'flag_historic_5year' :         'FF historic_5year',
                                    'flag_real_time' :              'FF real_time',
                                    'flag_lang_en' :                'FF lang_en',
                                    'flag_lang_pt' :                'FF lang_pt',
                                    'flag_lang_es' :                'FF lang_es',
                                    'flag_lang_it' :                'FF lang_it',
                                    'flag_lang_de' :                'FF lang_de',
                                    'flag_lang_fr' :                'FF lang_fr',
                                    'flag_lang_ar' :                'FF lang_ar',
                                    'flag_lang_da' :                'FF lang_da',
                                    'flag_lang_fi' :                'FF lang_fi',
                                    'flag_lang_nl' :                'FF lang_nl',
                                    'flag_lang_ru' :                'FF lang_ru',
                                    'flag_lang_sv' :                'FF lang_sv',
                                    'flag_lang_tr' :                'FF lang_tr',
                                    'flag_lang_zh-tw' :             'FF lang_zh-tw',
                                    'flag_lang_zh-cn' :             'FF lang_zh-cn',
                                    'flag_lang_no' :                'FF lang_no',
                                    'flag_content_pack_superfeedr' :'FF content_pack_superfeedr',
                                    'flag_content_pack_acquiremedia_not_licensed' : 'FF content_pack_acquiremedia_not_licensed',
                                    'flag_content_pack_acquiremedia_licensed' : 'FF content_pack_acquiremedia_licensed',
                                    'flag_unlimited_calls' : 'FF unlimited_calls',
                                    'flag_canary' : 'FF canary',
                                    'flag_advanced_entities_search' : 'FF advanced_entities_search',
                                    'flag_advanced_entities_search_include_elsa' : 'FF advanced_entities_search_include_elsa',
                                    'flag_lucene_query_search' : 'FF lucene_query_search',
                                    'flag_content_pack_twingly_news' : 'FF content_pack_twingly_news',
                                    'flag_tagger_industries' : 'FF tagger_industries',
                                    'flag_tagger_categories' : 'FF tagger_categories',
                                    'flag_clustering' : 'FF clustering',
                                    'flag_prominence_score_search' : 'FF prominence_score_search',
                                    'flag_content_pack_twingly_blogs' : 'FF content_pack_twingly_blogs',
                                    'flag_lang_fa' : 'FF lang_fa',
                                    'flag_published_datetime' : 'FF published_datetime',
                                    'flag_customer_ihs' : 'FF customer_ihs',
                                    'flag_new_v3_entities' : 'FF new_v3_entities',
                                    'flag_external-entity-mapping-duns' : 'FF external-entity-mapping-duns',
                                    'flag_relevance_boosting' : 'FF relevance_boosting',
                                    # Endpoints
                                    "Autocompletes Volumes": "Autocompletes Volume",
                                    "Clusters Volumes": "Clusters Volume",
                                    "Coverages Volumes": "Coverages Volume",
                                    "Related Stories Volumes": "Related Stories Volume",
                                    "Stories Volumes": "Stories Volume",
                                    "Time Series Volumes": "Time Series Volume",
                                    "Trends Volumes": "Trends Volume"

                                                               })

    
    #########################################################################################
    # As discussed with Omar removing the time from the date time stamp for columns that are in this format
    # As rediscussed with Omar on ticket https://aylien.monday.com/boards/2939156063/pulses/3762824650 - changigng the timestamp to show datetime on column 'Application created (Date)' 
    report['Account created (Date)'] = report['Account created (Date)'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    report['Account last updated (Date)'] = report['Account last updated (Date)'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    # report['Application created (Date)'] = report['Application created (Date)'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    report['Application last updated (Date)'] = report['Application last updated (Date)'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    report['Application first traffic (Date)'] = report['Application first traffic (Date)'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    report['Usage Data collected from (Timedate)'] = report['Hits and volumes collected from'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    report['Usage Data collected to (Timedate)'] = report['Hits and volumes collected to'].apply(lambda x : x.split("T")[0] if(x is not None) else None)
    # pprint (report.dtypes)
   
    #########################################################################################    
    
    #### fill the gaps
    report['Total Stories Volume limit period']= report['Total Stories Volume limit period'].replace(np.nan, 'N/A') 
    report['Total Stories Volume limit'] = report['Total Stories Volume limit'].replace(np.nan, 0) 
    report['Total Stories Volume limit']= report['Total Stories Volume limit'].astype('int')
    # report['Usage stats'] = round((report["Total Stories Volumes"] / report['Total Stories Volumes limit']) *100, 2)
    
    ###### Define percentage usage stats
    pct_calc = []
    for index, row in report.iterrows():
        if row['Total Stories Volume limit'] > 0:
            pct_calc.append(row["Total Stories Volume"] / row['Total Stories Volume limit'])
        else:
            pct_calc.append(0)
            
    report['Total Stories Volume Usage stats'] = pct_calc
    
    pct = []
    for index, row in report.iterrows():
        pct_var = Decimal(row['Total Stories Volume Usage stats'])
        pct_var = '{:,.2%}'.format(row['Total Stories Volume Usage stats'])
        pct.append(pct_var)
        
    report['Total Stories Volume Usage stats'] = pct    
    
    
    ########################################################################
    # Total Volume 
    # Sum of all endpoints volume 
    report['Total Volume'] = report["Autocompletes Volume"] + report["Coverages Volume"]+ report["Related Stories Volume"]+ report["Time Series Volume"]+ report["Trends Volume"]+  report["Clusters Volume"]+ report["Stories Volume"]
    
    
    # result = report['Usage Data Duration (Days)'].dtypes
    # pprint(result)
    
    
    # Average per day
    report['Total Volume'] = report['Total Volume'].astype('int')
    delta_int = int(delta)
    report['Total Volume per day (Average)'] = report['Total Volume'] / delta_int
    report['Total Volume'] = report['Total Volume'].astype('str')

    # Round numbers up
    # Fix the issue with accounts with only one hit a day
    report['Total Volume per day (Average)'] = report['Total Volume per day (Average)'].fillna(0)
    total_vol_per_day = []
    for index, row in report.iterrows():
        total_vol_per_day.append(math.ceil(row['Total Volume per day (Average)']))
    
    report['Total Volume per day (Average)'] = total_vol_per_day
        
    
    ##########
    ## Select columns 
    # 2023-Jan-13 - Changes based on this ticket https://aylien.monday.com/boards/2939156063/pulses/3762824650
    # []I moved the Application columns to the left of the Account columns
    # [] I moved Application Created (Date) to be column A. 
    # [] if this could include a timestamp it would be great  - full time stamp with hour, e.g. "2023-01-03 10:50:15"
    # [] I sorted the sheet by column A Application Created (Date) DESC (Z-A) so that the newest applications are on the top
    report = report[[
                    'Account user email',
                    'Application ID',
                    'Application name',
                    'Application product',
                    'Hits and volumes collected from',
                    'Hits and volumes collected to',
                    'Total Hits',
                    'Average hits per day',
                    'Total Stories Volume',
                    'Autocompletes Volume',
                    'Coverages Volume',
                    'Time Series Volume',
                    'Related Stories Volume',
                    'Clusters Volume',
                    'Trends Volume',
                    'Total Stories Volume',
                    'Total Stories Volume limit',
                    'Total Stories Volume limit period',
                    'Total Stories Volume Usage stats'                 
                     ]]

                    #####################################################
                    ## All other fields #################################
                    #####################################################
                    # 'Application created (Date)',
                    # 'Application name',
                    # 'Application days since created',
                    # 'Application created in the last 15 days?',
                    # 'Application last updated (Date)',
                    # 'Application first traffic (Date)',
                    # 'Application state',
                    # 'Application description',
                    # 'Application plan custom (T/F)',
                    # 'Application plan default (T/F)',
                    # 'Application plan name',
                    # # Account fields:
                    # 'Account ID',
                    # 'Account created (Date)',
                    # 'Account last updated (Date)',
                    # 'Account state',
                    # 'Account company name',
                    # 'Account monthly billing enabled',
                    # 'account monthly charging enabled',
                    # 'Account credit card stored',
                    # 'Account user state',
                    # 'Account user role',
                    # 'Account email domain',
                    # 'Account username',
                    # 'Account propose',
                    # # Usage fields analytics
                    # 'Usage Data collected from (Timedate)',
                    # 'Usage Data collected to (Timedate)',
                    # 'Usage Data Duration (Days)',
                    # 'Autocompletes Hits',
                    # 'Autocompletes Hits limit',
                    # 'Autocompletes Hits limit period',
                    # 'Autocompletes Hits limit used (%)',
                    # 'Autocompletes Volume',
                    # 'Autocompletes Volume limit',
                    # 'Autocompletes Volume limit period',
                    # 'Autocompletes Volume limit used (%)',
                    # 'Clusters Hits',
                    # 'Clusters Hits limit',
                    # 'Clusters Hits limit period',
                    # 'Clusters Hits limit used (%)',
                    # 'Clusters Volume',
                    # 'Clusters Volume limit',
                    # 'Clusters Volume limit period',
                    # 'Clusters Volume limit used (%)',
                    # 'Coverages Hits',
                    # 'Coverages Hits limit',
                    # 'Coverages Hits limit period',
                    # 'Coverages Hits limit used (%)',
                    # 'Coverages Volume',
                    # 'Coverages Volume limit',
                    # 'Coverages Volume limit period',
                    # 'Coverages Volume limit used (%)',
                    # 'Related Stories Hits',
                    # 'Related Stories Hits limit',
                    # 'Related Stories Hits limit period',
                    # 'Related Stories Hits limit used (%)',
                    # 'Related Stories Volume',
                    # 'Related Stories Volume limit',
                    # 'Related Stories Volume limit period',
                    # 'Related Stories Volume limit used (%)',
                    # 'Stories Hits',
                    # 'Stories Hits limit',
                    # 'Stories Hits limit period',
                    # 'Stories Hits limit used (%)',
                    # 'Stories Volume',
                    # 'Stories Volume limit',
                    # 'Stories Volume limit period',
                    # 'Stories Volume limit used (%)',
                    # 'Time Series Hits',
                    # 'Time Series Hits limit',
                    # 'Time Series Hits limit period',
                    # 'Time Series Hits limit used (%)',
                    # 'Time Series Volume',
                    # 'Time Series Volume limit',
                    # 'Time Series Volume limit period',
                    # 'Time Series Volume limit used (%)',
                    # 'Trends Hits',
                    # 'Trends Hits limit',
                    # 'Trends Hits limit period',
                    # 'Trends Hits limit used (%)',
                    # 'Trends Volume',
                    # 'Trends Volume limit',
                    # 'Trends Volume limit period',
                    # 'Trends Volume limit used (%)',
                    # 'Total Hits per day (Average)',
                    # 'Total Volume',
                    # 'Total Volume per day (Average)',
                    # 'Total Stories Volume',
                    # 'Total Stories Volume limit',
                    # 'Total Stories Volume limit period',
                    # 'Total Stories Volume Usage stats',
                    # 'FF historic_1month',
                    # 'FF historic_3month',
                    # 'FF historic_unlimited',
                    # 'FF historic_1year',
                    # 'FF historic_2year',
                    # 'FF historic_3year',
                    # 'FF historic_4year',
                    # 'FF historic_5year',
                    # 'FF real_time',
                    # 'FF lang_en',
                    # 'FF lang_pt',
                    # 'FF lang_es',
                    # 'FF lang_it',
                    # 'FF lang_de',
                    # 'FF lang_fr',
                    # 'FF lang_ar',
                    # 'FF lang_da',
                    # 'FF lang_fi',
                    # 'FF lang_nl',
                    # 'FF lang_ru',
                    # 'FF lang_sv',
                    # 'FF lang_tr',
                    # 'FF lang_zh-tw',
                    # 'FF lang_zh-cn',
                    # 'FF lang_no',
                    # 'FF content_pack_superfeedr',
                    # 'FF content_pack_acquiremedia_not_licensed',
                    # 'FF content_pack_acquiremedia_licensed',
                    # 'FF unlimited_calls',
                    # 'FF canary',
                    # 'FF advanced_entities_search',
                    # 'FF advanced_entities_search_include_elsa',
                    # 'FF lucene_query_search',
                    # 'FF content_pack_twingly_news',
                    # 'FF tagger_industries',
                    # 'FF tagger_categories',
                    # 'FF clustering',
                    # 'FF prominence_score_search',
                    # 'FF content_pack_twingly_blogs',
                    # 'FF lang_fa',
                    # 'FF published_datetime',
                    # 'FF customer_ihs',
                    # 'FF new_v3_entities',
                    # 'FF external-entity-mapping-duns',
                    # 'FF relevance_boosting' 
                    #####################################################
    
    #############################################################################

    ##############################
    ## Filter only IBM apps ######
    ##############################

    # report = report['Account user email'].isin(['@ibm'])
    report = report[report['Account user email'].str.contains('@ibm')]


    ## Save to an excel
    formats = [
                {'value':'Account user email', 'cell':'A1' , 'color': 'gray'},
                {'value':'Application ID', 'cell':'B1' , 'color': 'gray'},
                {'value':'Application name', 'cell':'C1' , 'color': 'gray'},
                {'value':'Application product', 'cell':'D1' , 'color': 'grey'},
                {'value':'Hits and volumes collected from', 'cell':'E1' , 'color': 'purple'},
                {'value':'Hits and volumes collected to', 'cell':'F1' , 'color': 'purple'},
                {'value':'Total Hits', 'cell':'G1' , 'color': 'blue'},
                {'value':'Average hits per day', 'cell':'H1' , 'color': 'blue'},
                {'value':'Autocompletes Volume', 'cell':'I1' , 'color': 'blue'},
                {'value':'Clusters Volume', 'cell':'J1' , 'color': 'blue'},
                {'value':'Coverages Volume', 'cell':'K1' , 'color': 'blue'},
                {'value':'Related Stories Volume', 'cell':'L1' , 'color': 'blue'},
                {'value':'Stories Volume', 'cell':'M1' , 'color': 'blue'},
                {'value':'Time Series Volume', 'cell':'N1' , 'color': 'blue'},
                {'value':'Trends Volume', 'cell':'O1' , 'color': 'blue'},
                {'value':'Total Stories Volume', 'cell':'P1' , 'color': 'navy'},
                {'value':'Total Stories Volume limit', 'cell':'Q1' , 'color': 'navy'},
                {'value':'Total Stories Volume limit period', 'cell':'R1' , 'color': 'navy'},
                {'value':'Total Stories Volume Usage stats', 'cell':'S1' , 'color': 'navy'} 
                ]

        
                ################################################################################################
                ## All other fields ############################################################################
                ################################################################################################
                # {'value':'Application created (Date)', 'cell':'A1' , 'color': 'purple'},
                # {'value':'Application name', 'cell':'C1' , 'color': 'purple'},
                # {'value':'Application days since created', 'cell':'D1' , 'color': '#006400'},
                # {'value':'Application created in the last 15 days?', 'cell':'E1' , 'color': '#006400'},
                # {'value':'Application last updated (Date)', 'cell':'F1' , 'color': 'purple'},
                # {'value':'Application first traffic (Date)', 'cell':'G1' , 'color': 'purple'},
                # {'value':'Application state', 'cell':'H1' , 'color': 'purple'},
                # {'value':'Application description', 'cell':'I1' , 'color': 'purple'},
                # {'value':'Application plan custom (T/F)', 'cell':'J1' , 'color': '#006400'},
                # {'value':'Application plan default (T/F)', 'cell':'K1' , 'color': '#006400'},
                # {'value':'Application plan name', 'cell':'M1' , 'color': 'purple'},
                # {'value':'Account ID', 'cell':'N1' , 'color': 'gray'},
                # {'value':'Account created (Date)', 'cell':'O1' , 'color': 'gray'},
                # {'value':'Account last updated (Date)', 'cell':'P1' , 'color': 'gray'},
                # {'value':'Account state', 'cell':'Q1' , 'color': 'gray'},
                # {'value':'Account company name', 'cell':'R1' , 'color': 'gray'},
                # {'value':'Account monthly billing enabled', 'cell':'S1' , 'color': 'gray'},
                # {'value':'account monthly charging enabled', 'cell':'T1' , 'color': 'gray'},
                # {'value':'Account credit card stored', 'cell':'U1' , 'color': 'gray'},
                # {'value':'Account user state', 'cell':'V1' , 'color': 'gray'},
                # {'value':'Account user role', 'cell':'W1' , 'color': 'gray'},
                # {'value':'Account email domain', 'cell':'Y1' , 'color': 'gray'},
                # {'value':'Account username', 'cell':'Z1' , 'color': 'gray'},
                # {'value':'Account propose', 'cell':'AA1' , 'color': 'gray'},
                # {'value':'Usage Data Duration (Days)', 'cell':'AD1' , 'color': '#006400'},
                # {'value':'Autocompletes Hits', 'cell':'AE1' , 'color': 'red'},
                # {'value':'Autocompletes Hits limit', 'cell':'AF1' , 'color': 'red'},
                # {'value':'Autocompletes Hits limit period', 'cell':'AG1' , 'color': 'red'},
                # {'value':'Autocompletes Hits limit used (%)', 'cell':'AH1' , 'color': 'red'},
                # {'value':'Autocompletes Volume', 'cell':'AI1' , 'color': '#0000FF'},
                # {'value':'Autocompletes Volume limit', 'cell':'AJ1' , 'color': 'red'},
                # {'value':'Autocompletes Volume limit period', 'cell':'AK1' , 'color': 'red'},
                # {'value':'Autocompletes Volume limit used (%)', 'cell':'AL1' , 'color': 'red'},
                # {'value':'Clusters Hits', 'cell':'AM1' , 'color': 'red'},
                # {'value':'Clusters Hits limit', 'cell':'AN1' , 'color': 'red'},
                # {'value':'Clusters Hits limit period', 'cell':'AO1' , 'color': 'red'},
                # {'value':'Clusters Hits limit used (%)', 'cell':'AP1' , 'color': 'red'},
                # {'value':'Clusters Volume', 'cell':'AQ1' , 'color': '#0000FF'},
                # {'value':'Clusters Volume limit', 'cell':'AR1' , 'color': 'red'},
                # {'value':'Clusters Volume limit period', 'cell':'AS1' , 'color': 'red'},
                # {'value':'Clusters Volume limit used (%)', 'cell':'AT1' , 'color': 'red'},
                # {'value':'Coverages Hits', 'cell':'AU1' , 'color': 'red'},
                # {'value':'Coverages Hits limit', 'cell':'AV1' , 'color': 'red'},
                # {'value':'Coverages Hits limit period', 'cell':'AW1' , 'color': 'red'},
                # {'value':'Coverages Hits limit used (%)', 'cell':'AX1' , 'color': 'red'},
                # {'value':'Coverages Volume', 'cell':'AY1' , 'color': '#0000FF'},
                # {'value':'Coverages Volume limit', 'cell':'AZ1' , 'color': 'red'},
                # {'value':'Coverages Volume limit period', 'cell':'BA1' , 'color': 'red'},
                # {'value':'Coverages Volume limit used (%)', 'cell':'BB1' , 'color': 'red'},
                # {'value':'Related Stories Hits', 'cell':'BC1' , 'color': 'red'},
                # {'value':'Related Stories Hits limit', 'cell':'BD1' , 'color': 'red'},
                # {'value':'Related Stories Hits limit period', 'cell':'BE1' , 'color': 'red'},
                # {'value':'Related Stories Hits limit used (%)', 'cell':'BF1' , 'color': 'red'},
                # {'value':'Related Stories Volume', 'cell':'BG1' , 'color': '#0000FF'},
                # {'value':'Related Stories Volume limit', 'cell':'BH1' , 'color': 'red'},
                # {'value':'Related Stories Volume limit period', 'cell':'BI1' , 'color': 'red'},
                # {'value':'Related Stories Volume limit used (%)', 'cell':'BJ1' , 'color': 'red'},
                # {'value':'Stories Hits', 'cell':'BK1' , 'color': 'red'},
                # {'value':'Stories Hits limit', 'cell':'BL1' , 'color': 'red'},
                # {'value':'Stories Hits limit period', 'cell':'BM1' , 'color': 'red'},
                # {'value':'Stories Hits limit used (%)', 'cell':'BN1' , 'color': 'red'},
                # {'value':'Stories Volume', 'cell':'BO1' , 'color': '#0000FF'},
                # {'value':'Stories Volume limit', 'cell':'BP1' , 'color': 'red'},
                # {'value':'Stories Volume limit period', 'cell':'BQ1' , 'color': 'red'},
                # {'value':'Stories Volume limit used (%)', 'cell':'BR1' , 'color': 'red'},
                # {'value':'Time Series Hits', 'cell':'BS1' , 'color': 'red'},
                # {'value':'Time Series Hits limit', 'cell':'BT1' , 'color': 'red'},
                # {'value':'Time Series Hits limit period', 'cell':'BU1' , 'color': 'red'},
                # {'value':'Time Series Hits limit used (%)', 'cell':'BV1' , 'color': 'red'},
                # {'value':'Time Series Volume', 'cell':'BW1' , 'color': '#0000FF'},
                # {'value':'Time Series Volume limit', 'cell':'BX1' , 'color': 'red'},
                # {'value':'Time Series Volume limit period', 'cell':'BY1' , 'color': 'red'},
                # {'value':'Time Series Volume limit used (%)', 'cell':'BZ1' , 'color': 'red'},
                # {'value':'Trends Hits', 'cell':'CA1' , 'color': 'red'},
                # {'value':'Trends Hits limit', 'cell':'CB1' , 'color': 'red'},
                # {'value':'Trends Hits limit period', 'cell':'CC1' , 'color': 'red'},
                # {'value':'Trends Hits limit used (%)', 'cell':'CD1' , 'color': 'red'},
                # {'value':'Trends Volume', 'cell':'CE1' , 'color': '#0000FF'},
                # {'value':'Trends Volume limit', 'cell':'CF1' , 'color': 'red'},
                # {'value':'Trends Volume limit period', 'cell':'CG1' , 'color': 'red'},
                # {'value':'Trends Volume limit used (%)', 'cell':'CH1' , 'color': 'red'},
                # {'value':'Total Volume', 'cell':'CK1' , 'color': 'navy'},
                # {'value':'Total Volume per day (Average)', 'cell':'CL1' , 'color': '#006400'},
                # {'value':'Total Stories Volume', 'cell':'CM1' , 'color': 'navy'},
                # {'value':'Total Stories Volume limit', 'cell':'CN1' , 'color': 'navy'},
                # {'value':'Total Stories Volume limit period', 'cell':'CO1' , 'color': 'navy'},
                # {'value':'Total Stories Volume Usage stats', 'cell':'CP1' , 'color': 'navy'},
                # {'value':'FF historic_1month', 'cell':'CQ1' , 'color': '#E022E0'},
                # {'value':'FF historic_3month', 'cell':'CR1' , 'color': '#E022E0'},
                # {'value':'FF historic_unlimited', 'cell':'CS1' , 'color': '#E022E0'},
                # {'value':'FF historic_1year', 'cell':'CT1' , 'color': '#E022E0'},
                # {'value':'FF historic_2year', 'cell':'CU1' , 'color': '#E022E0'},
                # {'value':'FF historic_3year', 'cell':'CV1' , 'color': '#E022E0'},
                # {'value':'FF historic_4year', 'cell':'CW1' , 'color': '#E022E0'},
                # {'value':'FF historic_5year', 'cell':'CX1' , 'color': '#E022E0'},
                # {'value':'FF real_time', 'cell':'CY1' , 'color': '#E022E0'},
                # {'value':'FF lang_en', 'cell':'CZ1' , 'color': '#E022E0'},
                # {'value':'FF lang_pt', 'cell':'DA1' , 'color': '#E022E0'},
                # {'value':'FF lang_es', 'cell':'DB1' , 'color': '#E022E0'},
                # {'value':'FF lang_it', 'cell':'DC1' , 'color': '#E022E0'},
                # {'value':'FF lang_de', 'cell':'DD1' , 'color': '#E022E0'},
                # {'value':'FF lang_fr', 'cell':'DE1' , 'color': '#E022E0'},
                # {'value':'FF lang_ar', 'cell':'DF1' , 'color': '#E022E0'},
                # {'value':'FF lang_da', 'cell':'DG1' , 'color': '#E022E0'},
                # {'value':'FF lang_fi', 'cell':'DH1' , 'color': '#E022E0'},
                # {'value':'FF lang_nl', 'cell':'DI1' , 'color': '#E022E0'},
                # {'value':'FF lang_ru', 'cell':'DJ1' , 'color': '#E022E0'},
                # {'value':'FF lang_sv', 'cell':'DK1' , 'color': '#E022E0'},
                # {'value':'FF lang_tr', 'cell':'DL1' , 'color': '#E022E0'},
                # {'value':'FF lang_zh-tw', 'cell':'DM1' , 'color': '#E022E0'},
                # {'value':'FF lang_zh-cn', 'cell':'DN1' , 'color': '#E022E0'},
                # {'value':'FF lang_no', 'cell':'DO1' , 'color': '#E022E0'},
                # {'value':'FF content_pack_superfeedr', 'cell':'DP1' , 'color': '#E022E0'},
                # {'value':'FF content_pack_acquiremedia_not_licensed', 'cell':'DQ1' , 'color': '#E022E0'},
                # {'value':'FF content_pack_acquiremedia_licensed', 'cell':'DR1' , 'color': '#E022E0'},
                # {'value':'FF unlimited_calls', 'cell':'DS1' , 'color': '#E022E0'},
                # {'value':'FF canary', 'cell':'DT1' , 'color': '#E022E0'},
                # {'value':'FF advanced_entities_search', 'cell':'DU1' , 'color': '#E022E0'},
                # {'value':'FF advanced_entities_search_include_elsa', 'cell':'DV1' , 'color': '#E022E0'},
                # {'value':'FF lucene_query_search', 'cell':'DW1' , 'color': '#E022E0'},
                # {'value':'FF content_pack_twingly_news', 'cell':'DX1' , 'color': '#E022E0'},
                # {'value':'FF tagger_industries', 'cell':'DY1' , 'color': '#E022E0'},
                # {'value':'FF tagger_categories', 'cell':'DZ1' , 'color': '#E022E0'},
                # {'value':'FF clustering', 'cell':'EA1' , 'color': '#E022E0'},
                # {'value':'FF prominence_score_search', 'cell':'EB1' , 'color': '#E022E0'},
                # {'value':'FF content_pack_twingly_blogs', 'cell':'EC1' , 'color': '#E022E0'},
                # {'value':'FF lang_fa', 'cell':'ED1' , 'color': '#E022E0'},
                # {'value':'FF published_datetime', 'cell':'EE1' , 'color': '#E022E0'},
                # {'value':'FF customer_ihs', 'cell':'EF1' , 'color': '#E022E0'},
                # {'value':'FF new_v3_entities', 'cell':'EG1' , 'color': '#E022E0'},
                # {'value':'FF external-entity-mapping-duns', 'cell':'EH1' , 'color': '#E022E0'},
                # {'value':'FF relevance_boosting', 'cell':'EI1' , 'color': '#E022E0'}
                ################################################################################################

    save_to_excel = True
    
    if save_to_excel is True:

        file_name = str(datetime.now().strftime('%Y-%m-%d')) + " IBM usage report" + ".xlsx" 
        
        file_dir = os.path.dirname(os.path.abspath(__file__))
        
        options = {}
        options['strings_to_formulas'] = False
        options['strings_to_urls'] = False
        
        with pd.ExcelWriter(file_dir + '/reports/' + file_name) as writer: 
            report.to_excel(writer, sheet_name='Data', index=False, engine='xlsxwriter')

        
            workbook  = writer.book
            worksheet = writer.sheets['Data']

            for frm in formats:
            
                cell_format = workbook.add_format()
    
                cell_format.set_pattern(1)  # This is optional when using a solid fill.
                cell_format.set_bg_color(frm['color'])
                cell_format.set_font_color('white')
                cell_format.set_bold()
                
                
                worksheet.write(frm['cell'], frm['value'], cell_format)


            # Close the Pandas Excel writer and output the Excel file.
            writer.save()
    

if __name__ == "__main__":
    main()
