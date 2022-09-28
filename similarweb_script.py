from lib2to3.pgen2.token import STRING
from clickhouse_driver import Client
from collections import defaultdict
from datetime import datetime,date
import similarweb_functions as sw
import numpy as np
import pandas as pd
import requests as r
import calendar
import math
import json
import iso3166


pd.set_option('display.float_format', lambda x: '%.5f' % x)

# PRE-PROCESSING 

# Desired_window
most_recent_window = datetime.today().replace(month= datetime.today().month - 1).replace(day=1).strftime('%Y-%m')

# Review how many calls we have available for the month
# This does not cost a hit.
remaining_amount = sw.remaining_calls()

# Divide by 6, as each domain will cost us 6 api hits per domain
max_said_amt = math.floor(remaining_amount/6)

# Retrieve domains from CH     
advertiser_domains_call = sw.advertiser_saids_from_ch()
raw_domains_call = sw.raw_saids_from_ch()

domain_list_tbl = pd.concat([advertiser_domains_call,raw_domains_call]).drop_duplicates()

print("Coverage: "+"{0:.0f}%".format((max_said_amt/domain_list_tbl.shape[0]) * 100)+" of desired SAID's are going to be sent to SimilarWeb endpoint for the period of: "+most_recent_window+".")

# Take the top n amount that fit under the remaining available calls
subset_domain_list_tbl = domain_list_tbl.head(max_said_amt)

# Convert the said's to a list for for loop
domain_list = np.unique(subset_domain_list_tbl['said'].to_list())

# CORE CODE - SIMILARWEB INTERACTION

# Hit the SimilarWeb endpoints for the desired information
us_final_results = []
all_domains = domain_list

# While Logic added to maximize calls after discovering that calls to 
# domains SW doesn't have does not cost us Ad.Net any calls
while remaining_amount > 100:
    
    initial_remaining_amount = sw.remaining_calls()
    
    # Create a list of all domains sent to the SW endpoint
    all_domains = [*set([*all_domains , *domain_list])] 
    
    # Send in all domains to the endpoint via the `similarwb_us_only` function
    # each address passed to this function will hit the needed 6 endpoints and append to a final df
    for address in domain_list:    
        us_return = sw.similarweb_us_only(domain = address,start_date = most_recent_window,end_date=most_recent_window,country='us',lead_enrichment_api =True)
        us_final_results.append(us_return)

    # Since, via testing, after the initial list of domains is passed, their will likely be 
    # a substantial amount of calls remaining. We'll repeat the logic above (this should
    # likely be functionized)
    remaining_amount = sw.remaining_calls()
    
    # Make sure that the new list we'll pass to `similarwb_us_only` does not contain
    # any domains we've already sent in by removing them in this new subset process
    new_domain_list_tbl = domain_list_tbl[~domain_list_tbl.said.isin(all_domains)]
    
    max_said_amt = math.floor(remaining_amount/6)
    
    new_subset_domain_list_tbl = new_domain_list_tbl.head(max_said_amt)
    
    # Having refreshed the domain list, the `while` logic will kick back to the top of the loop
    # appending the new list to `all_domains` so they may be excluded if another loop is required,
    # and by re-assigning `domain_list` to the new domains, the for loop will send in the new
    # domains, using up as many calls as possible, until the remaining amount of calls is <= 100.
    domain_list = new_subset_domain_list_tbl['said'].to_list()
    
        
    # If all API calls used successfully, with remainder under 100, break the while loop
    if initial_remaining_amount == remaining_amount:
        break
    
# The resulting information is nested, this will unnest and flatten into desired final dataframes
listed_results = sw.similarweb_unpack_results_into_dfs(us_final_results)

# EXPORT CODE BELOW

# Categorical Table
#--------------------------------
categorization_tbl = listed_results[0]
categorization_tbl.to_csv('/usr/local/airflow/data/similarweb_review/similarweb_categorization_'+most_recent_window+'.csv',index=False)
# Traffic Table
#--------------------------------
traffic_tbl = listed_results[1]
traffic_tbl.to_csv('/usr/local/airflow/data/similarweb_review/similarweb_traffic_'+most_recent_window+'.csv',index=False)
# Domain Details Table
#--------------------------------
domain_details_tbl = listed_results[2]
domain_details_tbl.to_csv('/usr/local/airflow/data/similarweb_review/similarweb_domain_details_'+most_recent_window+'.csv',index=False)
# Source Table
#--------------------------------
source_tbl = listed_results[3]
source_tbl.to_csv('/usr/local/airflow/data/similarweb_review/similarweb_source_'+most_recent_window+'.csv',index=False)

# Clickhouse Ingestion

sw.insert_data_into_ch(insert_table_name='similarweb_categorization',data=categorization_tbl)

sw.insert_data_into_ch(insert_table_name='similarweb_traffic'       ,data=traffic_tbl)

sw.insert_data_into_ch(insert_table_name='similarweb_domain_details',data=domain_details_tbl)

sw.insert_data_into_ch(insert_table_name='similarweb_source'        ,data=source_tbl)