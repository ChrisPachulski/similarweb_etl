from lib2to3.pgen2.token import STRING
from clickhouse_driver import Client
from collections import defaultdict
from datetime import datetime,date
import numpy as np
import pandas as pd
import requests as r
import calendar
import math
import json
import iso3166

def remaining_calls():
    """
    Review the remaining api calls Ad.Net has left for the month
    """
    url =  'https://api.similarweb.com/capabilities?api_key='
    api_key = ''.join( pd.read_json('/root/.config/similarweb/similarweb_secret.json',orient='index').iloc[0,:].apply(str).values)
    api_call_url = url+api_key
    remaining_api_calls  = r.get(api_call_url).content
    available_remaining_calls = json.loads(remaining_api_calls)['remaining_hits']
    print(str(available_remaining_calls)+" API hits remain for the current month.")
    return(available_remaining_calls)

def manually_create_current_window():
    """
    Mainly used for testing, allows for the extensions of the start_date beyond the default month
    """
    manual_start_date = datetime.today().replace(month= datetime.today().month - 1).replace(day=1).strftime('%Y-%m-%d')
    
    month_end_date = calendar.monthrange(datetime.today().year, datetime.today().replace(month= datetime.today().month - 1).month)[1]
    manual_end_date = datetime.today().replace(month= datetime.today().month - 1).replace(day=month_end_date).strftime('%Y-%m-%d')

    return([manual_start_date,manual_end_date])

def listed_dict_col_conversion(df,col):
    """Converts listed dictionaries into dataframe columns
    Args:
        df (pd.DataFrame): Any dataframe
        col (pd.Series): desired column to unpack
    Returns:
        pd.DataFrame: Final DataFrame that contains the unnested column
    """
    
    res = defaultdict(list)
    for sub in df[col][0]:
        for key in sub:
            res[key].append(sub[key])
    final_df = pd.DataFrame(res)
    return (final_df)

def column_type_adjustment(df):
    """ Detects character strings in column names to assign correct data typing for the columns contents.
    Args:
        df (pd.DataFrame): Any similarweb pd.DataFrame that utilizes their default nomenclature
    """
    
    float_matches = ['ratio','share']
    integer_matches = ['total','count','visits']
    date_matches = ['date']
    for column in df.columns:
        if any(x in column for x in float_matches):
            df[column] = round(df[column].astype(np.float64),3)

    for column in df.columns:
        if any(x in column for x in integer_matches):
            df[column] = df[column].astype(np.int64)

    for column in df.columns:
        if any(x in column for x in date_matches):
            df[column] = pd.to_datetime(df[column]).dt.normalize()

    return(df)  

def format_date_columns(df):
    """Looks through all columns to auto detect date columns seen as data type object and converts
    to dateime64[ns]
    Args:
        df (pd.DataFrame): Any pd.DataFrame
    """
    for column in df.columns:

        if df[column].dtype == "datetime64[ns]":

            try:
                df[column] = df[column].dt.strftime('%Y-%m-%d')
            except:
                pass
    
    return(df)

def advertiser_saids_from_ch():
    """ 
    Pulls down the active advertiser to include in the similarweb pull.
    These should always be included for the most up to date information on active advertisers.
    """
    end_date = datetime.today().replace(day=1).strftime('%Y-%m-%d')

    if datetime.today().month == 1:
        start_date = datetime.today().replace(month= 12).replace(day=1).strftime('%Y-%m-%d')
    else:
        start_date = datetime.today().replace(month= datetime.today().month - 1).replace(day=1).strftime('%Y-%m-%d')

    
    client = Client('192.168.101.80', database='addotnet')
    new_sql = """
        SELECT DISTINCT advertiser_name, domain(next_hop_url) advertiser_domain
        FROM ad_event_view
        WHERE event_date between \'"""+start_date+"""\' and \'"""+end_date+"""\' 
        """

    advertiser_saids_tbl = client.query_dataframe(new_sql)
    
    advertiser_saids_tbl_filter_1 = advertiser_saids_tbl[~advertiser_saids_tbl.advertiser_domain.str.contains('(^[0-9]+|^(c|a|t|e)\.|^click(serve)*|\s+)', regex=True,na=False)].drop_duplicates()
    
    advertiser_saids_tbl_filter_2 = advertiser_saids_tbl_filter_1[advertiser_saids_tbl_filter_1['advertiser_domain'] != '']
        
    advertiser_saids_tbl_filter_3 = advertiser_saids_tbl_filter_2[advertiser_saids_tbl_filter_2['advertiser_domain'].notnull()]
    
    advertiser_saids_tbl_filter_3['advertiser_domain'] = advertiser_saids_tbl_filter_3['advertiser_domain'].str.replace(r'ww(w|3)\.','',regex=True)
    
    advertiser_saids_tbl_filter_final = advertiser_saids_tbl_filter_3.drop_duplicates().rename(columns={'advertiser_domain':'said'})[['said']].reset_index(drop=True)
    
    print("advertiser_saids_from_ch successfully fired.")
    return(advertiser_saids_tbl_filter_final)

def raw_saids_from_ch():
    """
    Pulls the desired raw domains to be sent to similarweb. Prio should default to last.
    """
    
    end_date = datetime.today().replace(day=1).strftime('%Y-%m-%d')

    if datetime.today().month == 1:
        start_date = datetime.today().replace(month= 12).replace(day=1).strftime('%Y-%m-%d')
    else:
        start_date = datetime.today().replace(month= datetime.today().month - 1).replace(day=1).strftime('%Y-%m-%d')

    
    client = Client('192.168.101.80', database='addotnet')
    new_sql = """
    SELECT said 
    FROM (
        SELECT distinct said, sum(paid_clicks) paid_clicks, sum(requests) requests 
        FROM ad_event_view 
        WHERE event_date between \'"""+start_date+"""\' and \'"""+end_date+"""\' and 
            (said like '%.com' or said like '%.net' or said like '%.edu') and
            affiliate_account_name not in ('DMPRO.com','TONIC MB','Trellian','Bodis XML','Skenzo','Adrenalads','Domain Power','RVN Digital')
        GROUP BY said having paid_clicks >0 order by requests desc) 
        """

    raw_saids_tbl = client.query_dataframe(new_sql)
    
    print("raw_saids_from_ch successfully fired.")
    return(raw_saids_tbl)

def empty_lead_enrichment():
    """
    Generate an empty lead_enrichment pd.DataFrame to ensure similarweb data appending continues
    """
    empty_df = pd.DataFrame(columns=['process_date','domain','start_date','end_date','country','source_type','share','nation_name','share'])
    return(empty_df)

def empty_lead_enrichment_core():
    """
    Generate an empty lead_enrichment_core pd.DataFrame to ensure similarweb data appending continues
    """
    empty_df = pd.DataFrame(columns=['process_date', 'domain', 'start_date', 'end_date', 'country',
       'desktop_share', 'mobile_share', 'global_rank', 'employee_range',
       'site_category', 'est_revenue', 'pages_per_visit', 'visits',
       'mom_growth', 'unique_visitors', 'bounce_rate',
       'average_visit_duration'])
    return(empty_df)

def empty_descriptive_elements(domain,country):
    """Generates an empty descriptive pd.DataFrame should SimilarWeb be unable to provide
    Args:
        domain (str): particular domain of interest
        country (str): geography of interest. Default should always be 'US'
    """
    placeholder_data = [[date.today().strftime('%Y-%m-%d'),domain,manually_create_current_window()[0],manually_create_current_window()[1],country]]
    empty_descriptive_elements = pd.DataFrame(placeholder_data,columns=['process_date','domain','start_date','end_date','country'])
    return(empty_descriptive_elements)

def empty_traffic_share():
    """
    Generate an empty traffic_share pd.DataFrame to ensure similarweb data appending continues
    """
    empty_df = pd.DataFrame(columns=['desktop_share','mobile_share'])
    return(empty_df)
     
def unpack_lead_enrichment(api_content):
    """
    Dedicated function to parse the return from the lead enrichment api endpoint
    Args:
        api_content (json/dict): response.content from the endpoint
    """
    lead_json = json.loads(api_content)
    
    descriptive_element = pd.json_normalize(lead_json['meta']['request'])[['domain','start_date','end_date','country']]
    descriptive_element['process_date'] = date.today().strftime("%Y-%m-%d")
    descriptive_element = descriptive_element[['process_date','domain','start_date','end_date','country']]
    traffic_share = pd.json_normalize(lead_json['mobile_desktop_share'])[['value.desktop_share','value.mobile_share']].rename(columns={"value.desktop_share":"desktop_share","value.mobile_share":"mobile_share"})
    
    try:
        traffic_sources = pd.json_normalize(lead_json['traffic_sources'],record_path='value')
    except NotImplementedError:
        traffic_sources = empty_traffic_share()
    
        
    geo_share = pd.json_normalize(lead_json['geography_share'],record_path='value')
    
    if geo_share.shape[0] == 0:
        geo_share = pd.DataFrame(columns=['country','share'])
    
    
    iso_country_df = pd.DataFrame(iso3166.countries_by_numeric).transpose().iloc[:,[0,2,3]].reset_index(drop=True)
    iso_country_df = iso_country_df.rename(columns={0:"nation_name",
                                                2:"nation_alpha3",
                                                3:"country"})
    iso_country_df['country'] = iso_country_df['country'].astype(np.int64)
    
    geo_share = geo_share.merge(iso_country_df,left_on='country',right_on='country',how='left')[['nation_name','share']]
    
    lead_enrichment = pd.concat([descriptive_element,traffic_sources,geo_share],axis=1)
    cols = ['process_date','domain','start_date','end_date','country']
    lead_enrichment.loc[:,cols] = lead_enrichment.loc[:,cols].ffill()
    
    lead_enrichment_core = pd.concat([descriptive_element,traffic_share],axis=1)
    lead_enrichment_core['global_rank'] = lead_json['global_rank']
    lead_enrichment_core['employee_range'] = lead_json['employee_range']
    lead_enrichment_core['site_category'] = lead_json['website_category'] 
    lead_enrichment_core['est_revenue'] = lead_json['estimated_revenue_in_usd']
    try:
        lead_enrichment_core['pages_per_visit'] = pd.json_normalize(lead_json['pages_per_visit'])[['value']].rename(columns={"value":"pages_per_visit"})
    except NotImplementedError:
        lead_enrichment_core['pages_per_visit'] = 0
    try:    
        lead_enrichment_core['visits'] = pd.json_normalize(lead_json['visits'])[['value']].rename(columns={"value":"visits"}).astype(np.int64)
    except NotImplementedError:
        lead_enrichment_core['visits'] = 0
    try:    
        lead_enrichment_core['mom_growth'] = pd.json_normalize(lead_json['mom_growth'])[['value']].rename(columns={"value":"mom_growth"})
    except NotImplementedError:
        lead_enrichment_core['mom_growth'] = 0    
    try:
        lead_enrichment_core['unique_visitors'] = pd.json_normalize(lead_json['unique_visitors'])[['value']].rename(columns={"value":"unique_visitors"})
    except NotImplementedError:
        lead_enrichment_core['unique_visitors'] = 0    
    try:
        lead_enrichment_core['bounce_rate'] = pd.json_normalize(lead_json['bounce_rate'])[['value']].rename(columns={"value":"bounce_rate"})
    except NotImplementedError:
        lead_enrichment_core['bounce_rate'] = 0    
    try:
        lead_enrichment_core['average_visit_duration'] = pd.json_normalize(lead_json['average_visit_duration'])[['value']].rename(columns={"value":"average_visit_duration"})
    except NotImplementedError:
        lead_enrichment_core['average_visit_duration'] = 0    
    try:        
        incoming_total_visits = pd.json_normalize(lead_json['visits'])[['value']].rename(columns={"value":"visits"})
    except NotImplementedError:
        incoming_total_visits = 0
    
    return([lead_enrichment, lead_enrichment_core,descriptive_element,traffic_share,incoming_total_visits])

def unpack_desktop_incoming(api_content):
    """
    Dedicated function to parse the return from the incoming desktop api endpoint
    Args:
        api_content (json/dict): response.content from the endpoint
    """
    desktop_incoming_json = json.loads(api_content)
    desktop_incoming_domains = pd.json_normalize(desktop_incoming_json['referrals']).rename(columns={"share":"desktop_share","change":"desktop_change"})
    desktop_incoming_domains['desktop_referred_visits'] = round(desktop_incoming_json['visits'] * desktop_incoming_domains['desktop_share'],0).astype(np.int64)
    
    return([desktop_incoming_json,desktop_incoming_domains])

def unpack_mobile_incoming(api_content):
    """
    Dedicated function to parse the return from the mobile incoming api endpoint
    Args:
        api_content (json/dict): response.content from the endpoint
    """
    mobile_incoming_json = json.loads(api_content)
    mobile_incoming_domains = pd.json_normalize(mobile_incoming_json['referrals']).rename(columns={"share":"mobile_share","change":"mobile_change"})
    mobile_incoming_domains['mobile_referred_visits'] = round(mobile_incoming_json['visits'] * mobile_incoming_domains['mobile_share'],0).astype(np.int64)
    
    return([mobile_incoming_json,mobile_incoming_domains])

def merge_incoming_traffic(incoming_total_visits,desktop_incoming,mobile_incoming,desktop_json,mobile_json,descriptive_element,traffic_share):
    """
    Crucial function for merging together lead enrichment data with the incoming traffic into one pd.DataFrame
    Args:
        incoming_total_visits (pd.DataFrame): pd.DataFrame containing total visits from the unpack_lead_enrichment
        desktop_incoming (pd.DataFrame): pd.DataFrame containing the desktop incoming visits for the time period
        mobile_incoming (pd.DataFrame): pd.DataFrame containingthe mobile incoming visits for the time period
        desktop_json (json/dict): parsed json from the unpack_desktop_incoming function 
        mobile_json (json/dict): parsed json from the unpack_mobile_incoming
        descriptive_element (pd.DataFrame): pd.DataFrame containing descriptive information from the unpack_lead_enrichment
        traffic_share (pd.DataFrame): pd.DataFrame containing traffic share information from the unpack_lead_enrichment
    """
    incoming_domains_merged = mobile_incoming.merge(desktop_incoming,left_on='domain',right_on='domain',how='outer')
    incoming_domains_merged['mobile_change'] = incoming_domains_merged['mobile_change'].fillna(0)
    incoming_domains_merged['mobile_share'] = incoming_domains_merged['mobile_share'].fillna(0)
    incoming_domains_merged['mobile_referred_visits'] = incoming_domains_merged['mobile_referred_visits'].fillna(0)
    
    incoming_domains_merged['mobile_referred_visits'] = incoming_domains_merged.mobile_referred_visits.fillna(0).astype('int')
    incoming_domains_merged['desktop_referred_visits'] = incoming_domains_merged.desktop_referred_visits.fillna(0).astype('int')
    
    incoming_domains_merged['domain_incoming_visits'] = incoming_domains_merged['desktop_referred_visits'] + incoming_domains_merged['mobile_referred_visits']
    
    incoming_domains_merged = incoming_domains_merged[['domain','desktop_referred_visits','mobile_referred_visits','domain_incoming_visits']].rename(columns={"domain":"domain_of_interest"})
    
    incoming_domains_merged['referred_visits'] = mobile_json['visits'] + desktop_json['visits']
    
    incoming_traffic_tbl = pd.concat([descriptive_element,traffic_share,incoming_domains_merged],axis=1)
    incoming_traffic_tbl['total_visits'] = incoming_total_visits

    incoming_traffic_tbl['domain_type'] = 'incoming'
    cols = ['process_date','domain','start_date','end_date','country','domain_type','desktop_share','mobile_share','total_visits']
    incoming_traffic_tbl.loc[:,cols] = incoming_traffic_tbl.loc[:,cols].ffill()
    incoming_traffic_tbl['total_visits'] = incoming_traffic_tbl['total_visits'].astype(np.int64)
    
    return(incoming_traffic_tbl)

def unpack_desktop_outgoing(api_content):
    """
    Dedicated function to parse the return from the outgoing desktop api endpoint
    Args:
        api_content (json/dict): response.content from the endpoint
    """
    desktop_outgoing_json = json.loads(api_content)
    desktop_outgoing_domains = pd.json_normalize(desktop_outgoing_json['referrals']).rename(columns={"share":"desktop_share","change":"desktop_change"})
    desktop_outgoing_domains['desktop_outgoing_visits'] = round(desktop_outgoing_json['visits'] * desktop_outgoing_domains['desktop_share'],0).astype(np.int64)
    
    return([desktop_outgoing_json,desktop_outgoing_domains])   

def unpack_mobile_outgoing(api_content):
    """
    Dedicated function to parse the return from the outgoing mobile api endpoint
    Args:
        api_content (json/dict): response.content from the endpoint
    """
    mobile_outgoing_json = json.loads(api_content)
    mobile_outgoing_domains = pd.json_normalize(mobile_outgoing_json['referrals']).rename(columns={"share":"mobile_share","change":"mobile_change"})
    mobile_outgoing_domains['mobile_outgoing_visits'] = round(mobile_outgoing_json['visits'] * mobile_outgoing_domains['mobile_share'],0).astype(np.int64)
    
    return([mobile_outgoing_json,mobile_outgoing_domains])    

def merge_outgoing_traffic(desktop_outgoing,mobile_outgoing,desktop_json,mobile_json,descriptive_element,traffic_share):
    """
    Crucial function for merging together lead enrichment data with the outgoing traffic into one pd.DataFrame.
    Important to note, the descriptive/traffic share elements will allow for the join of incoming w/ outgoing.
    Args:
        desktop_outgoing (pd.DataFrame): pd.DataFrame containing the desktop outgoing visits for the time period
        mobile_outgoing (pd.DataFrame): pd.DataFrame containingthe mobile outgoing visits for the time period
        desktop_json (json/dict): parsed json from the unpack_desktop_outgoing function 
        mobile_json (json/dict): parsed json from the unpack_mobile_outgoing
        descriptive_element (pd.DataFrame): pd.DataFrame containing descriptive information from the unpack_lead_enrichment
        traffic_share (pd.DataFrame): pd.DataFrame containing traffic share information from the unpack_lead_enrichment
    """
    outgoing_domains_merged = mobile_outgoing.merge(desktop_outgoing,left_on='domain',right_on='domain',how='outer')
    outgoing_domains_merged['mobile_outgoing_visits'] = outgoing_domains_merged.mobile_outgoing_visits.fillna(0).astype('int')
    outgoing_domains_merged['desktop_outgoing_visits'] = outgoing_domains_merged.desktop_outgoing_visits.fillna(0).astype('int')
    
    outgoing_domains_merged['domain_outgoing_visits'] = (outgoing_domains_merged['desktop_outgoing_visits'] + outgoing_domains_merged['mobile_outgoing_visits']).fillna(0).astype('int')
    
    outgoing_domains_merged = outgoing_domains_merged[['domain','desktop_outgoing_visits','mobile_outgoing_visits','domain_outgoing_visits']].rename(columns={"domain":"domain_of_interest"})
    
    outgoing_domains_merged['outgoing_visits'] = (mobile_json['visits'] + desktop_json['visits'])
    
    outgoing_traffic_tbl = pd.concat([descriptive_element,traffic_share,outgoing_domains_merged],axis=1)
    #outgoing_traffic_tbl['total_outgoing'] = np.NaN
    outgoing_traffic_tbl['domain_type'] = 'outgoing'
    cols = ['process_date','domain','start_date','end_date','country','domain_type','desktop_share','mobile_share']
    outgoing_traffic_tbl.loc[:,cols] = outgoing_traffic_tbl.loc[:,cols].ffill()
    
    return(outgoing_traffic_tbl)

def empty_desktop_outgoing_domains():
    """
    Generate an empty desktop_outgoing_domains pd.DataFrame to ensure similarweb data appending continues should the endpoint
    not have data to return
    """
    empty_df = pd.DataFrame(columns=['desktop_share','domain', 'desktop_change', 'desktop_outgoing_visits'])
    return(empty_df)

def empty_mobile_outgoing_domains():
    """
    Generate an empty mobile_outgoing_domains pd.DataFrame to ensure similarweb data appending continues should the endpoint
    not have data to return
    """
    empty_df = pd.DataFrame(columns=['mobile_share','domain', 'mobile_change', 'mobile_outgoing_visits'])
    return(empty_df)

def empty_categorization_lite():
    """
    Generate an empty categorization_lite pd.DataFrame to ensure similarweb data appending continues should the endpoint
    not have data to return
    """
    empty_df = pd.DataFrame(columns=['site_name','start_date','end_date','monthly_visits','usa_visits','global_visits','super_category','sub_category','traffic_category_share','incoming_traffic','incoming_share','incoming_share_chg','outgoing_traffic','outgoing_share','outgoing_share_chg','nation_alpha3','nation_share','nation_share_chg','nation_name','organic_keyword','organic_keyword_share','organic_keyword_share_chg','keyword','paid_keyword_share','paid_keyword_share_chg','publisher_site','publisher_share','publisher_share_chg','advertiser_site','advertiser_share','advertiser_share_chg'])
    return(empty_df)

def empty_core_lite():
    """
    Generate an empty core_lite pd.DataFrame to ensure similarweb data appending continues should the endpoint
    not have data to return
    """
    empty_df = pd.DataFrame(columns=['site_name','category','title','description','total_countries','referrals_ratio','total_referring','search_ratio','organic_keywords_rolling_unique_count',	'paid_keywords_rolling_unique_count',	'organic_search_share','paid_search_share','social_ratio','display_ads_ratio','incoming_ads_rolling_unique_count','also_visited_unique_count','top_also_visited'])
    return(empty_df)

def empty_similar_sites_lite():
    """
    Generate an empty similar_sites_lite pd.DataFrame to ensure similarweb data appending continues should the endpoint
    not have data to return
    """
    empty_df = pd.DataFrame(columns=["site_name","start_date","end_date","similar_sites","similar_site_rank","similar_sites_rank","similar_site_rank_rank","tag_word","tag_word_correlation"])
    return(empty_df)

def similarweb_lite(domain=STRING, desired_output = 'categorization'):
    """
    Hits the LITE endpoint of SimilarWeb (https://docs.api.similarweb.com/#8afe4197-b360-41f5-baae-e0df84b39d40).
    This function will consume 1 call per domain fed.
    Args:
        domain (str): Domain of interest to query SimilarWeb with. Defaults to STRING.
        desired_output (str, optional): Should this be set to anything else but 'categorization',
                                        will return the entire response of the lite call parsed into a pd.DateFrame. 
                                        Defaults to 'categorization'.
    """
    
    url =  'https://api.similarweb.com/v1/website/'
    # Production - Pull Key from resources json
    api_key = ''.join( pd.read_json('/root/.config/similarweb/similarweb_secret.json',orient='index').iloc[0,:].apply(str).values)
    endpoint = '/general-data/all?'
    #domain = domain
    
    api_call_url = url + domain + endpoint +'api_key='+api_key
    #print(api_call_url)
    results  = r.get(api_call_url)
    
    if results.status_code == 200:
        raw_json = json.loads(results.content)
        #print(raw_json)
        monthly_visits_tbl = pd.DataFrame(raw_json['estimated_monthly_visits'].items(),columns =['date','monthly_visits'])

        normalized_df = pd.json_normalize(raw_json, max_level=0)

        ## Unpack the dict columns
        global_rank = pd.json_normalize(normalized_df['global_rank'])
        global_rank = global_rank.rename(columns={"rank":"global_rank","direction":"global_chg"})

        country_rank = pd.json_normalize(normalized_df['country_rank'])
        country_rank = country_rank.rename(columns={"country":"country_id","rank":"country_rank","direction":"country_chg"})

        category_rank = pd.json_normalize(normalized_df['category_rank'])
        category_rank = category_rank.rename(columns={"category":"sw_category",
                                                    "rank":"sw_category_rank",
                                                    "direction":"sw_category_chg"})


        engagements = pd.json_normalize(normalized_df['engagments'])
        engagements['end_date'] = normalized_df['daily_visits_max_date']
        engagements = engagements[['end_date','visits',
                                'time_on_site', 'page_per_visit',
                                'bounce_rate']]

        traffic_sources = pd.json_normalize(normalized_df['traffic_sources'])
        traffic_sources = traffic_sources.rename(columns={"search":"traffic_sources_search",
                                                    "social":"traffic_sources_social",
                                                    "paid _referrals":"traffic_sources_paid_referrals",
                                                    "referrals":"traffic_sources_referrals",
                                                    "mail":"traffic_sources_mail",
                                                    "direct":"traffic_sources_direct"})

        ## Transforming listed dictionary columns to expanded values
        # Top Country Share
        try:
            top_country_shares = listed_dict_col_conversion(df = normalized_df, col = 'top_country_shares')
        except TypeError:
            top_country_shares = pd.DataFrame(columns=['country','value','change'],index=range(1))

        top_country_shares =top_country_shares.rename(columns={
            "country":"country_id",
            "value":"nation_share",
            "change":"nation_share_chg"
        })

        iso_country_df = pd.DataFrame(iso3166.countries_by_numeric).transpose().iloc[:,[0,2,3]].reset_index(drop=True)
        iso_country_df = iso_country_df.rename(columns={0:"nation_name",
                                                        2:"nation_alpha3",
                                                        3:"country_id"})
        iso_country_df['country_id'] = iso_country_df['country_id'].astype(np.int64)

        country_merged_df = top_country_shares.merge(right = iso_country_df,how='left',left_on='country_id',right_on='country_id')

        top_country_shares = country_merged_df[['nation_alpha3','nation_share','nation_share_chg','nation_name']]

        # Top referring (incoming traffic)
        try:
            top_referring = listed_dict_col_conversion(df = normalized_df, col = 'top_referring')
        except TypeError:
            top_referring = pd.DataFrame(columns=['site','value','change'],index=range(1))
        
        if top_referring.shape[0] == 0:
            top_referring = pd.DataFrame(columns=['site','value','change'],index=range(1))
        
        top_referring =top_referring.rename(columns={
            "site":"incoming_traffic",
            "value":"incoming_share",
            "change":"incoming_share_chg"
        })

        # Top Destinations (outgoing traffic)
        try:
            top_destinations = listed_dict_col_conversion(df = normalized_df, col = 'top_destinations')
        except TypeError:
            top_destinations = pd.DataFrame(columns=['site','value','change'],index=range(1))
            
        if top_destinations.shape[0] == 0:
            top_destinations = pd.DataFrame(columns=['site','value','change'],index=range(1))

        top_destinations = top_destinations.rename(columns={
            "site":"outgoing_traffic",
            "value":"outgoing_share",
            "change":"outgoing_share_chg"
        })

        # Organic Keywords
        try:
            top_org_keywords = listed_dict_col_conversion(df = normalized_df, col = 'top_organic_keywords')
        except TypeError:
            top_org_keywords = pd.DataFrame(columns=['keyword','value','change'],index=range(1))
        
        if top_org_keywords.shape[0] == 0:
            top_org_keywords = pd.DataFrame(columns=['keyword','value','change'],index=range(1))

        
        top_org_keywords = top_org_keywords.rename(columns={
            "keyword":"organic_keyword",
            "value":"organic_keyword_share",
            "change":"organic_keyword_share_chg"
        })

        # Paid Keywords
        try:
            top_paid_keywords = listed_dict_col_conversion(df = normalized_df, col = 'top_paid_keywords')
        except TypeError:
            top_paid_keywords = pd.DataFrame(columns=['keyword','value','change'],index=range(1))
            
        if top_paid_keywords.shape[0] == 0:
            top_paid_keywords = pd.DataFrame(columns=['keyword','value','change'],index=range(1))

        top_paid_keywords = top_paid_keywords.rename(columns={
            "site":"paid_keyword",
            "value":"paid_keyword_share",
            "change":"paid_keyword_share_chg"
        })

        # Publishers
        try:
            top_publishers = listed_dict_col_conversion(df = normalized_df, col = 'top_publishers')
        except TypeError:
            top_publishers = pd.DataFrame(columns=['site','value','change'],index=range(1))
        
        if top_publishers.shape[0] == 0:
            top_publishers = pd.DataFrame(columns=['site','value','change'],index=range(1))

        top_publishers = top_publishers.rename(columns={
            "site":"publisher_site",
            "value":"publisher_share",
            "change":"publisher_share_chg"
        })
        # Ad Networks
        try:
            top_ad_networks = listed_dict_col_conversion(df = normalized_df, col = 'top_ad_networks')
        except TypeError:
            top_ad_networks = pd.DataFrame(columns=['site','value','change'],index=range(1))
        
        if top_ad_networks.shape[0] == 0:
            top_ad_networks = pd.DataFrame(columns=['site','value','change'],index=range(1))
        
        top_ad_networks = top_ad_networks.rename(columns={
            "site":"advertiser_site",
            "value":"advertiser_share",
            "change":"advertiser_share_chg"
        })
        # Similar Sites
        try:
            similar_sites = listed_dict_col_conversion(df = normalized_df, col = 'similar_sites')
        except TypeError:
            similar_sites = pd.DataFrame(columns=['site','screenshot','rank'],index=range(1))
        
        if similar_sites.shape[0] == 0:
            similar_sites = pd.DataFrame(columns=['site','screenshot','rank'],index=range(1))
        
        similar_sites = similar_sites.rename(columns={
            "site":"similar_sites",
            "screenshot":"similar_site_screenshot",
            "rank":"similar_site_rank"
        })
        similar_sites = similar_sites[['similar_sites','similar_site_rank']].sort_values('similar_site_rank').reset_index(drop=True)

        # Similar Sites by Rank    
        try:    
            similar_sites_by_rank = listed_dict_col_conversion(df = normalized_df, col = 'similar_sites_by_rank')
        except TypeError:
            similar_sites_by_rank = pd.DataFrame(columns=['site','screenshot','rank'],index=range(1))
            
        if similar_sites_by_rank.shape[0] == 0:
            similar_sites_by_rank = pd.DataFrame(columns=['site','screenshot','rank'],index=range(1))
        

        similar_sites_by_rank = similar_sites_by_rank.rename(columns={
            "site":"similar_sites_rank",
            "screenshot":"similar_site_rank_screenshot",
            "rank":"similar_site_rank_rank"
        })
        similar_sites_by_rank = similar_sites_by_rank[['similar_sites_rank','similar_site_rank_rank']].sort_values('similar_site_rank_rank')

        # Top Categories
        try:
            top_categories = listed_dict_col_conversion(df = normalized_df, col = 'top_categories')
        except TypeError:
            top_categories = pd.DataFrame(columns=['category','affinity'],index=range(1))
        
        if top_categories.shape[0] == 0:
            top_categories = pd.DataFrame(columns=['category','affinity'],index=range(1))

        top_categories = top_categories.rename(columns={
            "category":"traffic_category",
            "affinity":"traffic_category_share"
        })
       
        if (type(top_categories['traffic_category'].unique()[0]) != str) and (math.isnan(top_categories['traffic_category'].unique()[0]) == True):
            top_categories[['super_category','sub_category']] = np.nan
            top_categories = top_categories[['super_category','sub_category','traffic_category_share']]
        else:
            top_categories[['super_category','sub_category']] = top_categories['traffic_category'].str.split('/',expand=True)
            top_categories = top_categories[['super_category','sub_category','traffic_category_share']]

        # Tags
        try:
            tags = listed_dict_col_conversion(df = normalized_df, col = 'tags')
        except TypeError:
            tags = pd.DataFrame(columns=['tag','strength'],index=range(1))
        
        if tags.shape[0] == 0:
            tags = pd.DataFrame(columns=['tag','strength'],index=range(1))
        
        
        tags = tags.rename(columns={
            "tag":"tag_word",
            "strength":"tag_word_correlation"
        })
        ## Create Core DF of all fields not nested/listed/dict'ed
        # Explode out 'top_also_visted'
        if normalized_df['top_also_visited'][0] == None:
            normalized_df['top_also_visited'] = ''
        else:
            normalized_df['top_also_visited'] = ', '.join(normalized_df['top_also_visited'][0])
            
            
        core_df = normalized_df[['site_name',
                'category',
                'title',
                'description',
                'total_countries',
                'referrals_ratio',
                'total_referring',
                'search_ratio',
                'organic_keywords_rolling_unique_count',
                'paid_keywords_rolling_unique_count',
                'organic_search_share',
                'paid_search_share',
                'social_ratio',
                'display_ads_ratio',
                'incoming_ads_rolling_unique_count',
                'also_visited_unique_count',
                'top_also_visited']]

        trial_0 = pd.concat([core_df,global_rank,country_rank,category_rank,traffic_sources, engagements],axis = 1)


        monthly_visits_tbl = pd.DataFrame(raw_json['estimated_monthly_visits'].items(),columns =['date','monthly_visits'])
        monthly_visits_tbl['site_name'] = trial_0['site_name']
        monthly_visits_tbl = monthly_visits_tbl.ffill(axis = 0)[['site_name','date','monthly_visits']]

        extended_df = monthly_visits_tbl.tail(1)[['site_name','date','monthly_visits']]
        extended_df = extended_df.rename(columns={"date":"start_date"})
        extended_df['end_date'] = normalized_df['daily_visits_max_date'][0]
        extended_df = extended_df.reset_index(drop=True)
        extended_df = extended_df[['site_name' ,'start_date', 'end_date', 'monthly_visits']]
        
        core_df['start_date'] = extended_df['start_date'][0]
        core_df['end_date'] = extended_df['end_date'][0]
        
        core_df = core_df[['site_name', 'start_date', 'end_date', 'category', 'title', 'description', 'total_countries',
        'referrals_ratio', 'total_referring', 'search_ratio',
        'organic_keywords_rolling_unique_count',
        'paid_keywords_rolling_unique_count', 'organic_search_share',
        'paid_search_share', 'social_ratio', 'display_ads_ratio',
        'incoming_ads_rolling_unique_count', 'also_visited_unique_count',
        'top_also_visited']]
        
        
        if (len(top_country_shares[top_country_shares['nation_alpha3'] == 'USA']['nation_share']) > 0) and (top_country_shares[top_country_shares['nation_alpha3'] == 'USA']['nation_share'].reset_index(drop=True)[0] > 0):
            try:
                extended_df['usa_visits'] = (extended_df['monthly_visits'][0] * top_country_shares[top_country_shares['nation_alpha3'] == 'USA']['nation_share']).astype(np.int64).reset_index(drop=True)[0]
            except KeyError:
                extended_df['usa_visits'] = (extended_df['monthly_visits'][0] * top_country_shares[top_country_shares['nation_alpha3'] == 'USA']['nation_share']).astype(np.int64).reset_index(drop=True)[1]
        else:
            extended_df['usa_visits'] = 0
        
        extended_df['global_visits'] = (extended_df['monthly_visits'][0] -  extended_df['usa_visits']).astype(np.int64)


        fill_columns = ['site_name','start_date','end_date','monthly_visits','usa_visits','global_visits']
        domain_monthly_information = pd.concat([extended_df,
                top_categories,
                top_referring,
                top_destinations,
                top_country_shares,
                top_org_keywords,
                top_paid_keywords,
                top_publishers,
                top_ad_networks,],axis=1)
        domain_monthly_information.loc[:,fill_columns] = domain_monthly_information.loc[:,fill_columns].ffill()

        fill_columns = ['site_name','start_date','end_date']
        similar_sites_tbl = pd.concat([extended_df[['site_name' ,'start_date', 'end_date']],
                similar_sites,
                similar_sites_by_rank,
                tags],axis=1)

        similar_sites_tbl.loc[:,fill_columns] = similar_sites_tbl.loc[:,fill_columns].ffill()

        # Ensure dataframes have correct column dtypes
        core_df = column_type_adjustment(core_df)
        domain_monthly_information = column_type_adjustment(domain_monthly_information)
        similar_sites_tbl = column_type_adjustment(similar_sites_tbl)
        
        if desired_output == 'categorization':
            results = [domain_monthly_information]
        elif desired_output == 'descriptive':
            results = [core_df, domain_monthly_information]
        elif desired_output == 'all':
            results = [core_df, domain_monthly_information,similar_sites_tbl]
    else:
        if desired_output == 'categorization':
            results = [empty_categorization_lite()]
        elif desired_output == 'descriptive':
            results = [empty_core_lite(), empty_categorization_lite()]
        elif desired_output == 'all':
            results = [empty_core_lite(), empty_categorization_lite(),empty_similar_sites_lite()]
        
    return(results)

def similarweb_us_only(domain=STRING,start_date=STRING,end_date=STRING,country='us',lead_enrichment_api=False):
    """
    Hits the SimilarWeb endpoints for data tied to any specific geography. 
    Consumes a minimum of 5 api calls per domain fed, 6 if lead enrichment is set to true.
    API endpoints being hit:
        https://docs.api.similarweb.com/#d0d305b4-0aa6-4d34-b1ba-33d3465c8722
    
        https://docs.api.similarweb.com/#6692c4b7-ede0-4429-9333-8d87f272c2c5
        https://docs.api.similarweb.com/#04d3aabe-49e4-4d40-8946-fca05f007700
        
        https://docs.api.similarweb.com/#ff1734fc-9c0b-4e14-8b56-717d22fb4356
        https://docs.api.similarweb.com/#d43b417a-908d-4e34-9ca5-927944fe62c3
        
        And finally, utilizes the `similarweb_lite` function for the final call.
    Args:
        domain (str): Domain to query SimilarWeb for. Defaults to STRING.
        start_date (str, optional): Endpoint requires an str date formatted as '%Y-%m'. Defaults to STRING.
        end_date (str, optional): Endpoint requires an str date formatted as '%Y-%m'. Defaults to STRING.
        country (str, optional): Country of interest. See available options here: https://en.wikipedia.org/wiki/ISO_3166-1_numeric. Defaults to 'us'.
        lead_enrichment_api (bool, optional): On (True)/Off(False) switch for utilizing the lead_enrichment endpoint. Defaults to False.
    """
    try:
        if lead_enrichment_api == True:
            url =  'https://api.similarweb.com/v1/website/'
            # Production - Pull Key from resources json
            api_key = ''.join( pd.read_json('/root/.config/similarweb/similarweb_secret.json',orient='index').iloc[0,:].apply(str).values)
            
            endpoint = '/lead-enrichment/all?'
            api_call_url = url+domain+endpoint+'api_key='+api_key+'&start_date='+start_date+'&end_date='+end_date+'&country='+country+'&main_domain_only=false&format=json&show_verified=false'
            lead_results  = r.get(api_call_url)
            
            if lead_results.status_code == 200:
                lead_enrichment_elements = unpack_lead_enrichment(api_content=lead_results.content)
                
                lead_enrichment =lead_enrichment_elements[0]
                
                lead_enrichment_core = lead_enrichment_elements[1]
                
                descriptive_element = lead_enrichment_elements[2]
                
                traffic_share = lead_enrichment_elements[3]
                
                incoming_total_visits = lead_enrichment_elements[4]

            else:
                
                lead_enrichment = empty_lead_enrichment()
                
                lead_enrichment_core = empty_lead_enrichment_core()
                
                descriptive_element = empty_descriptive_elements(domain=domain,country=country)
                
                traffic_share = empty_traffic_share()
                
                incoming_total_visits = 0
                
        # Desktop Incoming
        endpoint = '/traffic-sources/referrals?'
        api_call_url = url+domain+endpoint+'api_key='+api_key+'&start_date='+start_date+'&end_date='+end_date+'&country='+country+'&main_domain_only=false&format=json'
        desktop_incoming_results  = r.get(api_call_url)
        
        if desktop_incoming_results.status_code == 200:
            desktop_incoming_json = unpack_desktop_incoming(api_content=desktop_incoming_results.content)[0]
            desktop_incoming_domains = unpack_desktop_incoming(api_content=desktop_incoming_results.content)[1]
        else:
            desktop_incoming_json = {'visits': 0}
            desktop_incoming_domains = pd.DataFrame(columns=['desktop_share',	'domain',	'desktop_change',	'desktop_referred_visits'])
        
        # Mobile Incoming
        endpoint = '/traffic-sources/mobileweb-referrals?'
        api_call_url = url+domain+endpoint+'api_key='+api_key+'&start_date='+start_date+'&end_date='+end_date+'&country='+country+'&main_domain_only=false&format=json'
        mobile_incoming_results  = r.get(api_call_url)
        
        if mobile_incoming_results.status_code == 200:
            mobile_incoming_json = unpack_mobile_incoming(mobile_incoming_results.content)[0]
            mobile_incoming_domains = unpack_mobile_incoming(mobile_incoming_results.content)[1]
        else:
            mobile_incoming_json = {'visits': 0}
            mobile_incoming_domains = pd.DataFrame(columns=['mobile_share','domain','mobile_change','mobile_referred_visits'])
        
        incoming_traffic_tbl = merge_incoming_traffic(incoming_total_visits=incoming_total_visits,
                                                    desktop_incoming=desktop_incoming_domains,
                                                    mobile_incoming=mobile_incoming_domains,
                                                    desktop_json=desktop_incoming_json,
                                                    mobile_json=mobile_incoming_json,
                                                    descriptive_element=descriptive_element,
                                                    traffic_share=traffic_share)
        
        # GET Mobile Web Outgoing Referrals
        # https://api.similarweb.com/v1/website/bbc.com/traffic-sources/mobileweb-outgoing-referrals?api_key={{similarweb_api_key}}&start_date=2020-01&end_date=2020-03&country=gb&main_domain_only=false&format=json
        # GET Desktop Organic Outgoing Links
        # https://api.similarweb.com/v1/website/bbc.com/traffic-sources/outgoing-referrals?api_key={{similarweb_api_key}}&start_date=2020-01&end_date=2020-03&country=gb&main_domain_only=false&format=json

        # Desktop Outgoing
        endpoint = '/traffic-sources/outgoing-referrals?'
        api_call_url = url+domain+endpoint+'api_key='+api_key+'&start_date='+start_date+'&end_date='+end_date+'&country='+country+'&main_domain_only=false&format=json'
        desktop_outgoing_results  = r.get(api_call_url)
        
        if desktop_outgoing_results.status_code == 200:
            desktop_outgoing_json = unpack_desktop_outgoing(api_content=desktop_outgoing_results.content)[0]
            desktop_outgoing_domains = unpack_desktop_outgoing(api_content=desktop_outgoing_results.content)[1]
        else:
            desktop_outgoing_json = {'visits': 0}
            desktop_outgoing_domains = empty_desktop_outgoing_domains()
        # Mobile Outgoing
        endpoint = '/traffic-sources/mobileweb-outgoing-referrals?'
        api_call_url = url+domain+endpoint+'api_key='+api_key+'&start_date='+end_date+'&end_date='+end_date+'&country='+country+'&main_domain_only=false&format=json'
        mobile_outgoing_results  = r.get(api_call_url)
        
        if mobile_outgoing_results.status_code == 200:    
            mobile_outgoing_json = unpack_mobile_outgoing(mobile_outgoing_results.content)[0]
            mobile_outgoing_domains = unpack_mobile_outgoing(mobile_outgoing_results.content)[1]
        else:
            mobile_outgoing_json = {'visits': 0}
            mobile_outgoing_domains = empty_mobile_outgoing_domains()
        
        outgoing_traffic_tbl = merge_outgoing_traffic(desktop_outgoing=desktop_outgoing_domains,
                                                    mobile_outgoing=mobile_outgoing_domains,
                                                    desktop_json=desktop_outgoing_json,
                                                    mobile_json=mobile_outgoing_json,
                                                    descriptive_element=descriptive_element,
                                                    traffic_share=traffic_share)
            
        # ------------------    
        us_domain_traffic = pd.concat([incoming_traffic_tbl,outgoing_traffic_tbl],axis=0)
        
        categorization_df = similarweb_lite(domain=domain, desired_output = 'categorization')[0][['site_name','start_date','end_date','super_category','sub_category']]
        
        categorization_df = categorization_df[categorization_df['super_category'] != '']
        
        if lead_enrichment_api == True:
            results = [categorization_df,us_domain_traffic,lead_enrichment_core, lead_enrichment]
        else:
            results = [categorization_df,us_domain_traffic]
        

        return(results)
    except:
        print('Error Occurred for Domain: '+str(domain)+'. Moving On.')
      
def similarweb_unpack_results_into_dfs(us_final_results):
    """
    Takes in the results of the `similarweb_us_only` or *uncategorized* `similarweb_lite`. These results contain
    a list of dataframes that need too be parsed out into individual & separated df's for clickhouse ingestion.
    Args:
        us_final_results (list): A list of dataframes
    """
    if len(us_final_results[0]) > 2:
        domain_categorization = pd.DataFrame()
        us_domain_results = pd.DataFrame()
        us_core_results = pd.DataFrame()
        us_monthly_results = pd.DataFrame()

        for i in range((len(us_final_results))):
            for j in range((len(us_final_results[i]))):
                try:
                    if j == 0:
                        domain_categorization  = pd.concat([domain_categorization, us_final_results[i][j]])
                    elif j == 1:
                        us_domain_results = pd.concat([us_domain_results,us_final_results[i][j]])
                    elif j == 2:
                        us_core_results = pd.concat([us_core_results, us_final_results[i][j]])
                    elif j == 3:
                        us_monthly_results = pd.concat([us_monthly_results,us_final_results[i][j]])
                except:
                    pass
                        

        
        domain_categorization = domain_categorization[domain_categorization['super_category'].notnull()]
        domain_categorization['process_date'] = date.today().strftime("%Y-%m-%d")
        domain_categorization = domain_categorization[['process_date','site_name', 'start_date', 'end_date', 'super_category','sub_category']].rename(columns={"site_name":"domain"})
        
        int_cols = [col for col in us_domain_results.columns if 'visit' in col]
        us_domain_results[int_cols] = us_domain_results[int_cols].fillna(0).astype(np.int64)
                
        us_core_results['unique_visitors'] = round(us_core_results['unique_visitors'],0).astype(np.int64)

        us_monthly_results = us_monthly_results.iloc[:,0:7].dropna().rename(columns={"share":"source_share"})
        
        results = [domain_categorization,us_domain_results,us_core_results,us_monthly_results]

        return(results)
    else:
        domain_categorization = pd.DataFrame()
        us_domain_results = pd.DataFrame()

        for i in range((len(us_final_results))):
            for j in range((len(us_final_results[i]))):
                if j == 0:
                    domain_categorization  = pd.concat([domain_categorization, us_final_results[i][j]])
                elif j == 1:
                    us_domain_results = pd.concat([us_domain_results,us_final_results[i][j]])
        
        
        domain_categorization.dropna()
        domain_categorization['process_date'] = date.today().strftime("%Y-%m-%d")
        domain_categorization = domain_categorization[['process_date','site_name', 'start_date', 'end_date', 'super_category','sub_category']]

        int_cols = [col for col in us_domain_results.columns if 'visit' in col]
        us_domain_results[int_cols] = us_domain_results[int_cols].fillna(0).astype(np.int64)
          
        
        results = [domain_categorization,us_domain_results]
        
        return(results)

def insert_data_into_ch(insert_table_name=STRING,data=pd.DataFrame):
    """
    Inserts the data into Clickhouse Tables directly. If the desired tables do not yet exist,
    this will set the schemas and create them. If they do already exist, this will append the new data.
    Args:
        insert_table_name (str): Must be one of the following: 
                                    similarweb_categorization, 
                                    similarweb_traffic,
                                    similarweb_domain_details,
                                    similarweb_source. 
                                Defaults to STRING.
        data (pd.DataFrame): DataFrame to be ingested into clickhouse. Defaults to pd.DataFrame.
    """
    client = Client('192.168.101.80', database='addotnet', settings={'use_numpy': True})
    
    sql_query = "SHOW TABLES"
    
    available_tables = client.query_dataframe(sql_query)
    
    table_exists_detection_logic = available_tables[available_tables['name']==insert_table_name]
    
    
    if table_exists_detection_logic.shape[0]==0:
        if insert_table_name == 'similarweb_categorization':
            client.execute("CREATE TABLE addotnet."+insert_table_name+" (`process_date` Date, `domain` String,`start_date` Date,`end_date` Date,`super_category` String,`sub_category` String ) ENGINE = MergeTree() ORDER BY process_date SETTINGS index_granularity=8192")
            print(insert_table_name+" Table has been created in Clickhouse!")
        elif insert_table_name == 'similarweb_traffic':
            client.execute("CREATE TABLE addotnet."+insert_table_name+" (`process_date` Date, `domain` String,`start_date` Date,`end_date` Date,`country` String,`desktop_share` Float64,`mobile_share` Float64,`domain_of_interest` String,`desktop_referred_visits` Int64,`mobile_referred_visits` Int64,`domain_incoming_visits` Int64,`referred_visits` Int64,`total_visits` Int64,`domain_type` String,`desktop_outgoing_visits` Int64,`mobile_outgoing_visits` Int64,`domain_outgoing_visits` Int64,`outgoing_visits` Int64 ) ENGINE = MergeTree() ORDER BY process_date SETTINGS index_granularity=8192")
            print(insert_table_name+" Table has been created in Clickhouse!")
        elif insert_table_name == 'similarweb_domain_details':
            client.execute("CREATE TABLE addotnet."+insert_table_name+" (`process_date` Date, `domain` String,`start_date` Date,`end_date` Date,`country` String,`desktop_share` Float64,`mobile_share` Float64,`global_rank` Int64,`employee_range` String, `site_category` String,`est_revenue` String, `pages_per_visit` Float64, `visits` Int64,`mom_growth` Float64, `unique_visitors` Int64, `bounce_rate` Float64, `average_visit_duration` Float64 ) ENGINE = MergeTree() ORDER BY process_date SETTINGS index_granularity=8192")
            print(insert_table_name+" Table has been created in Clickhouse!")
        elif insert_table_name == 'similarweb_source':
            client.execute("CREATE TABLE addotnet."+insert_table_name+" (`process_date` Date, `domain` String,`start_date` Date,`end_date` Date,`country` String,`source_type` String, `source_share` Float64 ) ENGINE = MergeTree() ORDER BY process_date SETTINGS index_granularity=8192")
            print(insert_table_name+" Table has been created in Clickhouse!")
        else:
            print('Please enter one of the following insert_table_names: similarweb_categorization, similarweb_traffic, similarweb_domain_details, similarweb_source')
    else:
        print(insert_table_name+' exists in Clickhouse Already!')

    
    client.insert_dataframe(
        'INSERT INTO addotnet.'+insert_table_name+' VALUES',
        data
    )
    print("Data successfully loaded into "+insert_table_name)


