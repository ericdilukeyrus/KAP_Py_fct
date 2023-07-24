import datetime
import logging
import pandas as pd
import requests
from snowflake.snowpark.session import Session
from datetime import date
from calendar import Calendar, monthrange

import azure.functions as func

#Global variables


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function ran at %s', utc_timestamp)

    #Run every 1st of the month to revrieve previous month rate
    previous_month = date.today() + pd.DateOffset(months=0)
    
    for i in range(1, 18):
        day = str(previous_month.year) + '-' + ('0' +str(previous_month.month))[-2:] + '-' + ('0' +str(i))[-2:]     
        print(day)   
        fx_rates_url = f'http://api.exchangeratesapi.io/v1/{day}?access_key=cd9624ad550600360db79c4386b2a9ac&base=EUR&symbols=EUR,USD,CAD,GBP,BRL,SGD'
    
        #Read json data from api
        fx_rates_json_data = read_fx_api(fx_rates_url)
        print(fx_rates_json_data)

        #Read json to Pandas df
        fx_rates_pd_df = pd_reads_json(fx_rates_json_data)
        print(fx_rates_pd_df)

        #Snowflake
        sf_session = create_sf_session()
        pd_write_to_sf(sf_session, fx_rates_pd_df)
        sf_session.close()
    

def read_fx_api(url):
    json_data = pd.json_normalize(requests.get(url).json(), max_level=1)
    return json_data

def create_sf_session():
    connection_parameters = {
        "account": "vz31470.west-europe.azure",
        "user": "DBT_DEV",
        "password": "dbtPassword123",
        "role": "TRANSFORM_DEV",
        "warehouse": "COMPUTE_DEV_WH",
        "database": "OCEAN_DEV",
        "schema": "OCEAN_STG"
    }

    session = Session.builder.configs(connection_parameters).create()
    return session

def pd_reads_json(fx_json_data):
    # Rename the columns
    new_columns = ['SUCCESS', 'TIMESTAMP', 'HISTORICAL', 'INPUTCURRENCY', 'DATE', 'EUR','USD','CAD','GBP','BRL','SGD']   
    
    pd_df = pd.DataFrame(fx_json_data)
    pd_df.columns = new_columns
    
    return pd_df

def pd_write_to_sf(sf_session, pd_df):
    table_name = 'S_API_FX_RATES'
    sf_session.write_pandas(pd_df,table_name,auto_create_table = True, overwrite=False)
    
    