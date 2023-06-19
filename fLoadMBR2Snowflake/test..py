#import streamlit as st
import pandas as pd
import json
import time, os
import snowflake.snowpark as snowpark
import re
#import utils as u
import polars as pl
import pytz
import requests
import time
import io
from snowflake.snowpark import Session, DataFrame
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import azure.functions as func


container_name_az = str("mbr-landing")
connection_sf = "creds_dev.json"
# connect to Snowflake
with open(connection_sf) as f:
    connection_parameters = json.load(f)  
session = Session.builder.configs(connection_parameters).create()

#last_run = session.sql(f'''SELECT * FROM OCEAN_ODS.CFG_LOAD_CONTEXT WHERE LOAD_ID IN (SELECT MAX(LOAD_ID)FROM OCEAN_DEV.OCEAN_ODS.CFG_LOAD_CONTEXT)''').collect()[0][3]
#print(last_run=="NR")

load_status = session.sql(f'''SELECT LOAD_STATUS FROM OCEAN_ODS.C_FCT_CONTEXT''').collect()[0][0]

if load_status == "NR":
    session.sql(f'''UPDATE OCEAN_ODS.C_FCT_CONTEXT SET LOAD_STATUS='R' WHERE LOAD_ID = 1''').collect()
    print("the new job is running currently")
    time.sleep(60)
    session.sql(f'''UPDATE OCEAN_ODS.C_FCT_CONTEXT SET LOAD_STATUS='NR' WHERE LOAD_ID = 1''').collect()
else :
    isRunning = 1
    while isRunning == 1:
        print("Waiting 15 sec and retry")
        time.sleep(15)
        load_status = session.sql(f'''SELECT LOAD_STATUS FROM OCEAN_ODS.C_FCT_CONTEXT''').collect()[0][0]
        if load_status == "NR":
            isRunning = 0
            session.sql(f'''UPDATE OCEAN_ODS.C_FCT_CONTEXT SET LOAD_STATUS='R' WHERE LOAD_ID = 1''').collect()
            time.sleep(10)
            session.sql(f'''UPDATE OCEAN_ODS.C_FCT_CONTEXT SET LOAD_STATUS='NR' WHERE LOAD_ID = 1''').collect()



