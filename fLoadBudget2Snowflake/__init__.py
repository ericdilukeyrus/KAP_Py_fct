import logging
import time, os
import json
import pandas as pd
import snowflake.snowpark as snowpark
import pytz
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from snowflake.snowpark import Session, DataFrame
from datetime import datetime
import azure.functions as func

#Global variable
container_name_az = str("kap-budget")

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {myblob.name}\n"
                 f"Blob Size: {myblob.length} bytes")
    #time.sleep(15) #Wait copy file completed
    az_connection_string = os.getenv('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(az_connection_string)
    container_client = blob_service_client.get_container_client(container_name_az)

    blob_name = myblob.name.split("/")[1]
    blob_client_instance = blob_service_client.get_blob_client(container_name_az, blob_name, snapshot=None)
    blob_data = blob_client_instance.download_blob()

    loadInferAndPersist(blob_data, blob_name)


def excel_to_df(input_file_path):    
    #Load the settings from the worksheet
    df_Period = pd.read_excel(input_file_path,"Settings",skiprows=2,usecols="C",nrows=1, header=None,names=["Value"]).iloc[0]["Value"]
    df_BU = pd.read_excel(input_file_path, "Settings", skiprows=8, usecols="C", nrows=1, header=None, names=["Value"] ).iloc[0]["Value"]
    df_Currency = pd.read_excel(input_file_path, "Settings", skiprows=9, usecols="C", nrows=1, header=None, names=["Value"] ).iloc[0]["Value"]

    #Load the entire Budget file so we can iterate over the differen worksheet that we need
    df_xls = pd.ExcelFile(input_file_path)

    return df_BU, df_Period, df_Currency, df_xls


def loadInferAndPersist(file, file_name):
    connection_sf = "creds_bud_dev.json"
    #Connect to SF
    with open(connection_sf) as f:
        connection_parameters = json.load(f)
    session_dev = Session.builder.configs(connection_parameters).create()

    #Call the excel to dataframe function
    fileXlx = file.readall()
    BUD_BU, BUD_Period, BUD_Curr, xls = excel_to_df(fileXlx)

    # Back to logic to load new data for a BUDGET excel document
    # List of column names we want to keep, I used the index because the structure of the file should not be changed
    columns_to_be_read = [0, 1, 2, 4, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 151]
    # Rename the columns
    new_columns = ['INDEX', 'ANAPLANT_INDEX', 'PBI_INDEX', 'K_NATURE', 'BUDGET_CY_01', 'BUDGET_CY_02', 'BUDGET_CY_03', 
                    'BUDGET_CY_04', 'BUDGET_CY_05', 'BUDGET_CY_06', 'BUDGET_CY_07', 'BUDGET_CY_08', 'BUDGET_CY_09', 
                    'BUDGET_CY_10', 'BUDGET_CY_11', 'BUDGET_CY_12', 'COSTCENTER_CODE']
    
    #KPI Pyramid
    # columns_to_be_read_kpi = [1, 2, 3,4,5,6,7,8,9,10,11,12,13]
    # Rename the columns
    new_columns_kpi = ['BU', 'VERSION', 'PERIOD', 'COST_CENTER', 'PEOPLE_TYPE', 'LEVEL_SENIORITY', 
                    'ENDOFMONTH_EFT', 'SRVC_SALES_BEF_BONIMALI', 'BILLABLE_DAYS', 'DAILY_RATE', 
                    'ANNUAL_DIRECT_COSTS', 'ANNUAL_PRODUCTION_DAYS', 'DAILY_COST']
    
    #L&M
    # Rename the columns
    new_columns_lm = ['BU', 'VERSION', 'PERIOD', 'SOFTWARE_PARENT', 'REV_LIC_PERPETUAL', 'REV_LIC_NEW_SUBSCRIPTION', 
                       'REV_MAINT_1STYEAR','REV_LIC_RENEWED_SUBSCRIPTION','REV_MAINT_RENEWAL','REV_LIC_REFERRALS','TOTAL_REVENUE',
                        'CP_LICPUR_PERPETUAL', 'CP_LICPUR_NEW_SUBSCRIPTION', 'CP_MAINTPUR_1STYEAR','CP_LICPUR_RENEWED_SUBSCRIPTION',
                        'CP_MAINT_RENEWAL','TOTAL_COST']
    
    #Load aé sheet into a dataframe by its name
    for sheet_name in xls.sheet_names:
        if sheet_name == "P&L FI":
            skiprows = 8
            #Get the sheet that contains the P&L data
            df_finance = pd.read_excel(xls,sheet_name,skiprows=skiprows,usecols=columns_to_be_read)
            #Apply the new column names
            df_finance.columns=new_columns
            #Add additional columns
            df_finance['BU_NAME'] = str(BUD_BU).strip()
            df_finance['BUD_PERIOD'] = BUD_Period
            df_finance['BUD_CURRENCY'] = BUD_Curr
            df_finance['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_finance['BUD_FILENAME'] = str(file_name)
  
            #Keep data until line 573
            df_finance = df_finance.head(573)
            #Compose a new valid tablename for the P&L sheet
            table_name = "R_BUD_PL_" + str(BUD_BU).replace(" ","").upper()
            
            try:
                snow_df = session_dev.write_pandas(df_finance,table_name,auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e)

        #Load KPI Pyramid worksheet
        if sheet_name == "KPI Pyramid":
            skiprows = 77
            #Get the sheet that contains KPI Pyramid data
            df_kpi = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:N')
            #Apply the new columns names 
            df_kpi.columns = new_columns_kpi
            #Add additional columns
            df_kpi['BU_NAME'] = str(BUD_BU).strip()
            df_kpi['BUD_PERIOD'] = BUD_Period
            df_kpi['BUD_CURRENCY'] = BUD_Curr
            df_kpi['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_kpi['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            df_kpi = df_kpi.head(2000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_KPI_" + str(BUD_BU).replace(" ","").upper()

            try:
                snow_df = session_dev.write_pandas(df_kpi, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        
        #Load L&M worksheet
        if sheet_name == "License & Maintenance":
            skiprows = 124
            #Get the sheet that contains KPI Pyramid data
            df_lm = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:R', converters={'SOFTWARE_PARENT':str})
            #Apply the new columns names
            df_lm.columns = new_columns_lm
            #Add additional columns
            df_lm['BU_NAME'] = str(BUD_BU).strip()
            df_lm['BUD_PERIOD'] = BUD_Period
            df_lm['BUD_CURRENCY'] = BUD_Curr
            df_lm['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_lm['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_LM_" + str(BUD_BU).replace(" ","").upper()

            try:
                snow_df = session_dev.write_pandas(df_lm, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        