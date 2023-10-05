import logging
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
import azure.functions as func

#Global variable
container_name_az = str("mbr-landing")

urlTeams = "https://keyrusgroup.webhook.office.com/webhookb2/68b15510-2653-4855-be23-14cd5190e969@168e48b2-81f0-4aac-bc77-d58d07d205e2/IncomingWebhook/1217263db75b4b1ea586455578c14fef/7d069be0-a9ad-4d5d-9109-8a307e57a11d"
urlTeamsBU = "https://keyrusgroup.webhook.office.com/webhookb2/68b15510-2653-4855-be23-14cd5190e969@168e48b2-81f0-4aac-bc77-d58d07d205e2/IncomingWebhook/5d27e0016d2746efbdebd7444319945f/7d069be0-a9ad-4d5d-9109-8a307e57a11d"
headerTeams = {'Content-Type':'application/json'}

def main(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
            f"Name: {myblob.name}\n"
            f"Blob Size: {myblob.length} bytes")
    
    time.sleep(15) #Wait copy file completed
    az_connection_string = os.getenv('AzureWebJobsStorage')
    blob_service_client = BlobServiceClient.from_connection_string(az_connection_string)
    container_client = blob_service_client.get_container_client(container_name_az)
   
    blob_name = myblob.name.split("/")[1]
    blob_client_instance = blob_service_client.get_blob_client(container_name_az, blob_name, snapshot=None)
    blob_data = blob_client_instance.download_blob()
   
    #Teams
    msgTeams_2_4 =  {"text":"1/3 - File <b>" + blob_name + "</b> received  at " + datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}
    response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams_2_4))

    #MBR or BUDGET
    if 'BUDGET' in blob_name.upper().strip(): 
        try:
            loadBudgetInferAndPersist(blob_data,blob_name)
        except Exception as e :
            #Teams
            msgTeams =  {"text":"Error File <b>" + blob_name + "</b>, execption : " + str(e)} 
            response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams))
            
            blob_client_instance.delete_blob()  
    else :        
        try: 
            snowpark_df, mbr_scope, mbr_env = loadInferAndPersist(blob_data,blob_name)
            num_rows = snowpark_df.count()
            mbr_env_nm = 'DEV' if mbr_env=='D' else 'PROD'        
    
            if num_rows > 2:
                #Teams
                msgTeams_3_4 =  {"text":"2/3 - File <b>" + blob_name + "</b> raw data loaded in " + mbr_env_nm + " db (MBR Scope : <b>" + mbr_scope +"</b>)"}
                response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams_3_4))

                schedule_status, schedule_name = run_paradygme_schedule(mbr_scope,mbr_env)

                # Now you can do something with the status
                if schedule_status == "SUCCESS":
                    #Teams
                    msgTeams_4_4 =  {"text":"3/3 - File <b>" + blob_name + "</b> loaded in DWH. Schedule Success : <b>" +  schedule_name + "</b>"} 
                    response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams_4_4))

                    blob_client_instance.delete_blob()            
                else:
                    #Teams
                    msgTeams_4_4 =  {"text":"3/3 - File <b>" + blob_name + "</b> not loaded in DWH. Schedule Error : <b>" +  schedule_name + "</b>"} 
                    response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams_4_4))
        except Exception as e :
            #Teams
            msgTeams =  {"text":"Error File <b>" + blob_name + "</b>, execption : " + str(e)} 
            response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams))
            
            blob_client_instance.delete_blob()  
                

def excel_to_df(input_file_path):

    # load the parameters from the worksheet they are the starting point for everything
    df_MBRscope = pd.read_excel(input_file_path,"MBR Parameters",skiprows=1, usecols="C", nrows=1, header=None, names=["Value"]).iloc[0]["Value"]
    df_MBRmonth = pd.read_excel(input_file_path,"MBR Parameters",skiprows=3, usecols="C", nrows=1, header=None, names=["Value"]).iloc[0]["Value"]

    # Load the table with BU'S from the "MBR Parameters" worksheet
    df_MBRparams = pd.read_excel(input_file_path,"MBR Parameters", header=6, usecols='F:H',nrows=11)
    # we are only interested in these columns
    new_MBR_parameters_columns = ["BU_Code","BU_Name","Currency_Code"]
    df_MBRparams.columns = new_MBR_parameters_columns
    # since we defined to import 10 rows, see nrows = 11 (zero based index) we want to drop all rows that are NaN on the existing df
    df_MBRparams.dropna(subset=['BU_Code', 'BU_Name', 'Currency_Code'], inplace=True)

    # Load the entire MBR excel file so we can iterate over the different worksheet that we need
    df_xls = pd.ExcelFile(input_file_path)

    return df_MBRscope, df_MBRmonth, df_MBRparams, df_xls

def excel_budget_to_df(input_file_path):    
    #Load the settings from the worksheet
    df_Period = pd.read_excel(input_file_path,"Settings",skiprows=2,usecols="C",nrows=1, header=None,names=["Value"]).iloc[0]["Value"]
    df_BU = pd.read_excel(input_file_path, "Settings", skiprows=5, usecols="C", nrows=1, header=None, names=["Value"] ).iloc[0]["Value"]
    df_Currency = pd.read_excel(input_file_path, "Settings", skiprows=9, usecols="C", nrows=1, header=None, names=["Value"] ).iloc[0]["Value"]

    #Load the entire Budget file so we can iterate over the differen worksheet that we need
    df_xls = pd.ExcelFile(input_file_path)

    return df_BU, df_Period, df_Currency, df_xls

# cache the data
# @st.cache_data
# The function `loadInferAndPersist()` is decorated with `st.cache_data` but it returns an unevaluated dataframe
# of type `snowflake.snowpark.table.Table`. Please call `collect()` or `to_pandas()` on the dataframe before returning it,
# so `st.cache_data` can serialize and cache it.
def loadInferAndPersist(file,file_name):
    connection_sf = "creds.json"
    # connect to Snowflake Prod
    with open(connection_sf) as f:
        connection_parameters = json.load(f)  
    session_prod = Session.builder.configs(connection_parameters).create()

    # connect to Snowflake Dev
    with open("creds_dev.json") as f:
            connection_parameters = json.load(f)  
    session_dev = Session.builder.configs(connection_parameters).create()

    #call the excel to dataframe function
    fileXlx = file.readall()
    MBRscope, MBRmonth, MBRparams, xls = excel_to_df(fileXlx)

    #Connect to Snowflake // Retrieve the status of the MBR Scope (DEV or PROD)
    mbr_scope_state = session_prod.sql(f'''
    SELECT MBR_SCOPE, IS_ACTIVE, DEV_PROD, CONTACT_NM
    FROM OCEAN_ADM.MBR_SOURCES WHERE MBR_SCOPE = '{MBRscope}' ''').collect()

    mbr_env = mbr_scope_state[0][2] 
    #if(str.upper(mbr_env)== 'D'):
    #    session_prod.close()
    #else :
    #    session_dev.close()
        
    # Back to logic to load new data for a MBRscope excel document
    # List of column names we want to keep, I used the index because the structure of the file should not be changed
    columns_to_be_read = [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 46, 47, 48, 151]

    # Rename the columns
    new_columns = ['Index', 'Anaplant_Index', 'PBI_Index', 'K_Nature', 'Actual_LY_01', 'Actual_LY_02', 'Actual_LY_03', 
                    'Actual_LY_04', 'Actual_LY_05', 'Actual_LY_06', 'Actual_LY_07', 'Actual_LY_08', 'Actual_LY_09', 
                    'Actual_LY_10', 'Actual_LY_11', 'Actual_LY_12', 'Budget_CY_01', 'Budget_CY_02', 'Budget_CY_03', 
                    'Budget_CY_04', 'Budget_CY_05', 'Budget_CY_06', 'Budget_CY_07', 'Budget_CY_08', 'Budget_CY_09', 
                    'Budget_CY_10', 'Budget_CY_11', 'Budget_CY_12', 'Actual_CY_01', 'Actual_CY_02', 'Actual_CY_03', 
                    'Actual_CY_04', 'Actual_CY_05', 'Actual_CY_06', 'Actual_CY_07', 'Actual_CY_08', 'Actual_CY_09', 
                    'Actual_CY_10', 'Actual_CY_11', 'Actual_CY_12', 'Actual_NY_01', 'Actual_NY_02', 'Actual_NY_03', 'CostCenter_Code']
    
    #License & Maintenance sheet name
    licMainSheetName = str()

    for index, row in MBRparams.iterrows():
        BU_code = row['BU_Code']
        BU_name = row['BU_Name']
        Currency_Code = row['Currency_Code']
        # Load a sheet into a DataFrame by its name
        for sheet_name in xls.sheet_names:
            #Retrieve License & Maintenance correct sheet name 
            if 'LICENSE' in sheet_name.upper().strip() and 'MAINTENANCE' in sheet_name.upper().strip():
                licMainSheetName = sheet_name

            # Determine the row to start importing data based on the sheet name
            if sheet_name.endswith("FI_" + BU_code):
                skiprows = 8 # 
                #get all the sheets that contain the P&L data, one by one 
                df_Finance = pd.read_excel(xls, sheet_name, skiprows=skiprows, usecols=columns_to_be_read)
                # apply the new column names
                df_Finance.columns = new_columns
                # add the additional columns
                df_Finance['BU_Code'] = BU_code
                df_Finance['BU_Name'] = BU_name
                df_Finance['Currency_Code'] = Currency_Code
                df_Finance['MBR_Scope'] = str(MBRscope)
                df_Finance['MBR_Month'] = str(MBRmonth)
                df_Finance['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                df_Finance['MBR_FileName'] = str(file_name)
                # change the columns order so they are identical to the tables that have been created on snowflake
                df_Finance = df_Finance[['Index', 'Anaplant_Index', 'PBI_Index', 'K_Nature', 'Actual_LY_01', 'Actual_LY_02', 'Actual_LY_03', 
                    'Actual_LY_04', 'Actual_LY_05', 'Actual_LY_06', 'Actual_LY_07', 'Actual_LY_08', 'Actual_LY_09', 
                    'Actual_LY_10', 'Actual_LY_11', 'Actual_LY_12', 'Budget_CY_01', 'Budget_CY_02', 'Budget_CY_03', 
                    'Budget_CY_04', 'Budget_CY_05', 'Budget_CY_06', 'Budget_CY_07', 'Budget_CY_08', 'Budget_CY_09', 
                    'Budget_CY_10', 'Budget_CY_11', 'Budget_CY_12', 'Actual_CY_01', 'Actual_CY_02', 'Actual_CY_03', 
                    'Actual_CY_04', 'Actual_CY_05', 'Actual_CY_06', 'Actual_CY_07', 'Actual_CY_08', 'Actual_CY_09', 
                    'Actual_CY_10', 'Actual_CY_11', 'Actual_CY_12', 'Actual_NY_01', 'Actual_NY_02', 'Actual_NY_03', 
                    'BU_Code', 'BU_Name', 'Currency_Code', 'MBR_Scope', 'MBR_Month', 'CostCenter_Code', 'CREATED_ON','MBR_FileName' ]]
                #Keep data until line 473
                df_Finance = df_Finance.head(473)
                # compose a new valid tablename for the P&L file
                table_name = sheet_name.replace("P&L ", "R_PL_").replace("FI", "FI_" + str(MBRscope).upper())
                # write the data to the P&L tables on snowflake
                try:
                    if(mbr_env== 'P'):
                        snow_df = session_prod.write_pandas(df_Finance,table_name,auto_create_table = True, overwrite=True)
                    else :
                        snow_df = session_dev.write_pandas(df_Finance,table_name,auto_create_table = True, overwrite=True)
                except Exception as e :
                    #Slack
                    #slack_client.chat_postMessage(channel="#kap", text="3/4 - File " + file_name + " - Error in P&L sheet '" + sheet_name + "' (Skipped). Error : " + str(e))
                    #Teams
                    msgTeams =  {"text":"2/3 - File <b>" + file_name + "</b> - Error in P&L sheet <b>'" + sheet_name + "'</b> (Skipped). Error : " + str(e)} 
                    response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams))

                    print(e)
                    pass
            
    
    ###############################################################################################################################                
    # Load the "KPI Pyramid" worksheet
    # used Polars because the pandas.read_excel() was taking a very, very long time 
    df_polars = pl.read_excel(
            io.BytesIO(fileXlx) ,
            sheet_name="KPI Pyramid",  
        xlsx2csv_options={"skip_empty_lines": False,"skip_hidden_rows": False},
        read_csv_options={"has_header": False, "new_columns": ["ANCHOR","BU", "VERSION", "PERIOD", "COST_CENTER", "PEOPLE_TYPE", "LEVEL_SENIORITY", 
                    "ENDOFMONTH_EFT", "SRVC_SALES_BEF_BONIMALI", "BILLABLE_DAYS", "DAILY_RATE", 
                    "ANNUAL_DIRECT_COSTS", "ANNUAL_PRODUCTION_DAYS", "DAILY_COST"]},  
                )
    #drop the columns we do need
    df_new = df_polars.drop("ANCHOR") 
    # keep only the first 13 columns
    df_new = df_new.select(df_new.columns[:13])
    # remove the lines we do not need the info for the KPI pyramid only starts from line 78
    df_new = df_new.slice(78, len(df_polars)-78)
    # remove rows where column "BU" is null
    df_new = df_new.filter(pl.col("BU").is_not_null())
    

    # convert to pandas dataframe because the snowloader is dependant on pandas datatypes
    # KPI = df_new.to_pandas()


    # Add additional columns
        # convert the monthname year to a valid end of month date
    KPI = df_new.to_pandas()
    KPI['PERIOD'] = pd.to_datetime(KPI['PERIOD'], format="%b-%y") + pd.offsets.MonthEnd(1)
    # KPI['PERIOD'] = KPI['PERIOD'].dt.date
    # converted it to string again, in the dbt model there's a convertsion to a date 
    KPI['PERIOD'] = KPI['PERIOD'].dt.strftime('%Y-%m-%d')

    KPI['CREATED_ON'] = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    # snowflake is expecting a string value and not a date, dbt is failing on this
    KPI['MBR_MONTH'] = str(MBRmonth)
    KPI['MBR_FileName'] = str(file_name)
    # Compose new table name
    table_name = "KPI Pyramid".replace("KPI ", "R_KPI_").replace("Pyramid", "PYRAMID_" + str(MBRscope).upper())
    try:
        if(mbr_env== 'P'):
            snow_df =session_prod.write_pandas(KPI,table_name,auto_create_table = True, overwrite=True)
        else:
            snow_df =session_dev.write_pandas(KPI,table_name,auto_create_table = True, overwrite=True)
    except Exception as e:
        #Slack
        #slack_client.chat_postMessage(channel="#kap", text="3/4 - File " + file_name + " - Error in KPI Pyramid sheet '" + sheet_name + "' (Skipped). Error : " + str(e))
        #Teams
        msgTeams =  {"text":"2/3 - File <b>" + file_name + "</b> - Error in KPI Pyramid sheet <b>'" + sheet_name + "'</b> (Skipped). Error : " + str(e)} 
        response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams))
        
        pass

    ##################################################################################################################################################
    # Load "License & Maintenance" worksheet    
    try:
        df_polars_lm = pl.read_excel(
                io.BytesIO(fileXlx) ,
                sheet_name=licMainSheetName,  
            xlsx2csv_options={"skip_empty_lines": False,"skip_hidden_rows": False,"infer_schema_length" : 10000},
            read_csv_options={"has_header": False, "new_columns": ["ANCHOR","BU", "VERSION", "PERIOD", "SOFTWARE_PARENT", 
                        "REV_LIC_PERPETUAL", "REV_LIC_NEW_SUBSCRIPTION", "REV_MAINT_1STYEAR","REV_LIC_RENEWED_SUBSCRIPTION","REV_MAINT_RENEWAL","REV_LIC_REFERRALS","TOTAL_REVENUE",
                        "CP_LICPUR_PERPETUAL", "CP_LICPUR_NEW_SUBSCRIPTION", "CP_MAINTPUR_1STYEAR","CP_LICPUR_RENEWED_SUBSCRIPTION","CP_MAINT_RENEWAL","TOTAL_COST"], 
                        "dtypes":{"TOTAL_COST":str} 
                        }  
                    )
        
        #drop the columns we do need
        df_new_lm = df_polars_lm.drop("ANCHOR") 
        # keep only the first 13 columns
        df_new_lm = df_new_lm.select(df_new_lm.columns[:17])
        # remove the lines we do not need the info for the KPI pyramid only starts from line 78
        df_new_lm = df_new_lm.slice(125, len(df_polars_lm)-78)
        # remove rows where column "BU" is null
        df_new_lm = df_new_lm.filter(pl.col("BU").is_not_null())

        # Add additional columns
        # convert the monthname year to a valid end of month date
        LIC_MAIN = df_new_lm.to_pandas()
        #LIC_MAIN['PERIOD'] = pd.to_datetime(LIC_MAIN['PERIOD'], format="%m-%d-%Y") 
        # KPI['PERIOD'] = KPI['PERIOD'].dt.date
        # converted it to string again, in the dbt model there's a convertsion to a date 
        #LIC_MAIN['PERIOD'] = LIC_MAIN['PERIOD'].dt.strftime('%Y-%m-%d')

        LIC_MAIN['CREATED_ON'] = datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        # snowflake is expecting a string value and not a date, dbt is failing on this
        LIC_MAIN['MBR_MONTH'] = str(MBRmonth)
        LIC_MAIN['MBR_FileName'] = str(file_name)
        # Compose new table name
        table_name_lm =  "R_LIC_MAINT_"  + str(MBRscope).upper()
        
        if(mbr_env== 'P'):
            snow_df =session_prod.write_pandas(LIC_MAIN,table_name_lm,auto_create_table = True, overwrite=True)
        else : 
            snow_df =session_dev.write_pandas(LIC_MAIN,table_name_lm,auto_create_table = True, overwrite=True)

    except Exception as e: 
        #Slack
        #slack_client.chat_postMessage(channel="#kap", text="3/4 - File " + file_name + " - Error in Lice & Maintenance sheet (Skipped). Error : " + str(e))       
        #Teams
        msgTeams =  {"text":"2/3 - File <b>" + file_name + "</b> - Error in Lice & Maintenance sheet (Skipped). Error : " + str(e)} 
        response = requests.post(urlTeams, headers=headerTeams, data = json.dumps(msgTeams))

        pass        

    return snow_df, MBRscope,mbr_env

def loadBudgetInferAndPersist(file, file_name):
    connection_sf = "creds_bud_dev.json"
    #Connect to SF
    with open(connection_sf) as f:
        connection_parameters = json.load(f)
    session_dev = Session.builder.configs(connection_parameters).create()

    #Call the excel to dataframe function
    fileXlx = file.readall()
    BUD_BU, BUD_Period, BUD_Curr, xls = excel_budget_to_df(fileXlx)

    # Back to logic to load new data for a BUDGET excel document       
    
    #Load aé sheet into a dataframe by its name
    for sheet_name in xls.sheet_names:
        #Load P&L worksheet
        if sheet_name == "P&L FI_BU01":
            # List of column names we want to keep, I used the index because the structure of the file should not be changed
            columns_to_be_read = [0, 1, 2, 4, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 151]
            # Rename the columns
            new_columns = ['INDEX', 'ANAPLANT_INDEX', 'PBI_INDEX', 'K_NATURE', 'BUDGET_CY_01', 'BUDGET_CY_02', 'BUDGET_CY_03', 
                    'BUDGET_CY_04', 'BUDGET_CY_05', 'BUDGET_CY_06', 'BUDGET_CY_07', 'BUDGET_CY_08', 'BUDGET_CY_09', 
                    'BUDGET_CY_10', 'BUDGET_CY_11', 'BUDGET_CY_12', 'COSTCENTER_CODE']
    
            skiprows = 8
            #Get the sheet that contains the P&L data
            df_finance = pd.read_excel(xls,sheet_name,skiprows=skiprows,usecols=columns_to_be_read)
            #Apply the new column names
            df_finance.columns=new_columns
            #Add additional columns
            df_finance['BU_NAME'] = str(BUD_BU).strip()
            df_finance['BUD_PERIOD'] = str(BUD_Period)
            df_finance['BUD_CURRENCY'] = BUD_Curr
            df_finance['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_finance['BUD_FILENAME'] = str(file_name)
  
            #Keep data until line 573
            df_finance = df_finance.head(573)
            #Compose a new valid tablename for the P&L sheet
            table_name = "R_BUD_PL_" + str(BUD_BU).replace(" ","_").upper()
            
            try:
                snow_df = session_dev.write_pandas(df_finance,table_name,auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e)

        #Load KPI Pyramid worksheet
        if sheet_name == "KPI Pyramid":
            #KPI Pyramid
            # columns_to_be_read_kpi = [1, 2, 3,4,5,6,7,8,9,10,11,12,13]
            # Rename the columns
            new_columns_kpi = ['BU', 'SCENARIO', 'PERIOD', 'COST_CENTER', 'PEOPLE_TYPE', 'LEVEL_SENIORITY', 
                    'ENDOFMONTH_EFT', 'BILLABLE_DAYS', 'INTERNAL_PROJECT', 'PRE_SALES_DAYS', 'TRAINING_DAYS', 'INACTIVITY_DAYS','HOLIDAYS',
                    'SICK_DAYS', 'TOTAL_DAYS', 'OCCUPANCY_RATE', 'SRVC_SALES_BEF_BONIMALI',  'DAILY_RATE', 'ANNUAL_PACKAGE_COSTS', 'ANNUAL_PRODUCTION_DAYS', 
                    'DAILY_COST','DAILY_MARGIN']
               
            skiprows = 125
            #Get the sheet that contains KPI Pyramid data
            df_kpi = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:W', converters={k: str for k in range(22)})
            #Apply the new columns names 
            df_kpi.columns = new_columns_kpi
            #Add additional columns
            df_kpi['BU_NAME'] = str(BUD_BU).strip()
            df_kpi['BUD_PERIOD'] = str(BUD_Period)
            df_kpi['BUD_CURRENCY'] = BUD_Curr
            df_kpi['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_kpi['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            df_kpi = df_kpi.head(2000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_KPI_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_kpi, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        
        #Load L&M worksheet
        if sheet_name == "License & Maintenance":
            #L&M
            # Rename the columns
            new_columns_lm = ['BU', 'VERSION', 'PERIOD', 'SOFTWARE_PARENT', 'REV_LIC_PERPETUAL', 'REV_LIC_NEW_SUBSCRIPTION', 'REV_LIC_IC_SALES',
                       'REV_MAINT_1STYEAR', 'REV_MAINT_IC_SALES','REV_LIC_RENEWED_SUBSCRIPTION','REV_MAINT_RENEWAL','REV_LIC_REFERRALS','TOTAL_REVENUE',
                        'CP_LICPUR_PERPETUAL', 'CP_LICPUR_NEW_SUBSCRIPTION','CP_LICPUR_IC', 'CP_MAINTPUR_1STYEAR', 'CP_MAINT_IC_SUBCONTRACT',
                        'CP_LICPUR_RENEWED_SUBSCRIPTION','CP_MAINT_RENEWAL','TOTAL_COST']
                
            skiprows = 128
            #Get the sheet that contains KPI Pyramid data
            df_lm = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:V', converters={k: str for k in range(21)})
            #Apply the new columns names
            df_lm.columns = new_columns_lm
            #Add additional columns
            df_lm['BU_NAME'] = str(BUD_BU).strip()
            df_lm['BUD_PERIOD'] = str(BUD_Period)
            df_lm['BUD_CURRENCY'] = BUD_Curr
            df_lm['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_lm['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_LM_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_lm, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        
        #Load Client worksheet
        if sheet_name== "Clients":
            #Clients
            #Rename the columns
            new_columns_cli = ['BU','SCENARIO','PERIOD','CLIENT_NAME','PEOPLE_TYPE','LEVEL_SENIORITY','TOTAL_SIGNING','BACKLOG','PIPELINE','BLUE_SKY','TOTAL','MARGIN','MARGIN_PERC']
     
            skiprows = 44
            #Get the sheet that contains KPI Pyramid data
            df_cli = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:N', converters={k: str for k in range(13)})
            #Apply the new columns names
            df_cli.columns = new_columns_cli
            #Add additional columns
            df_cli['BU_NAME'] = str(BUD_BU).strip()
            df_cli['BUD_PERIOD'] = str(BUD_Period)
            df_cli['BUD_CURRENCY'] = BUD_Curr
            df_cli['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_cli['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            #df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_CLI_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_cli, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        
        #Load Revenue distribution
        if sheet_name == "Revenue distribution":
            #Rename the columns
            new_columns_rev_dis = ['BU','SCENARIO','PERIOD','CLIENT_NAME','PEOPLE_TYPE','LEVEL_SENIORITY','TOTAL_SIGNING','BACKLOG','PIPELINE','BLUE_SKY','TOTAL']

            skiprows = 44
            #Get the sheet that contains KPI Pyramid data
            df_rev_dis = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:L', converters={k: str for k in range(11)})
            #Apply the new columns names
            df_rev_dis.columns = new_columns_rev_dis
            #Add additional columns
            df_rev_dis['BU_NAME'] = str(BUD_BU).strip()
            df_rev_dis['BUD_PERIOD'] = str(BUD_Period)
            df_rev_dis['BUD_CURRENCY'] = BUD_Curr
            df_rev_dis['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_rev_dis['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            #df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_REVDIS_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_rev_dis, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 

        #Load Signing & Pipeline
        if sheet_name == "Signing & Pipeline":
            #Rename the columns
            new_columns_sig_pip = ['BU','SCENARIO','PERIOD','PRODUCTION_PERIOD','BUSINESS_LINE','EMPTY','TOTAL_SIGNING']

            skiprows = 115
            #Get the sheet that contains KPI Pyramid data
            df_sig_pip = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols='B:H', converters={k: str for k in range(7)})
            #Apply the new columns names
            df_sig_pip.columns = new_columns_sig_pip
            #Add additional columns
            df_sig_pip['BU_NAME'] = str(BUD_BU).strip()
            df_sig_pip['BUD_PERIOD'] = str(BUD_Period)
            df_sig_pip['BUD_CURRENCY'] = BUD_Curr
            df_sig_pip['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_sig_pip['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            #df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_SIGPIP_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_sig_pip, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 
        
        #Load IC Declaration
        if sheet_name == "IC declaration":
            # List of column names we want to keep, I used the index because the structure of the file should not be changed
            columns_to_be_read = [1,2,3,4, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30]
            #Rename the columns
            new_columns_ic_decl = ['BU','COL1','COL2','IC_PARTNER','BUDGET_CY_01','BUDGET_CY_02','BUDGET_CY_03','BUDGET_CY_04','BUDGET_CY_05',
                                   'BUDGET_CY_06','BUDGET_CY_07','BUDGET_CY_08','BUDGET_CY_09','BUDGET_CY_10','BUDGET_CY_11','BUDGET_CY_12']

            skiprows = 10
            #Get the sheet that contains KPI Pyramid data
            df_ic_decl = pd.read_excel(xls,sheet_name, skiprows=skiprows,usecols=columns_to_be_read, converters={k: str for k in range(16)})
            #Apply the new columns names
            df_ic_decl.columns = new_columns_ic_decl
            #Add additional columns
            df_ic_decl['BU_NAME'] = str(BUD_BU).strip()
            df_ic_decl['BUD_PERIOD'] = str(BUD_Period)
            df_ic_decl['BUD_CURRENCY'] = BUD_Curr
            df_ic_decl['CREATED_ON'] =  datetime.now().astimezone(pytz.timezone('Europe/Paris')).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
            df_ic_decl['BUD_FILENAME'] = str(file_name)

            #Change the columns order so they are identical o the tables that have been created on SF
            #df_lm = df_lm.head(4000)
            #Compose a new valid tablename for the KPI Pyramid sheet
            table_name = "R_BUD_ICDECL_" + str(BUD_BU).replace(" ","_").upper()

            try:
                snow_df = session_dev.write_pandas(df_ic_decl, table_name, auto_create_table=True, overwrite=True)
            except Exception as e:
                print(e) 




def run_paradygme_schedule(mbr_scope, mbr_env):

    def _extract_gql_response(request: requests.Response, query_name: str, field: str) -> str:
        response_json = request.json()
        if "errors" in response_json:
            raise Exception(f"{response_json['errors']}")
        try:
            return response_json["data"][query_name][field]
        except (TypeError, KeyError) as e:
            raise ValueError(f"{e}: {response_json}")


    # The URL, key, and secret would need to be obtained from Paradime
    url = "https://api.paradime.io/api/v1/sha8vppvucjfvxwa/graphql"
    headers = {
            "Content-Type": "application/json",
            "X-API-KEY": "dq47d74m6kwkgg37r7gk2zyvghfyw6zu",
            "X-API-SECRET": "gv7ajlg4ga50vl2e95620g33fieh1ahlkpkdpj2iw75phuwntmi1ca26amji61qph4dqywtl4zwplqfdt26vjybu5qcq05an",
    }

    bolt_schedule_name = "operations_run_" + str(mbr_scope).lower()

    if mbr_env == 'D' :
        bolt_schedule_name = bolt_schedule_name + '_dev'
        
    # Define the GraphQL query for triggering a Bolt run
    query = """
        mutation trigger($scheduleName: String!) {
        triggerBoltRun(scheduleName: $scheduleName){
            runId
        }
        }
    """
    variables = {"scheduleName": bolt_schedule_name}  # Replace with your actual schedule name

    data = {"query": query, "variables": variables}
    response = requests.post(url, headers=headers, json=data) # make the request to start the Bolt job

    if response.status_code == 200:
        print(response.json())
    else:
        print(f"Request failed with status code {response.status_code}")

    # Parse the response to get the Bolt run ID
    run_id = _extract_gql_response(response, "triggerBoltRun", "runId")

    # Now we can use this ID to get the status of the Bolt run
    bolt_run_status_query = """
        query Status($runId: Int!) {
        boltRunStatus(runId: $runId) {
            state
        }
    }
    """

     # Keep checking the status until the run is complete
    while True:
        response = requests.post(
            url, json={"query": bolt_run_status_query, "variables": {"runId": int(run_id)}}, headers=headers
        )
        status = response.json()["data"]["boltRunStatus"]["state"]
        #st.write(status)
        if status in ["SUCCESS", "FAILED"]:
            break        
        # Sleep for a while before checking again
        time.sleep(10)
    
    # Now you can do something with the status
    if status == "SUCCESS":        
        print("Snowflake DWH refreshed! Start the PowerBI refresh")
        return "SUCCESS", bolt_schedule_name
    else:
        return "ERROR", bolt_schedule_name
        print("Snowflake DWH refresh failed!")      
