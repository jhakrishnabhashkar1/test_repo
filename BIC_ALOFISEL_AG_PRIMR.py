# Databricks notebook source
pip install s3fs fsspec

# COMMAND ----------

# DBTITLE 1,Required Packages
import logging
import boto3
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import regexp_replace

# COMMAND ----------

# DBTITLE 1,Logging Configuration
try:
  directory = '/dbfs/../tmp/'
  prefix = 'BIC_ALOFISEL_AG_PRIMER_LOG'
  file_name = prefix + '_' + datetime.now().strftime("%Y%m%d%H%M%S") + '.log'
  local_log_path = directory + file_name
  logger = logging.getLogger(prefix)
  logger.propagate = False
  logger.setLevel(logging.DEBUG)
  FileHandler = logging.FileHandler(local_log_path, mode = 'w')
  formatter = logging.Formatter('%(asctime)s - %(levelname)s: %(message)s',datefmt ='%m/%d/%Y %I:%M:%S %p')
  FileHandler.setFormatter(formatter)
  logger.addHandler(FileHandler)
  logger.info(' Setting logger completed. ')
  logger.info(' Log file name: ' + file_name)
except Exception as error:
  raise error

# COMMAND ----------

# DBTITLE 1,User Defined Functions
# Method to upload the log file in S3 Location
def upload_log_file(local_path,bucket,folder_path,file_name):
  """
  local_path [argument] - from local
  """
  try:
    s3client = boto3.client('s3')
    logger.info('Moving log file from local to s3 location')
    s3client.upload_file(local_path,bucket,folder_path+file_name)
    dbutils.fs.rm(local_path, True)
    print("File sucessfully moved to s3 location")
    logger.info('Log file sucessfully moved to s3 location')
  except Exception as error:
    logger.error(' Error while moving the log file to s3' + str(error))
    raise error


# Method to extract the AG Data and return in DataFrame.
def ag_data_load(D_Data_table,Discoverant_Manifest_table,Ag_name)-> DataFrame:
  try:
    logger.info(' Starting AG Data extract Operation -> in ag_data_load method')
    
    # fetching Data From Discoverant_EDB_PD_AG table based on AG_Name
    ag_data_df = spark.sql("select * from {0} where AG_ID in (select AG_ID from (select AG_ID, ROW_NUMBER() over (partition by AG_ID order by AUD_LD_DTS desc) as ROW_KEY from {1} where AG_NAME in ({2}) ))".format(D_Data_table,Discoverant_Manifest_table,Ag_name)).toPandas()
    ag_data_df['PROCESS_STEP'] = ag_data_df['AG_PATH'].apply(lambda row: row.split('\\')[-1])
    ag_data_df['FIELD_NAME'] = ag_data_df['PARAMETER'].apply(lambda row: row.split('.')[-1])
    ag_data_df['REPLICATE_ROW'] = ag_data_df['FIELD_NAME'].apply(lambda row: (row.split('_')[-1]) if((row.split('_')[-1]).isnumeric()) else '0' )
    ag_data_df['FIELD_NAME'] = ag_data_df['FIELD_NAME'].apply(lambda row:  (row.split('_',1)) )
    ag_data_df['FIELD_NAME'] = ag_data_df['FIELD_NAME'].str[0]
    ag_data_df['SUBPROCESS_STEP'] = ag_data_df['PARAMETER'].apply(lambda row: ".".join(row.split('.')[0:-1])) 
    
    # trimming last '.' from the parameter string
    ag_data_df['PARAMETER'] = ag_data_df['PARAMETER'].apply(lambda row: (row.rstrip("\.") ))
    
    # Getting MCS FDS and FP from BOOK template name
    ag_data_df['PROCESS_STEP'] = ag_data_df['PROCESS_STEP'].str.split('_').str[0]
    
    # Adding and Renaming columns in ag dataframe
    ag_data_df = ag_data_df.reindex(columns = ag_data_df.columns.tolist() + ['PAGE_INDEX','WEEK','EXPANSION'])
    ag_data_df['SOURCE'] = 'AG'
    ag_data_df.rename(columns= {'PS_NAME': 'BATCH','PARAMETER': 'PATH','REPLICATE_ROW':'ROW_NUMBER'},inplace=True)
    # Creating Final Df with required Fileds
    Clean_ag_data_df = ag_data_df[['SOURCE','PROCESS_STEP','SUBPROCESS_STEP','BATCH','FIELD_NAME','PATH','PS_DATE','TEXT_VALUE','DATE_VALUE','NUMBER_VALUE','ROW_NUMBER','PAGE_INDEX','WEEK','EXPANSION']]
    
  except Exception as error:
    logger.error(' Error while retrieving the data' + str(error))
    upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
    raise error   
  return Clean_ag_data_df


# Method to extract the PRIMER Data and return in DataFrame.
def primer_data_load(Mv_48_Path)-> DataFrame:
  try:
      logger.info(' Starting PRIMER Data extract Operation -> in primer_data_load method')
      
      # Reading the MV-48 File.
      primer_rd_df= pd.read_csv("{}".format(Mv_48_Path), encoding= 'unicode_escape',dtype=str)
      # Filtring data From Primer Dataframe.
      primer_rd_df =primer_rd_df[( primer_rd_df['SCOPE'].str.contains('Yes') ) & (~ primer_rd_df['SCOPE'].isna()) & (primer_rd_df['TYPE'] !='AG')]
      # Renaming the Mv-48 columns based on Primer and Ag Standards
      primer_rd_df = primer_rd_df.rename(columns={'PS_NAME_BOOK_TEMPLATE_NAME': 'BOOK_TEMPLATE_NAME','PS_NAME_PAGE_NAME':'PAGE_NAME','DISCO_FIELD_NAME':'FIELD_NAME'})
  
  except Exception as error:
    logger.error(' Error while retrieving the data' + str(error))
    upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
    raise error    
  return primer_rd_df


# Method to extract the Discoverant Data and return in DataFrame.
def discoverant_primer_data(primer_rd_df,Discoverant_Primer_table,Discoverant_Book_Template_Name)-> DataFrame:
  try:
    logger.info(' Starting Discoverant Data extract Operation -> in discoverant_primer_data method')
    
    cols=['BOOK_TEMPLATE_NAME', 'BOOK_NAME', 'PAGE_NAME','ROW_NUMBER', 'TEXT_VALUE','PAGE_INDEX','CREATION_DATE']
    
    # Reading from discoverant Primer table.
    discoverant_df = spark.sql("select * from {0} where BOOK_TEMPLATE_NAME in  ({1})".format(Discoverant_Primer_table,Discoverant_Book_Template_Name)).toPandas().astype(str) 
    # Filtring and Joining the dataframe data.
    df_week = discoverant_df[(discoverant_df['FIELD_NAME'].str.contains('week'))].loc[:,cols]
    df_expansion = discoverant_df[(discoverant_df['FIELD_NAME'].str.contains('expansion'))].loc[:,cols]
    df_process_step = discoverant_df[(discoverant_df['FIELD_NAME'].str.contains('process_step'))].loc[:,cols] 
    df_process_step = df_process_step.rename(columns={'TEXT_VALUE':'PROCESS_STEP'})
    df_expansion = df_expansion.rename(columns={'TEXT_VALUE':'EXPANSION'})
    df_week = df_week.rename(columns={'TEXT_VALUE':'WEEK'})
    sql_merge_col = pd.merge(discoverant_df, df_week, how='left', on=['BOOK_TEMPLATE_NAME', 'BOOK_NAME', 'PAGE_NAME','ROW_NUMBER','PAGE_INDEX','CREATION_DATE'])
    sql_merge_col = pd.merge(sql_merge_col, df_expansion, how='left', on=['BOOK_TEMPLATE_NAME', 'BOOK_NAME', 'PAGE_NAME','ROW_NUMBER','PAGE_INDEX','CREATION_DATE'])
    sql_merge_col = pd.merge(sql_merge_col, df_process_step, how='left', on=['BOOK_TEMPLATE_NAME','BOOK_NAME','PAGE_NAME','ROW_NUMBER','PAGE_INDEX','CREATION_DATE'])
    # Filter 'Node' data.
    config_v48 = primer_rd_df[primer_rd_df['ITEM_TYPE']!='Node']
    final_data = pd.merge(sql_merge_col.astype(str), config_v48.astype(str), how='inner', on=['BOOK_TEMPLATE_NAME','PAGE_NAME','WEEK','EXPANSION','PROCESS_STEP','FIELD_NAME'])
    # Removing last dot from the path
    final_data['AG_PATH'] = final_data['AG_PATH'].apply(lambda row: (row.rstrip("\.") )) 
    final_data['NEW_SUBPROCESS'] = final_data['AG_PATH'].apply(lambda row: ('.'.join(row.split('.')[:-1]) ))
    # Added Souce column in dataframe
    final_data['SOURCE'] = 'PRIMR'  
    # Replacing _IPC_QC with Blank Space
    final_data["BOOK_TEMPLATE_NAME"]= final_data["BOOK_TEMPLATE_NAME"].str.replace('_IPC_QC','')
    # Getting MCS FDS and FP from BOOK template name
    final_data['PROCESS_STEP'] = final_data['BOOK_TEMPLATE_NAME'].str.split('_').str[-1]
    
    # Getting FIELD_NAME from PATH
    final_data['FIELD_NAME'] = final_data['AG_PATH'].str.split('.').str[-1]
    
    # trimming last '.' from the parameter string
    final_data['AG_PATH'] = final_data['AG_PATH'].apply(lambda row: (row.rstrip("\.") ))
    # Renaming and Selecting Columns.
    final_data.rename(columns= {'NEW_SUBPROCESS': 'SUBPROCESS_STEP','BOOK_NAME': 'BATCH','AG_PATH':'PATH','CREATION_DATE':'PS_DATE'},inplace=True)
    final_data = final_data[['SOURCE','PROCESS_STEP','SUBPROCESS_STEP','BATCH','FIELD_NAME','PATH','PS_DATE','TEXT_VALUE','DATE_VALUE','NUMBER_VALUE','ROW_NUMBER','PAGE_INDEX','WEEK','EXPANSION']]
    
  except Exception as error:
    logger.error(' Error while retrieving the data' + str(error))
    upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
    raise error     
  return final_data


def data_cleaning(Clean_Finaldf)-> DataFrame:
  try:
    logger.info(' Starting data cleaning Operation -> in data_cleaning method')
    
    # Filter out the records where FIELD_NAME == Parameter Set Date
    Clean_Finaldf = Finaldf.filter(Finaldf.FIELD_NAME != "Parameter Set Date")
    # Filter out the Batch ID mentioned in parameters 
    list_batch = list(Batch_Id.split('||'))
    for batch in list_batch:
      Clean_Finaldf = Clean_Finaldf.filter(Clean_Finaldf.BATCH != batch)
    # Removing IPC-QC from BAtch column 
    Clean_Finaldf = Clean_Finaldf.withColumn("BATCH", regexp_replace("BATCH", "-IPC-QC", ""))
    
    # Replacing some missmatch vales with NULL
    null_list = ['','nan','NaT']
    for i in null_list:
      Clean_Finaldf = Clean_Finaldf.replace(i,None)
  
  except Exception as error:
    logger.error(' Error while retrieving the data' + str(error))
    upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
    raise error 

  return Clean_Finaldf

# COMMAND ----------

# DBTITLE 1,Widgets to get parameters
try:
  logger.info(' Widget parameterizibg started')
  
  dbutils.widgets.text('Batch_Id', '', 'BATCH_ID')
  dbutils.widgets.text('Ag_name', '', 'AG_NAME')
  dbutils.widgets.text('Mv_48_Path', '', 'MV_48_PATH')
  dbutils.widgets.text('Discoverant_Primer_table', '', 'DISCOVERANT_PRIMER_TABLE')
  dbutils.widgets.text('D_Data_table', '', 'D_DATA_TABLE')
  dbutils.widgets.text('Discoverant_Manifest_table', '', 'DISCOVERANT_MANIFEST_TABLE')
  dbutils.widgets.text('Log_bucket', '', 'LOG_BUCKET')
  dbutils.widgets.text('Log_folder_path', '', 'LOG_FOLDER_PATH')
  dbutils.widgets.text('Discoverant_Book_Template_Name', '', 'DISCOVERANT_BOOK_TEMPLATE_NAME')
  
  logger.info(' Widget parameterizibg ENDED')
except Exception as error:
  logger.error(' Error while parmeterizing' + str(error))
  upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
  raise error

# COMMAND ----------

# DBTITLE 1,Assigning variables to the parameters
try:
  logger.info(' Assigning variables to the parameters')
  
  Batch_Id = dbutils.widgets.get('Batch_Id').strip()
  Ag_name = dbutils.widgets.get('Ag_name').strip()
  Mv_48_Path = dbutils.widgets.get('Mv_48_Path').strip()
  Discoverant_Primer_table = dbutils.widgets.get('Discoverant_Primer_table').strip()
  D_Data_table = dbutils.widgets.get('D_Data_table').strip()
  Discoverant_Manifest_table = dbutils.widgets.get('Discoverant_Manifest_table').strip()
  Log_bucket = dbutils.widgets.get('Log_bucket').strip()
  Log_folder_path = dbutils.widgets.get('Log_folder_path').strip()
  Discoverant_Book_Template_Name = dbutils.widgets.get('Discoverant_Book_Template_Name').strip()
  
  logger.info(' Assigning variables to the parameters ENDED')
  
except Exception as error:
  logger.error(' Error while retrieving the data' + str(error))
  upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
  raise error

# COMMAND ----------

# DBTITLE 1,Calling Function 
try:
  logger.info(' Started Call all methods  -> calling methods')
  
  # Calling AG Data Load Method.
  Clean_Final_ag_data_df = ag_data_load(D_Data_table,Discoverant_Manifest_table,Ag_name)
  Clean_Final_ag_data_df = Clean_Final_ag_data_df.astype(str)

  # Calling Primer Data Load Method.
  Clean_primer_df = primer_data_load(Mv_48_Path)
  Clean_Final_primer_df = discoverant_primer_data(Clean_primer_df,Discoverant_Primer_table,Discoverant_Book_Template_Name)

  # Union of AG and PRIMER Dataframe
  Finaldf = pd.concat([Clean_Final_ag_data_df, Clean_Final_primer_df], axis=0)

  # Converting PD->Dataframe to Pyspark->Dataframe
  Finaldf = Finaldf.dropna(axis='columns', how='all')
  Finaldf = spark.createDataFrame(Finaldf)

  # Data Cleaning Method
  Clean_Finaldf = data_cleaning(Finaldf)
  
  rename_dict = {'SOURCE':'SRC_SYS_CD','PROCESS_STEP':'PROC_STEP_CD','SUBPROCESS_STEP':'PROC_SUB_STEP_DESC','BATCH': 'BATCH_DESC','FIELD_NAME': 'FIELD_NM_DESC','PATH': 'PATH_DESC','PS_DATE': 'CRT_DTS',
                 'TEXT_VALUE': 'TXT_VAL','DATE_VALUE': 'DT_VAL','NUMBER_VALUE': 'NUM_VAL','ROW_NUMBER': 'ROW_NUM','PAGE_INDEX': 'PAGE_INDEX_NUM','WEEK': 'WK_NUM','EXPANSION': 'EXPAN_NUM'}
  
  # Renaming columns with withColumnRenamed method.
  for key, value in rename_dict.items():
      Clean_Finaldf= Clean_Finaldf.withColumnRenamed(key,value)
  
#   display(Clean_Finaldf)
  
except Exception as error:
  logger.error(' Error while retrieving the data' + str(error))
  upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
  raise error 

# COMMAND ----------

display(Clean_Finaldf)

# COMMAND ----------

# DBTITLE 1,Uploading Log File in S3
try:
  upload_log_file(local_log_path,Log_bucket,Log_folder_path,file_name)
except Exception as error:
  logger.error(' Error while uploading log file' + str(error)) 
  raise error