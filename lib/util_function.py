# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 16:20:43 2022

@author: aless
"""
#In this script we define all utility function that are not directly used in Causal Impact methodology

import json
import boto3
from config_dir import config
from datetime import date, timedelta,datetime
from calendar import monthrange


#Create function to read snowflake data
# create function for impot of data by using spark

spark = config.spark_session

def get_dbutils(spark):
        try:
            from pyspark.dbutils import DBUtils
            dbutils = DBUtils(spark)
        except ImportError:
            import IPython
            dbutils = IPython.get_ipython().user_ns["dbutils"]
        return dbutils
    
def read_table(start,end):
    dbutils = get_dbutils(spark = spark)
    #Define connection 
    sfUser = dbutils.secrets.get(scope="snowflake", key="username")
    sfPassword = dbutils.secrets.get(scope="snowflake", key="password")
    sfUrl = dbutils.secrets.get(scope="snowflake", key="host")
    #add secrets
    options = {
      "sfUrl": sfUrl,
      "sfUser": sfUser,
      "sfPassword": sfPassword,
      "sfDatabase": "DEEP_PURPLE",
      "sfSchema": "CDS",
      "sfWarehouse": "DATABRICKS_WH"
    }
    #read data
    df = spark.read \
    .format("snowflake") \
    .options(**options) \
    .option("query", f"""select * from agg_growth""") \
    .load()
    #filter data
    out = df.filter((df["DATE"] >= start) & (df["DATE"] <= end))
    return out

#Define function to get start and end of month
def get_complete_month_start_end(start,end,campaign_start):
    #Go behind actual start because of successive specificty test
    campaign_start = datetime.strptime(campaign_start,"%Y-%m-%d")
    campaign_duration = (datetime.strptime(end,"%Y-%m-%d") - campaign_start).days
    #Compute end and campaign_start for specifity test
    spec_end = campaign_start - timedelta(days = 1)
    spec_campaign_start = spec_end - timedelta(days = campaign_duration)
    #Compute duration of pre-intervention period used in the analysis
    training_period = (campaign_start -  datetime.strptime(start,"%Y-%m-%d")).days
    #Compute start of observation period for specificity test
    spec_start = spec_campaign_start - timedelta(days =  training_period)
    #Add this only for completeness of imported table
    complete_start = date(spec_start.year, spec_start.month, 1)
    complete_end = date(datetime.strptime(end, "%Y-%m-%d").year, datetime.strptime(end, "%Y-%m-%d").month, monthrange(datetime.strptime(end, "%Y-%m-%d").year, datetime.strptime(end, "%Y-%m-%d").month)[1])
    return complete_start, complete_end

#Define a function to break time interval in months
def get_date_list_breakdown(start,end,campaign_start):
    month_start, month_end = get_complete_month_start_end(start = start,end = end, campaign_start = campaign_start)
    list_out = []
    current = month_start
    while current <= month_end:
        start_int = current
        end_int = date(current.year, current.month, monthrange(current.year, current.month)[1])
        list_el = tuple([str(start_int),str(end_int)])
        list_out.append(list_el)
        current = end_int + timedelta(days = 1)
    return list_out    


def get_snowflake_data(start,end,campaign_start):
    #Get dbuitls
    dbutils = get_dbutils(spark = spark)
    #Empty temporary table the table by using fictional dates
    table = read_table(start = "2030-01-01",end = "2030-01-10")
    table.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date").save("/tmp/delta/temp_table")
    #Read spark data
    df = spark.read.load("/tmp/delta/temp_table")
    #Write spark data
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("default.ci_anl_table")
    #Delete temporary table
    dbutils.fs.rm('/tmp/delta/temp_table',recurse=True)
    #iteratively fill S3 table
    date_list = get_date_list_breakdown(start = start, end = end, campaign_start = campaign_start)
    for (start_int,end_int) in date_list:
        print('processing',start_int,end_int)
        table = read_table(start = start_int,end = end_int)
        table.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("date").save("/tmp/delta/test")
        df = spark.read.load("/tmp/delta/test")
        df.write.format("delta").mode("append").option("overwriteSchema", "true").saveAsTable("default.ci_anl_table")
        dbutils.fs.rm('/tmp/delta/temp_table',recurse=True)
        print('completed',start_int,end_int)
    return

def initialize_query_mapping():
    #Initialize query mapping file
    json.dump({}, open("/tmp/%s" % "query_mapping.txt",'w'),indent="")
    s3 = boto3.client('s3')
    #Upload file from temporary storage place to final storage location
    response = s3.upload_file("/tmp/%s" % "query_mapping.txt", 'databricks-redshift-preply', 'alessandro_picca/%s' % "query_mapping.txt")
    return


def list_s3_file():
    s3 = boto3.client("s3")
    model_list = []
    for obj in s3.list_objects_v2(Bucket='databricks-redshift-preply',Prefix = "alessandro_picca")["Contents"]:
        model_list.append(obj['Key'].replace("alessandro_picca/","").replace(".pickle","").replace(".txt","").replace("_spec",""))
        #print(obj['Key'].replace("alessandro_picca/","").replace(".pickle","").replace(".txt","").replace("_spec",""))
    model_list.remove("query_mapping")
    model_list = list(set(model_list))
    for i in model_list:
        print(i)
    return

def delete_model_from_s3(pickle_name, spec = False):
    if spec == False:
        pickle_name_total = pickle_name
    else:
        pickle_name_total = pickle_name + "_spec"
        
    s3 = boto3.client("s3")
    s3.delete_object(Bucket='databricks-redshift-preply', Key="alessandro_picca/%s" % pickle_name + ".pickle")
    return
