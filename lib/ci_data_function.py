# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 16:20:20 2022

@author: aless
"""
#In this script we define all function that are need for the data preparation and exploration

#Import configuration script

from config_dir import config

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame


import sys
#Create system variable for pytrend repo
sys.path.append("/Workspace/Repos/alessandro.picca@preply.com/pytrends")
#Import specific version of pytrend libary
from pytrends.request import TrendReq
from pytrends.exceptions import ResponseError

#Import all other required libraries
import importlib
import imp
import itertools
from functools import partial, reduce
from datetime import date, timedelta,datetime
import pandas as pd
import pyspark.pandas as ps
from time import sleep
from pyspark.sql.functions import *
from calendar import monthrange
import numpy as np
import re
import os
import scipy.stats
from prettytable import PrettyTable,DOUBLE_BORDER
import holidays
#import copy
from statsmodels.tsa.stattools import acf,pacf,adfuller
from statsmodels.tsa.seasonal import seasonal_decompose
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dateutil.relativedelta import relativedelta
import time
import copy
import pickle
import json
import logging
import boto3
logging.getLogger('tensorflow').disabled = True
logging.getLogger('tensorflow_probability').disabled = True
import tensorflow as tf
import tensorflow_probability as tfp
from causalimpact import CausalImpact
from causalimpact.misc import standardize
import warnings

warnings.filterwarnings('ignore')
imp.reload(config)

#Initialize spark session for all funtions

spark = config.spark_session

#Define a utility function to get month_name
@F.udf
def get_month_name_udf(x):
    name = x.strftime("%B")
    return name

#get_month_name_udf = udf(get_month_name, StringType())

#Define a function to check if the chosen model_name is already setted
def check_model_name(model_name):
    s3 = boto3.client("s3")
    model_list = []
    for obj in s3.list_objects_v2(Bucket='databricks-redshift-preply',Prefix = "alessandro_picca")["Contents"]:
        model_list.append(obj['Key'].replace("alessandro_picca/","").replace(".pickle","").replace(".txt","").replace("_spec",""))
        #print(obj['Key'].replace("alessandro_picca/","").replace(".pickle","").replace(".txt","").replace("_spec",""))
    model_list.remove("query_mapping")
    model_list = list(set(model_list))
    if model_name in model_list:
        print(f"""Warning: the chosen model_name is already present in S3, continue with this only if you want to overwrite the {model_name} object""")
    else:
        print(f"""The model_name, equal to {model_name} is available to be chosen""")
    return


#Define a function load the query_mapping file
def load_query_mapping():
    #Download existing query mapping
    s3 = boto3.client('s3')
    #Download the object from S3 final storage location and put in download location
    s3.download_file('databricks-redshift-preply', 'alessandro_picca/%s' % "query_mapping.txt", "/tmp/%s" % "query_mapping" + "_downloaded.txt")
    #Create query mapping json 
    query_mapping = json.load(open("/tmp/%s" % "query_mapping" + "_downloaded.txt","r"))
    return query_mapping

#Define a function that update the query mapping file, with the specific use case (the file is stored for tracking and checking purposes)
def update_query_mapping(config_name):
    value = f"{'config_query_' + config_name}"
    #Create query mapping json 
    query_mapping = load_query_mapping()
    if config_name not in query_mapping:
        query_mapping[config_name] = value
        #Initialize query mapping file
        json.dump(query_mapping, open("/tmp/%s" % "query_mapping.txt",'w'),indent="")
        s3 = boto3.client('s3')
        #Upload file from temporary storage place to final storage location
        response = s3.upload_file("/tmp/%s" % "query_mapping.txt", 'databricks-redshift-preply', 'alessandro_picca/%s' % "query_mapping.txt")
        print(f"Query mapping correctly configured.\nNow please create the {value + '.py'} file, in the folder config_dir, and update it with the desired query")
    else:
        print(f"""Warning: the chosen config_name is already present in the query_mapping.txt file\nPlease continue with this only if you want to use the already exhisting {value + ".py"} configuration file""")
    return

#Define the function to estend starting period for specificty test
def get_extend_start_end(start:str, end:str, campaign_start:str):
    #Retrieve campaing start from fitted model object
    campaign_start = pd.to_datetime(campaign_start,format = "%Y-%m-%d")
    campaign_duration = (pd.to_datetime(end,format = "%Y-%m-%d") - campaign_start).days
    #Compute end and campaign_start for specifity test
    spec_end = campaign_start - timedelta(days = 1)
    spec_campaign_start = spec_end - timedelta(days = campaign_duration)
    #Compute duration of pre-intervention period used in the analysis
    training_period = (campaign_start - pd.to_datetime(start,format = "%Y-%m-%d")).days
    #Compute start of observation period for specificity test
    spec_start = spec_campaign_start - timedelta(days =  training_period)
    #spec_end = str(spec_end.date())
    spec_start = str(spec_start.date())
    #spec_campaign_start = str(spec_campaign_start.date())
    #end is returned as-is because it does not need to be extended
    return spec_start, end

#Define the function to read target_metric_in_perim data
def write_target_metric_in_perim(target:str,start:str,end:str,campaign_start:str,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
        imp.reload(config_query)
        input_query = config_query.get_target_metric_in_perim_query()
        params = {'start' : start,'end' : end,'target' : target}
        out = spark.sql(input_query.format(**params))
        out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
        out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
        if list(out.columns) != ["reference_date", "campaign_dimension", "metric"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/target_metric_in_perim_" + model_name)
        return
    else:
         raise ValueError("The value of the config_name parameter does not match the configuration python file.\nPlease check the config_dir directory and try again")
        
#Define the function to read target_metric_out_perim data
def write_target_metric_out_perim(start:str,end:str,campaign_start:str,target:str,out_perim_usage:bool,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        if out_perim_usage == True:
            config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
            imp.reload(config_query)
            input_query = config_query.get_target_metric_out_perim_query()
            params = {'start' : start,'end' : end,'target' : target}
            out = spark.sql(input_query.format(**params))
            out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
            out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
            if list(out.columns) != ["reference_date", "campaign_dimension", "metric"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        else:
            print("The import of target_metric_out_perim data was disabled by setting the out_perim_usage parameter as False")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                      StructField("campaign_dimension", StringType(), True),
                                                      StructField("metric", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/target_metric_out_perim_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter does not match the configuration python file.\nPlease check the config_dir directory and try again")

#Define the function to read target_metric_control data
def write_target_metric_control(start:str,end:str,campaign_start:str,control:tuple,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
        imp.reload(config_query)
        input_query = config_query.get_target_metric_control_query()
        params = {'start' : start,'end' : end,'control' : control}
        out = spark.sql(input_query.format(**params))
        out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
        out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
        if list(out.columns) != ["reference_date", "campaign_dimension", "metric"]:
            raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/target_metric_control_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter does not match the configuration python file.\nPlease check the config_dir directory and try again")

#Define the function to read visitor_control data
def write_visitor_control(start:str,end:str,campaign_start:str,control:tuple,visitor_usage:bool,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        if visitor_usage == True:
            config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
            imp.reload(config_query)
            input_query = config_query.get_visitor_control_query()
            params = {'start' : start,'end' : end,'control' : control}
            out = spark.sql(input_query.format(**params))
            out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
            out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
            if list(out.columns) != ["reference_date", "campaign_dimension", "visitor"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        else:
            print("The import of visitor_control data was disabled by setting the visitor_control parameter as False")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                      StructField("campaign_dimension", StringType(), True),
                                                      StructField("visitor", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/visitor_control_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter does not match the configuration python file.\nPlease check the config_dir directory and try again")

#Define the function to read click_control data
def write_click_control(start:str,end:str,campaign_start:str,control:tuple,click_usage:bool,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        if click_usage == True:
            config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
            imp.reload(config_query)
            input_query = config_query.get_click_control_query()
            params = {'start' : start,'end' : end,'control' : control}
            out = spark.sql(input_query.format(**params))
            out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
            out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
            if list(out.columns) != ["reference_date", "campaign_dimension", "click"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        else:
            print("The import of click_control data was disabled by setting the click_control parameter as False")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                      StructField("campaign_dimension", StringType(), True),
                                                      StructField("click", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/click_control_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter is not included in the query mapping.\nPlease check the config.py file and try again")

#Define the function to read impression_control
def write_impression_control(start:str,end:str,campaign_start:str,control:tuple,impression_usage:bool,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        if impression_usage == True:
            config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
            imp.reload(config_query)
            input_query = config_query.get_impression_control_query()
            params = {'start' : start,'end' : end,'control' : control}
            out = spark.sql(input_query.format(**params))
            out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
            out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
            if list(out.columns) != ["reference_date", "campaign_dimension", "impression"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        else:
            print("The import of impression_control data was disabled by setting the impression_control parameter as False")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                      StructField("campaign_dimension", StringType(), True),
                                                      StructField("impression", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/impression_control_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter is not included in the query mapping.\nPlease check the config.py file and try again")

#Define the function to read npc_control
def write_npc_control(start:str,end:str,campaign_start:str,control:tuple,npc_usage:bool,config_name:str,model_name:str):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    query_mapping = load_query_mapping()
    if config_name in list(query_mapping.keys()):
        if npc_usage == True:
            config_query = importlib.import_module(f".{query_mapping[config_name]}","config_dir")
            imp.reload(config_query)
            input_query = config_query.get_npc_control_query()
            params = {'start' : start,'end' : end,'control' : control}
            out = spark.sql(input_query.format(**params))
            out = out.withColumn(out.columns[0],col(out.columns[0]).cast(DateType()))
            out = out.withColumn(out.columns[2],col(out.columns[2]).cast(DoubleType()))
            if list(out.columns) != ["reference_date", "campaign_dimension", "npc"]:
                raise ValueError(f"Something in the query configuration that define the data to be imported in this funtion is wrong.\n Please check the {query_mapping[config_name] + '.py'} file and try again")
        else:
            print("The import of npc_control data was disabled by setting the npc_control parameter as False")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                      StructField("campaign_dimension", StringType(), True),
                                                      StructField("npc", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("campaign_dimension").save("/tmp/delta/npc_control_" + model_name)
        return
    else:
        raise ValueError("The value of the config_name parameter is not included in the query mapping.\nPlease check the config.py file and try again")
        
#Utility function for import of gtrend        
def get_final_month_range(year: int, month: int, end)-> date:
    final = date(year, month, monthrange(year, month)[1])
    if final < end:
        out = final
    else:
        out = end
    return out



def get_gtrend_start_date(start: str):
    start = pd.to_datetime(start).date()
    day_of_week = start.isoweekday()
    if day_of_week != 7:
        start_gtrend = start - timedelta(days = day_of_week)
    else:
        start_gtrend = start
    return start_gtrend


def get_gtrend_end_date(end: str):
    end = pd.to_datetime(end).date()
    day_of_week = end.isoweekday()
    if day_of_week !=7:   
        end_gtrend = end + timedelta(days =  6 - day_of_week)
    else:
        end_gtrend = end + timedelta(days = day_of_week - 1)
    return end_gtrend



def convert_dates_to_timeframe(start: date, stop: date) -> str:
    """Given two dates return a string version of the interval 
    between the two dates, which is used to retrieve data"""
    return f"{start.strftime('%Y-%m-%d')} {stop.strftime('%Y-%m-%d')}"


def fetch_data(pytrends, build_payload, timeframe: str) -> pd.DataFrame:
    """Attempts to fecth data and retries in case of a ResponseError."""
    attempts, fetched = 0, False
    while not fetched:
        try:
            build_payload(timeframe=timeframe)
        except ResponseError as err:
            print(err)
            print(f'Trying again in {60 + 5 * attempts} seconds.')
            sleep(60 + 5 * attempts)
            attempts += 1
            if attempts > 10:
                print('Failed after 10 attemps, abort fetching.')
                break
        else:
            fetched = True
    return pytrends.interest_over_time()

#Define the function to get single keyword gtrend data
def get_single_gtrends(word, start_gtrend: str,end_gtrend: str,geo: str,verbose,wait_time):
    start_date = get_gtrend_start_date(start_gtrend)
    stop_date = get_gtrend_end_date(end_gtrend)
    pytrends = TrendReq()
    #pytrends = TrendReq(proxies=[{"https": "https://147.135.255.62:8139"}],timeout=(60))
    # Initialize build_payload with the word we need data for
    build_payload = partial(pytrends.build_payload,
                            kw_list=[word],geo = geo,cat = "0")
    # Obtain weekly data for all months in years [start_year, stop_year]
    weekly = fetch_data(pytrends, build_payload,
                         convert_dates_to_timeframe(start_date,stop_date))
    if weekly.shape[0] == 0:
        complete = None
    else:
        #Get weekly data
        weekly.loc[:,"reference_date"] = weekly.index
        weekly.reset_index(drop = True, inplace = False)
        weekly = spark.createDataFrame(weekly) #transform result in SparkDataframe
        weekly = weekly.withColumn("reference_date",to_date("reference_date"))
        weekly = weekly.drop('isPartial')
        weekly = weekly.withColumn("month",concat_ws('-',year("reference_date").cast(StringType()),month("reference_date").cast(StringType())))
        weekly = weekly.sort("reference_date")
        #Compute monthly sum (scale factor)
        weekly_gr = weekly[[word,"month"]].groupBy("month").agg(sum(word).alias(word))
        weekly_gr = weekly_gr.withColumn("scale_factor", weekly_gr[word]/weekly_gr.select(max(col(word)).alias("max")).first().max)
        # Get daily data, month by month (only in the desired period) , and stor in a dictionary
        results = {}
        # if a timeout or too many requests error occur we need to adjust wait time
        current = start_date
        while current < stop_date:
            last_date_of_month = get_final_month_range(current.year, current.month,stop_date)
            timeframe = convert_dates_to_timeframe(current, last_date_of_month)
            if verbose:
                print(f'{word}:{timeframe}')
            results[current] = fetch_data(pytrends, build_payload, timeframe)
            results[current].loc[:,"reference_date"] = results[current].index
            results[current].reset_index(drop = True, inplace = False)
            if results[current].shape[0] == 0:
                results[current] = spark.createDataFrame([], StructType([
                                                              StructField(word, DoubleType(), True),
                                                              StructField('reference_date', DateType(), True) ]) )
            else:
                results[current] = spark.createDataFrame(results[current])#transform result in SparkDataframe
                results[current] = results[current].withColumn("reference_date",to_date("reference_date"))
                results[current] = results[current].drop('isPartial')#drop this column
            current = last_date_of_month + timedelta(days=1)
            sleep(wait_time)  # don't go too fast or Google will send 429s
        #Obtain overall daily data
        daily = reduce(lambda x,y: x.union(y), results.values())
        daily = daily.withColumn("month",concat_ws('-',year("reference_date").cast(StringType()),month("reference_date").cast(StringType())))
        daily = daily.withColumnRenamed(word, word + "_unscaled")
        #Join with scale factor dataset
        complete = daily.join(weekly_gr[["month","scale_factor"]],on = "month",how = "left")
        complete = complete.withColumn(word,complete[word + "_unscaled"] * complete["scale_factor"])
        complete = complete[["reference_date",word]].sort("reference_date")
    return complete

#Define the function to get all keyword gtrend data
def write_all_gtrends(start: str,end: str,campaign_start:str,kw_list: list,geo: str,gtrend_usage: bool,gtrend_smooth: bool,model_name: str,disable_gtrend: bool,
                    verbose = True,wait_time = 60.0, win = 7):
    if disable_gtrend == False:
        start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
        out_schema = StructType([
                  StructField('reference_date', DateType(), True),
                  StructField('gtrend', DoubleType(), True),
                  StructField('keyword', StringType(), True)
                  ])
        out = spark.createDataFrame([], out_schema)
        if gtrend_usage == True:
            for i in kw_list:
                temp = get_single_gtrends(word = i,start_gtrend = start,end_gtrend = end,geo = geo,
                                        verbose = verbose,wait_time = wait_time)
                if temp is not None:
                    temp = temp.withColumnRenamed(i,"gtrend")
                    temp = temp.withColumn("keyword",lit(i))
                    out = out.union(temp)
        preply_gtrend = get_single_gtrends(word = "preply",start_gtrend = start,end_gtrend = end,geo = "",
                                              verbose = verbose,wait_time = wait_time)
        preply_gtrend = preply_gtrend.withColumnRenamed("preply","gtrend")
        preply_gtrend = preply_gtrend.withColumn("keyword",lit("global_preply"))
        out = out.union(preply_gtrend)
        out = out["reference_date","keyword","gtrend"]
        if out.count() == 0:
            print("For all the selected keywords as well as for preply no gtrend data were founded")
            out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("keyword").save("/tmp/delta/google_data_"  + model_name)
        else:
            if gtrend_smooth == False:
                out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("keyword").save("/tmp/delta/google_data_"  + model_name)
            else:
                out_smooth_schema = StructType([
                              StructField('reference_date', DateType(), True),
                              StructField('keyword', StringType(), True),
                              StructField('gtrend', DoubleType(), True)
                              ])
                out_smooth = spark.createDataFrame([], out_smooth_schema)
                #Smooth only the country specific trend
                gtrend = [i.keyword for i in out.select('keyword').distinct().collect()]
                if "global_preply" in gtrend:
                    gtrend.remove("global_preply")
                days = lambda x: x * 60*60*24 #create function utils to rolling_average
                for j in gtrend:
                    temp = out[out["keyword"] == j].dropna(how = "any", subset = "gtrend").sort("reference_date")
                    w = (Window.orderBy(F.col("reference_date").cast('timestamp').cast("long")).rangeBetween(-days(win - 1), 0))
                    temp = temp.withColumn('rolling_average', F.avg("gtrend").over(w))
                    temp = temp.drop("gtrend").withColumnRenamed("rolling_average","gtrend")
                    out_smooth = out_smooth.union(temp)
                #Append again global preply gtren dif excracted in the previus steps
                if "global_preply" in [i.keyword for i in out.select('keyword').distinct().collect()]:
                    out_smooth = out_smooth.union(out[out["keyword"] == "global_preply"])
                out_smooth.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("keyword").save("/tmp/delta/google_data_"  + model_name)
    else:
        out_schema = StructType([
                  StructField('reference_date', DateType(), True),
                  StructField('keyword', StringType(), True),
                  StructField('gtrend', DoubleType(), True),
                  ])
        out = spark.createDataFrame([], out_schema)
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("keyword").save("/tmp/delta/google_data_"  + model_name)
    return

#Define utility function for linear interpolation (use in case of missing values)
def fill_linear_interpolation(df,order_col,value_col):
    """ 
    Apply linear interpolation to dataframe to fill gaps. 

    :param df: spark dataframe
    :param order_col: column to use to order by the window function
    :param value_col: column to be filled

    :returns: spark dataframe updated with interpolated values
    """
    # create row number over window and a column with row number only for non missing values
    w = Window.orderBy(order_col)
    new_df = df.withColumn('rn',F.row_number().over(w))
    new_df = new_df.withColumn('rn_not_null',F.when(F.col(value_col).isNotNull(),F.col('rn')))

    # create relative references to the start value (last value not missing)
    w_start = Window.orderBy(order_col).rowsBetween(Window.unboundedPreceding,-1)
    new_df = new_df.withColumn('start_val',F.last(value_col,True).over(w_start))
    new_df = new_df.withColumn('start_rn',F.last('rn_not_null',True).over(w_start))

    # create relative references to the end value (first value not missing)
    w_end = Window.orderBy(order_col).rowsBetween(0,Window.unboundedFollowing)
    new_df = new_df.withColumn('end_val',F.first(value_col,True).over(w_end))
    new_df = new_df.withColumn('end_rn',F.first('rn_not_null',True).over(w_end))

    # create references to gap length and current gap position  
    new_df = new_df.withColumn('diff_rn',F.col('end_rn')-F.col('start_rn'))
    new_df = new_df.withColumn('curr_rn',F.col('diff_rn')-(F.col('end_rn')-F.col('rn')))

    # calculate linear interpolation value
    lin_interp_func = (F.col('start_val')+(F.col('end_val')-F.col('start_val'))/F.col('diff_rn')*F.col('curr_rn'))
    new_df = new_df.withColumn(value_col,F.when(F.col(value_col).isNull(),lin_interp_func).otherwise(F.col(value_col)))

    keep_cols = [order_col,value_col]
    new_df = new_df.select(keep_cols)
    new_df =  new_df.withColumn(value_col,round(value_col,0))
    new_df = new_df.withColumn(value_col,round(col(value_col),0))
    return new_df

#Define the function to get covid data
def write_covid_data(start: str,end: str,campaign_start:str,target: str,covid_usage: bool,covid_smooth: bool,model_name:str,win = 7):
    start, end = get_extend_start_end(start = start, end = end, campaign_start = campaign_start)
    if covid_usage == True:
        #Get covid data from repository
        if target == "US": #operaton for import of US covid data
            process_covid = True #create boolean that enables covid elaboration
            covid_schema = StructType([
                          StructField('date', StringType(), True),
                          StructField('cases', DoubleType(), True)
                          ])
            temp = spark.createDataFrame(data = pd.read_csv(config.covid_link[target])[["date","cases"]],schema = covid_schema)
            mapping = {"date" : "reference_date", "cases" : "covid_cases"}
            temp = temp.select([col(c).alias(mapping.get(c, c)) for c in temp.columns])
            temp = temp.withColumn("reference_date",col("reference_date").cast(DateType()))
            w = Window.orderBy("reference_date")
            temp = temp.withColumn("prev_covid_cases",F.lag(temp.covid_cases).over(w))
            temp = temp.withColumn("new_covid_cases", F.when(F.isnull(temp.covid_cases - temp.prev_covid_cases), -1)
                                          .otherwise(temp.covid_cases - temp.prev_covid_cases)).drop("prev_covid_cases")
            func =  udf (lambda x : None if x < 0 else x,DoubleType())
            temp = temp.withColumn('new_covid_cases', func(col('new_covid_cases')))
        elif target in config.covid_link.keys(): #operation for import of UE country
            process_covid = True #create boolean that enables covid elaboration
            covid_schema = StructType([
              StructField('datetime', StringType(), True),
              StructField('cases', DoubleType(), True)
              ])
            temp = spark.createDataFrame(data = pd.read_csv(config.covid_link[target])[["datetime","cases"]],schema = covid_schema)
            mapping = {"datetime" : "reference_date", "cases" : "covid_cases"}
            temp = temp.select([col(c).alias(mapping.get(c, c)) for c in temp.columns])
            temp = temp.withColumn("reference_date",col("reference_date").cast(DateType()))
            temp = temp.groupBy("reference_date").agg(sum("covid_cases").alias("covid_cases")).sort("reference_date")
            w = Window.orderBy("reference_date")
            temp = temp.withColumn("prev_covid_cases",F.lag(temp.covid_cases).over(w))
            temp = temp.withColumn("new_covid_cases", F.when(F.isnull(temp.covid_cases - temp.prev_covid_cases), -1)
                                          .otherwise(temp.covid_cases - temp.prev_covid_cases)).drop("prev_covid_cases")
            func =  udf (lambda x : None if x < 0 else x , DoubleType())
            temp = temp.withColumn('new_covid_cases', func(col('new_covid_cases')))
        else:
            process_covid = False
            print(f"Need to implement specific covid pipeline for campaing dimension equal to {target}")
            out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                  StructField("new_covid_cases", DoubleType(), True) ]) )
            out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta/covid_data_"  + model_name)
        if process_covid == True:
            start_df, end_df = spark.createDataFrame([(start, end)], ("start", "end")).select([col(c).cast("timestamp").cast("long") for c in ("start", "end")]).first()
            out = spark.range(start_df, end_df + 1, 60*60*24).select(col("id").cast("timestamp").alias("reference_date"))
            out = out.withColumn("reference_date",col("reference_date").cast(DateType()))
            out = out.join(temp[["reference_date","new_covid_cases"]], on = "reference_date", how = "left").sort("reference_date")
            out = out.withColumn("row_number",row_number().over(Window.orderBy("reference_date")))#create row_number indicator for NA purposes
            ind = out.filter(out.new_covid_cases.isNull())#isolate rows with NULL
            if ind.count() != 0: #if there are nan
                if ind.agg({"row_number": "min"}).collect()[0][0] == 1: #if nan are starting at the beginning of the series due to the missing of covid in that period, I go where real nan are placed
                    #create indicator diff column
                    ind = ind.withColumn("prev_row_number",F.lag(ind.row_number).over(Window.orderBy("reference_date")))
                    ind = ind.withColumn("indicator_diff", F.when(F.isnull(ind.row_number - ind.prev_row_number), 1)
                                                  .otherwise(ind.row_number - ind.prev_row_number))
                    if len([i.indicator_diff for i in ind.select('indicator_diff').distinct().collect()]) > 1: #if the nan are due to actual missing and not to lack of covid data
                        min_ind = ind.filter(ind.indicator_diff != 1).agg({"prev_row_number": "min"}).collect()[0][0]
                        res_1 = out.filter(out.row_number <= min_ind).drop("row_number")
                        res_2 = out.filter(out.row_number > min_ind).drop("row_number")
                        res_2 = fill_linear_interpolation(df = res_2, order_col = "reference_date", value_col = "new_covid_cases")
                        out = res_1.union(res_2)
                else:
                    out = fill_linear_interpolation(df = out, order_col = "reference_date", value_col = "new_covid_cases")
            if covid_smooth == True:
                temp_smooth = out.dropna(how = "any", subset = "new_covid_cases").sort("reference_date")
                days = lambda x: x * 60*60*24 #create function utils to rolling_average
                w = (Window.orderBy(F.col("reference_date").cast('timestamp').cast("long")).rangeBetween(-days(win - 1), 0))
                temp_smooth = temp_smooth.withColumn('rolling_average', F.avg("new_covid_cases").over(w))
                out = out.join(temp_smooth[["reference_date","rolling_average"]], on = "reference_date", how = "left").sort("reference_date")
                out = out.drop("new_covid_cases").withColumnRenamed("rolling_average","new_covid_cases")
            out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta/covid_data_"  + model_name)
    else:
        print("The import of covid_data was disabled by setting the covid_usage parameter as False")
        out = spark.createDataFrame([], StructType([StructField("reference_date", DateType(), True),
                                                  StructField("new_covid_cases", DoubleType(), True) ]) )
        out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta/covid_data_"  + model_name)
    return

#Define a utility function to build dummy from categorical
def spark_get_dummies(df):
    
    categories = []
    for i, values in enumerate(df.drop("reference_date").columns):
        categories.append(df.select(values).distinct().rdd.flatMap(lambda x: x).collect())
        
    expressions = []
    for i, values in enumerate(df.drop("reference_date").columns):
        expressions.append([F.when(F.col(values) == i, 1).otherwise(0).alias(str(values.replace("_name","")) + "_" + str(i)) for i in categories[i]])
    
    expressions_flat = list(itertools.chain.from_iterable(expressions))
    expressions_flat.append(F.col("reference_date"))
    
    df_final = df.select(*expressions_flat)
    
    return df_final


#Define the function to create final analytical datset
def write_analytical_dataset(start: str,end: str,target: str,yearly_seasonality: bool,add_holidays: bool,model_name: str,spec = False):
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    #spark.sql('OPTIMIZE default.ci_anl_table')
    #Create time sequence from start and end date
    start_df, end_df = spark.createDataFrame([(start, end)], ("start", "end")).select([col(c).cast("timestamp").cast("long") for c in ("start", "end")]).first()
    out = spark.range(start_df, end_df + 1, 60*60*24).select(col("id").cast("timestamp").alias("reference_date"))
    out = out.withColumn("reference_date",col("reference_date").cast(DateType()))
    #Read target metric in perim data
    target_metric_in_perim_df = spark.read.load("/tmp/delta/target_metric_in_perim_" + model_name)
    #Manipulate target series perimeter student
    target_metric_in_perim_df = target_metric_in_perim_df[["reference_date","metric"]]
    target_metric_in_perim_df  = target_metric_in_perim_df.withColumnRenamed("metric","metric_in_perim_" + target)
    out = out.join(target_metric_in_perim_df, on = "reference_date", how = "left").sort("reference_date")
    #Read target metric out perim data
    target_metric_out_perim_df = spark.read.load("/tmp/delta/target_metric_out_perim_" + model_name)
    if target_metric_out_perim_df.count() != 0:
        #Manipulate target series out perimeter student
        target_metric_out_perim_df = target_metric_out_perim_df[["reference_date","metric"]]
        target_metric_out_perim_df = target_metric_out_perim_df.withColumnRenamed("metric","metric_out_perim_" + target)
        out = out.join(target_metric_out_perim_df, on = "reference_date", how = "left").sort("reference_date")
    #Read target metric control data
    target_metric_control_df = spark.read.load("/tmp/delta/target_metric_control_" + model_name)
    for i in [i.campaign_dimension for i in target_metric_control_df.select("campaign_dimension").distinct().collect()]:
        temp = target_metric_control_df[target_metric_control_df["campaign_dimension"] == i][["reference_date","metric"]].sort("reference_date")
        temp = temp.withColumnRenamed("metric","metric_" + i)
        out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read visitor control data
    visitor_control_df = spark.read.load("/tmp/delta/visitor_control_" + model_name)
    if visitor_control_df.count() != 0 :
        for i in [i.campaign_dimension for i in visitor_control_df.select("campaign_dimension").distinct().collect()]:
            temp = visitor_control_df[visitor_control_df["campaign_dimension"] == i][["reference_date","visitor"]].sort("reference_date")
            temp = temp.withColumnRenamed("visitor","visitor_" + i)
            out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read click control data
    click_control_df = spark.read.load("/tmp/delta/click_control_" + model_name)
    if click_control_df.count() != 0:
        for i in [i.campaign_dimension for i in click_control_df.select("campaign_dimension").distinct().collect()]:
            temp = click_control_df[click_control_df["campaign_dimension"] == i][["reference_date","click"]].sort("reference_date")
            temp = temp.withColumnRenamed("click","click_" + i)
            out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read impression control data
    impression_control_df = spark.read.load("/tmp/delta/impression_control_" + model_name)
    if impression_control_df.count() != 0:
        for i in [i.campaign_dimension for i in impression_control_df.select("campaign_dimension").distinct().collect()]:
            temp = impression_control_df[impression_control_df["campaign_dimension"] == i][["reference_date","impression"]].sort("reference_date")
            temp = temp.withColumnRenamed("impression","impression_" + i)
            out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read npc control data
    npc_control_df = spark.read.load("/tmp/delta/npc_control_" + model_name)
    if npc_control_df.count() != 0 :
        for i in [i.campaign_dimension for i in npc_control_df.select("campaign_dimension").distinct().collect()]:
            temp = npc_control_df[npc_control_df["campaign_dimension"] == i][["reference_date","npc"]].sort("reference_date")
            temp = temp.withColumnRenamed("npc","npc_" + i)
            out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read google data
    google_data_df = spark.read.load("/tmp/delta/google_data_" + model_name)
    if google_data_df.count() != 0 :
        for i in [i.keyword for i in google_data_df.select("keyword").distinct().collect()]:
            temp = google_data_df[google_data_df["keyword"] == i][["reference_date","gtrend"]].sort("reference_date")
            temp = temp.withColumnRenamed("gtrend","gtrend_" + i)
            out = out.join(temp, on = "reference_date", how = "left").sort("reference_date")
    #Read covid data
    covid_data_df = spark.read.load("/tmp/delta/covid_data_" + model_name)
    if covid_data_df.count() != 0 :
        out = out.join(covid_data_df[["reference_date","new_covid_cases"]], on = "reference_date", how = "left").sort("reference_date")
    #Impute missing value as 0, please note that covid missing values are threated separately
    out = out.fillna(0)
    if add_holidays == True:
        target_holidays = holidays.CountryHoliday(country = target,prov = None,state = None,observed = False)
        ind_holidays = [i for i in out.select(F.collect_list('reference_date')).first()[0]]
        data_target_holiday = [i in target_holidays for i in ind_holidays]
        data_target_holiday_dummy = [1.0 if i == True else 0.0 for i in data_target_holiday]
        holiday_schema = StructType([StructField("reference_date", StringType())
                      ,StructField("flag_holiday", DoubleType())])
        holiday_data = [[i,j] for i, j in zip(out.select(col("reference_date").cast("string")).rdd.flatMap(lambda x : x).collect(), data_target_holiday_dummy)]
        holiday_df = spark.createDataFrame(holiday_data,holiday_schema) 
        holiday_df = holiday_df.withColumn("reference_date",col("reference_date").cast(DateType()))
        out = out.join(holiday_df, on = "reference_date", how = "left").sort("reference_date")
    #Add month name to enable yearly seasonality
    if yearly_seasonality == True:
        #Get month name by using udf function
        out = out.withColumn('month_name', get_month_name_udf(col("reference_date")))
        #Isolate month name
        temp_month = out[["reference_date","month_name"]]
        #Convert month fetaure in dummies
        temp_month = spark_get_dummies(temp_month)
        #Join columns containing dummies
        out = out.drop("month_name").join(temp_month, how = "left", on = "reference_date").sort("reference_date")
    #Replace blanc spaces with underscore for saving purposes
    for v in out.columns:
        out = out.withColumnRenamed(v, v.replace(" ", "_"))
    out.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta/data_anl_" + model_name_total)
    return

#Create function for the visualization of input data
def plot_input_data(input_type,campaign_start: str, model_name: str):
    data = spark.read.load("/tmp/delta/data_anl_" + model_name)
    if input_type in ["metric","visitor","click","impression","npc","gtrend","covid"]:
        columns_plot = []
        for i in data.drop(*[data.columns[0],data.columns[1]]).columns:
            temp_ind = re.search(input_type,i)
            #print(i)
            if temp_ind is not None:
                columns_plot.append(i)
        max_y = data.agg({data.columns[1]: "max"}).collect()[0][0]
        min_y = data.agg({data.columns[1]: "min"}).collect()[0][0]
        shape = dict(type = "line", xref = "x", yref ="y2", x0 = campaign_start , y0 = min_y, x1 = campaign_start, y1 =  max_y,line=dict(color='Grey',width=2,dash="dot",))
        title = "<b> Input data " + input_type + " type </b>"
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        for j in columns_plot:
            fig.add_trace(go.Scatter(x = np.array(data.select('reference_date').collect()).reshape(-1), y = np.array(data.select(j).collect()).reshape(-1), showlegend = True, mode = 'lines', hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = j),secondary_y=False)
        fig.add_trace(go.Scatter(x = np.array(data.select('reference_date').collect()).reshape(-1), y = np.array(data.select(data.columns[1]).collect()).reshape(-1), showlegend = True, mode = 'lines', line_color = "Yellow", hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = data.columns[1]),secondary_y=True)
        fig.update_xaxes(showgrid = False,title_text="Time",tickfont_size = 10,title_font_size = 12)
        fig.update_yaxes(showgrid = False,title_text=input_type, tickfont_size = 10,title_font_size = 12,secondary_y = False)
        fig.update_yaxes(showgrid = False,title_text="metric", tickfont_size = 10,title_font_size = 12,secondary_y = True)
        fig.update_layout(shapes = [shape],title_text = title,plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
        fig.show()
        return
    else:
        raise ValueError("Wrong input type is specified")

#Create function for the visualization  of acf and pacf of the target time series
def plot_target_corr(campaign_start: str, model_name: str):
    #Reduce data to pre-interventon period
    data = spark.read.load("/tmp/delta/data_anl_" + model_name)
    data_corr = data[data["reference_date"] < campaign_start]
    #Compute correlations
    corr_fun = acf(np.array(data_corr.select(data_corr.columns[1]).collect()),nlags = 40,fft = False)
    pcorr_fun = pacf(np.array(data_corr.select(data_corr.columns[1]).collect()),nlags = 40)
    #Define shape of the plot
    shape_1 = dict(type = "line", xref = "x", yref ="y",  x0=0, x1=len(corr_fun), y0=-0.1, y1= -0.1,line=dict(color='Coral',width=2,dash="dot"))
    shape_2 = dict(type = "line", xref = "x", yref ="y",  x0=0, x1=len(corr_fun), y0=0.1, y1= 0.1,line=dict(color='Coral',width=2,dash="dot"))
    shape_3 = dict(type = "line", xref = "x2", yref ="y2",  x0=0, x1=len(pcorr_fun), y0=-0.1, y1= -0.1,line=dict(color='Coral',width=2,dash="dot"))
    shape_4 = dict(type = "line", xref = "x2", yref ="y2",  x0=0, x1=len(pcorr_fun), y0=0.1, y1= 0.1,line=dict(color='Coral',width=2,dash="dot"))
    shape = [shape_1,shape_2,shape_3,shape_4]
    #Initialize subplot structure
    fig = make_subplots(rows=2, cols=1,subplot_titles= ("<b> Auto Correlation </b>", "<b> Partial Auto Correlation </b>"))
    #Plot correlations
    fig.add_trace(go.Bar(x=[i for i in range(0,len(corr_fun))], y=corr_fun,showlegend = False,name = " ",
                         marker_color = "Blue",hovertemplate = '<b>Lag</b>: %{x}' '<b> Auto corr</b>: %{y}'),row = 1, col = 1)
    fig.add_trace(go.Bar(x=[i for i in range(0,len(pcorr_fun))], y=pcorr_fun,showlegend = False,name = " ",
                         marker_color = "Green",hovertemplate = '<b>Lag</b>: %{x}' '<b> Partial Auto corr</b>: %{y}'),row = 2, col = 1)
    #Set axis property
    fig.update_xaxes(title_text="Lag",tickfont_size = 10,title_font_size = 12, row = 1, col= 1,showgrid = False)
    fig.update_xaxes(title_text="Lag",tickfont_size = 10,title_font_size = 12, row = 2, col= 1,showgrid = False)
    fig.update_yaxes(title_text="Acf",tickfont_size = 10,title_font_size = 12, row = 1, col= 1,showgrid = False)
    fig.update_yaxes(title_text="Pacf",tickfont_size = 10,title_font_size = 12, row = 2, col= 1,showgrid = False)
    fig.update_layout(shapes = shape,title_text = "<b> Metric time series correlation </b>",plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
    fig.show()
    return

#Create function for the visualization of correlation between target and features
def plot_target_feature_corr(campaign_start: str,yearly_seasonality : bool, model_name):
    data_perc_change = spark.read.load("/tmp/delta/data_anl_" + model_name)
    if "flag_holiday" in data_perc_change.columns:
        data_perc_change = data_perc_change.drop("flag_holiday")
    if yearly_seasonality == True:
        columns_month = []
        for i in data_perc_change.columns:
            temp_ind = re.search("month_",i)
            if temp_ind is not None:
                columns_month.append(i)
        if len(columns_month) != 0:
            data_perc_change = data_perc_change.drop(*columns_month)
    data_perc_change = data_perc_change[data_perc_change["reference_date"] < campaign_start]
    data_perc_change = data_perc_change.to_pandas_on_spark().set_index("reference_date").pct_change(periods = 1)
    ind_na = data_perc_change.isna().sum()/data_perc_change.shape[0]
    col_drop = []
    for i in ind_na.index.to_numpy():
        if ind_na[i] >= 0.4:
            col_drop.append(i)
    if len(col_drop) != 0 :
        corr_matrix = data_perc_change.drop(labels = col_drop,axis = 1).dropna().corr()
    else:
        corr_matrix = data_perc_change.copy().dropna().corr()
    cor_targ_feat = ps.DataFrame(data = corr_matrix.iloc[1:,0].abs().to_numpy(),index = corr_matrix.iloc[1:,0].index.to_numpy(),
                                 columns = ["correlation"]).sort_values(by = "correlation",ascending = False)
    fig = go.Figure()
    fig.add_trace(go.Bar(x=cor_targ_feat["correlation"].to_numpy(), y = cor_targ_feat["correlation"].index.to_numpy(),
                         hovertemplate = '<b>Regressor</b>: %{y}' '<b> Correlation</b>: %{x}',
                         marker_color = "DodgerBlue",orientation = "h",
                         name = '', showlegend = False))
    fig.update_xaxes(showgrid = False,title_text="Correlation",tickfont_size = 10,title_font_size = 12)
    fig.update_yaxes(showgrid = False,title_text="Regressor",tickfont_size = 10,title_font_size = 12)

    fig.update_layout(yaxis = dict(autorange = "reversed"),title_text = "<b> Correlation with target </b>",
                      plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
    fig.show()
    return
