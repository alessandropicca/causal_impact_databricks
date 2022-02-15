# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 16:20:43 2022

@author: aless
"""
#In this script we defin all the fucntions for training, test, visualize and save models

#Import configuration script
from config_dir import config

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

import sys
#Create system variable for pytrend repo
sys.path.append("/Workspace/Repos/alessandro.picca@preply.com/causal_impact_databricks")
#Import data function to be used in specificity test
from lib import ci_data_function as data_fun

#Import all other required libraries
import importlib
import imp
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
imp.reload(data_fun)

#Initialize spark session for all funtions

spark = config.spark_session

#Define function to write post-processede analytical dataset
def write_post_process_data(campaign_start:str,model_name: str,yearly_seasonality: bool,spec = False, win = 7):
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    data_pp = spark.read.load("/tmp/delta/data_anl_" + model_name_total)
    #reduce to pre-intervention period
    data_pp_red = data_pp[data_pp["reference_date"] < campaign_start]
    #remove columns for which not check the sum, start with flag_holiday
    if "flag_holiday" in data_pp.columns:
        columns = data_pp_red.drop(*[data_pp_red.columns[0], data_pp_red.columns[1], "flag_holiday"]).columns
    else:
        columns = data_pp_red.drop(*[data_pp_red.columns[0], data_pp_red.columns[1]]).columns
    #remove month_name columns
    if yearly_seasonality == True:
        columns_month = []
        for i in data_pp_red.columns:
            temp_ind = re.search("month_",i)
            if temp_ind is not None:
                columns_month.append(i)
    if len(columns_month) != 0:
        columns = [i for i in columns if i not in columns_month]
    ind_zero = data_pp_red.groupBy().sum(*columns).toDF(*columns)
    col_drop = []
    for i in ind_zero.columns:
        if ind_zero.select(col(i)).collect()[0][0] == 0:
            col_drop.append(i)
    days = lambda x: x * 60*60*24 #create function utils to rolling_average
    w = (Window.orderBy(F.col("reference_date").cast('timestamp').cast("long")).rangeBetween(-days(win - 1), 0))
    data_pp_red = data_pp_red.withColumn('rolling_std', F.stddev_pop(data_pp.columns[1]).over(w))
    min_index = None
    for i in np.array(data_pp_red.select(col("reference_date")).collect()).reshape(-1)[1:]:
        #print(i)
        if data_pp_red.filter(data_pp_red.reference_date == i).select(col("rolling_std")).collect()[0][0] == 0:
            min_index = i
        else:
            break
    #Now apply pre-processing to original data object
    if len(col_drop) != 0:
        print(f"Internal pre-processing has deleted following columns {','.join(col_drop)}")
        data_pp = data_pp.drop(*col_drop)
    if min_index is not None:
        data_pp = data_pp[data_pp["reference_date"] > min_index]
    start_up = data_pp.agg({data_pp.columns[0]: "min"}).collect()[0][0]
    end_up = data_pp.agg({data_pp.columns[0]: "max"}).collect()[0][0]
    diff_start = relativedelta(datetime.strptime(campaign_start,"%Y-%m-%d"),start_up)
    diff_end = relativedelta(end_up,datetime.strptime(campaign_start,"%Y-%m-%d"))
    if (((diff_start.years != 0) | (diff_start.months >= 6 )) & ((diff_end.days >= 10) | (diff_end.months != 0) | (diff_end.years != 0)) & (((yearly_seasonality == True) & (diff_start.years >= 2)) | (yearly_seasonality == False))) == False:
        raise ValueError("Something in the specification of the parameter start, end, campaign_start, yearly_seasonality is wrong, or the internal pre-processing has deleted great part of data\nPlease check the setting parameter and the data analysis section, and retry to run the analysis")
    data_pp.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save("/tmp/delta/data_anl_post_process_" + model_name_total)
    return

#Create function to peform augmented Dick-Fuller test
def run_adf_test(campaign_start:str, model_name: str, sig = 0.01):
    #here not consider spec because it is not used in specificty test
    temp = spark.read.load("/tmp/delta/data_anl_post_process_" + model_name)
    temp = temp[temp["reference_date"] < campaign_start].select(*[temp.columns[0], temp.columns[1]])
    s_dc = seasonal_decompose(np.array(temp.select(col(temp.columns[1])).collect()), model='additive', period=7)#deseasonal for weekly period
    seasonal_schema = StructType([StructField("reference_date", StringType()),StructField("seasonal", DoubleType())])
    seasonal_data = [[i,float(j)] for i, j in zip(temp.select(col("reference_date").cast("string")).rdd.flatMap(lambda x : x).collect(), s_dc.seasonal)]
    seasonal_df = spark.createDataFrame(seasonal_data,seasonal_schema)
    seasonal_df = seasonal_df.withColumn("reference_date",col("reference_date").cast(DateType()))
    temp = temp.join(seasonal_df, on = "reference_date", how = "left").sort("reference_date")
    temp = temp.withColumn("deseasonal", temp[temp.columns[1]] - temp["seasonal"])
    test = adfuller(np.array(temp.select(col("deseasonal")).collect()), autolag = "AIC",regression = "c")#perform adfuller test trend_constant stationary
    if test[1] < sig :
        out = "Stationary"
    else:
        out = "Not stationary"
    return out

#Create function to create the trend list over to search model
def create_trend_list(campaign_start: str, model_name: str):
    #here not consider spec because it is not used in specificty test
    stationarity_test = run_adf_test(campaign_start = campaign_start, model_name = model_name)
    if stationarity_test == "Not stationary":
        trend_list = ["local_level", "local_linear", "semi_local_linear"]
    else:
        trend_list = ["no_trend"]
    return trend_list

#Create function to find autoregressive order (if not fixed)
def find_ar_order(campaign_start: str,model_name: str,fix_order,spec : bool,thr = 0.3):
    #here consider spec because it is used in specificty test
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    data = spark.read.load("/tmp/delta/data_anl_post_process_" + model_name_total)
    data_corr = data[data["reference_date"] < campaign_start]        
    pcorr_fun = pacf(np.array(data_corr.select(data_corr.columns[1]).collect()),nlags = 40)
    if fix_order == True:
        out = 1
    else:
        temp_out = 1
        for i in range(2,len(pcorr_fun)):
            #print(i)
            if np.abs(pcorr_fun[i]) >= thr:
                temp_out = i
            else:
                break
        out = temp_out
    return out

#Create function to build strucutural time series underlying model
def create_bsts_model(campaign_start: str,model_name: str,trend_type,monthly_seasonality: bool,
                      yearly_seasonality: bool,fix_order: bool, spec = False):
    if trend_type in ["local_level", "local_linear", "semi_local_linear","no_trend"]:
        #here consider spec because it is used in specificty test
        if spec == False:
            model_name_total = model_name
        else:
            model_name_total = model_name + "_spec"
        #work in pandas because of causla impact and tensorflow
        data = spark.read.load("/tmp/delta/data_anl_post_process_" + model_name_total).toPandas()
        data = data.set_index("reference_date").astype(np.float32)
        #Avoid to standardize dummy variables and then re-attach them
        col_drop = []
        if "flag_holiday" in data.columns:
            col_drop.append("flag_holiday")
        if yearly_seasonality == True:
            for i in data.columns:
                temp_ind = re.search("month_",i)
                if temp_ind is not None:
                    col_drop.append(i)
        if len(col_drop) != 0:
            data_stand, mu_sig = standardize(data.drop(col_drop,axis = 1))
            data_stand = data_stand.merge(data[col_drop], how = "left", right_index = True, left_index = True)
        else:
            data_stand, mu_sig = standardize(data)
        obs_data_stand = data_stand[data_stand.index < datetime.strptime(campaign_start,"%Y-%m-%d").date()].iloc[:,0]
        design_matrix_stand = data_stand.iloc[:,1:].values.reshape(-1, data_stand.shape[1] -1)
        #Set prior hyper parameters
        mean = np.abs(obs_data_stand.mean())
        std =  obs_data_stand.std(ddof = 0) * 0.1
        alpha_hyp = mean**2 / (std**2) + 2
        beta_hyp = mean * (mean**2 /(std**2) + 1)
        alpha_hyp = tf.cast(alpha_hyp,tf.float32)
        beta_hyp = tf.cast(beta_hyp,tf.float32)
        regression = tfp.sts.SparseLinearRegression(design_matrix = design_matrix_stand,name = "Linear Regression")	
        weekly_season = tfp.sts.Seasonal(num_seasons=7, num_steps_per_season=1, observed_time_series=obs_data_stand,allow_drift=True, name='Weekly')
        if trend_type == "local_level":
            trend = tfp.sts.LocalLevel(level_scale_prior = tfp.distributions.InverseGamma(
                    concentration = alpha_hyp, scale=beta_hyp), observed_time_series=obs_data_stand,name = "Trend")
            state_list = [trend, regression, weekly_season]
        elif trend_type == "local_linear":
            trend = tfp.sts.LocalLinearTrend(level_scale_prior = tfp.distributions.InverseGamma(
                    concentration = alpha_hyp, scale = beta_hyp),slope_scale_prior = tfp.distributions.InverseGamma(
                    concentration = alpha_hyp, scale = beta_hyp), observed_time_series=obs_data_stand,name = "Trend")
            state_list = [trend, regression, weekly_season]
        elif trend_type == "semi_local_linear":
            trend = tfp.sts.SemiLocalLinearTrend(level_scale_prior = tfp.distributions.InverseGamma(
                    concentration = alpha_hyp, scale = beta_hyp),slope_scale_prior = tfp.distributions.InverseGamma(
                    concentration = alpha_hyp, scale = beta_hyp), observed_time_series=obs_data_stand,name = "Trend")
            state_list = [trend, regression, weekly_season]
        elif trend_type == "no_trend":
            state_list = [regression, weekly_season]
        if monthly_seasonality == True:
            monthly_season = tfp.sts.Seasonal(num_seasons=30, num_steps_per_season=1, observed_time_series=obs_data_stand,allow_drift=True, name='Monthly')
            state_list.append(monthly_season)
        ar_order = find_ar_order(campaign_start = campaign_start, model_name = model_name, fix_order = fix_order, spec = spec)
        ar_proc = tfp.sts.Autoregressive(order = ar_order ,observed_time_series = obs_data_stand,name = "Auto-regressive")
        state_list.append(ar_proc)
        ts_model = tfp.sts.Sum(state_list, observed_time_series=obs_data_stand)
        return ts_model
    else:
        raise ValueError("Wrong trend_type is specified")

#Define function to run the causal impact mode, for a given bsts model
def run_causal_impact_custom(campaign_start: str,model_name: str,monthly_seasonality : bool, yearly_seasonality : bool,ts_model,alpha,fit_method, spec = False):
    #here I insert specificity parameter because this function is going to be used in specificity test
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    data = spark.read.load("/tmp/delta/data_anl_post_process_" + model_name_total).toPandas()
    data = data.set_index("reference_date").astype(np.float32)
    data.index = pd.to_datetime(data.index)
    start_int = data.index.min()
    end_int = data.index.max()
    pre_period = [start_int,pd.to_datetime(campaign_start,format = "%Y-%m-%d") - timedelta(days = 1) ]
    post_period = [pd.to_datetime(campaign_start,format = "%Y-%m-%d"),end_int]
    #Let us implement causal impact call with custom time series model
    tf.random.set_seed(123)
    ci = CausalImpact(data = data, pre_period = pre_period, post_period = post_period, model = ts_model,
                          alpha = alpha, model_args = {"fit_method" : fit_method})
    key_list = list(ci.__dict__.keys())
    remove_list = ["one_step_dist","posterior_dist"]
    key_list = [k for k in key_list if k not in remove_list]
    out = dict.fromkeys(key_list)
    for i in list(out.keys()):
        out[i] = ci.__dict__[i]
    out["inferences"] = out["inferences"].merge(out["data"].iloc[:,0],right_index = True,left_index = True)
    out["inferences"].rename(columns={out["inferences"].columns[len(out["inferences"].columns)-1]: "observed" }, inplace = True)
    out["inferences"].loc[:,"error"] = out["inferences"].loc[:,"observed"] - out["inferences"].loc[:,"complete_preds_means"]
    out["inferences"].loc[:,"perc_error"] = out["inferences"].loc[:,"error"]/out["inferences"].loc[:,"observed"]
    out["inferences"].loc[:,"perc_error"] = out["inferences"].loc[:,"perc_error"].apply(lambda x : np.nan if (x == float('inf') or x == -float('inf')) else x)
    #Add argument to model output
    out["campaign_start"] = campaign_start
    out["start"] = str(data.index.min())
    out["end"] = str(data.index.max())
    out["monthly_seasonality"] = monthly_seasonality
    out["yearly_seasonality"] = yearly_seasonality
    return out 

#Define function to save the pickle model
def save_model_pickle(model,model_name: str):
    #send the model object to temporary S3 location
    pickle_out = open("/tmp/%s" % model_name + ".pickle","wb")
    pickle.dump(model,pickle_out)
    pickle_out.close()
    #Initialize s3 object in Python
    s3 = boto3.client('s3')
    #Upload file from temporary storage place to final storage location
    response = s3.upload_file("/tmp/%s" % model_name + ".pickle", 'databricks-redshift-preply', 'alessandro_picca/%s' % (model_name) + ".pickle")
    return

#Define function to convert the panadas component of the model in py-spark dataframe
def convert_df_pandas_spark(model):
    #Transform output in pyspark dataframe
    for i in ["data", "inferences", "normed_post_data", "normed_pre_data","observed_time_series","post_data","pre_data"]:
        #print(i)
        temp = model[i].copy()
        temp_col_list = list(temp.columns)
        temp_col_list.insert(0,"reference_date")
        temp.loc[:,"reference_date"] = temp.index
        temp.reset_index(drop = True, inplace = True)
        temp = temp[temp_col_list]
        temp = spark.createDataFrame(temp)
        temp = temp.withColumn(temp.columns[0],col(temp.columns[0]).cast(DateType()))
        model[i] = temp
    temp = model["summary_data"].copy()
    temp_col_list = list(temp.columns)
    temp_col_list.insert(0,"quantity")
    temp.loc[:,"quantity"] = temp.index
    temp.reset_index(drop = True, inplace = True)
    temp = temp[temp_col_list]
    temp = spark.createDataFrame(temp)
    model["summary_data"] = temp
    return model

#Define function to load pickle model object
def load_model_pickle(model_name: str, convert_pd = True):
    #Initialize s3 object in Python
    s3 = boto3.client('s3')
    #Download the object from S3 final storage location and put in download location
    s3.download_file('databricks-redshift-preply', 'alessandro_picca/%s' % (model_name) + ".pickle", "/tmp/%s" % model_name + "_downloaded.pickle")
    #Read the pickle file
    pickle_in = open("/tmp/%s" % model_name + "_downloaded.pickle","rb")
    out = pickle.load(pickle_in)
    pickle_in.close()
    if convert_pd == True:#convert everything to pyspark (not use only in specifity test, beacsue then it saves back)
        out = convert_df_pandas_spark(out)
    return out

#Define function to perform model selection and to run final causal impact model
def search_run_causal_impact(campaign_start: str,model_name : str, monthly_seasonality: bool,
                             yearly_seasonality,fix_order: bool,search_model = "search",
                             alpha = 0.05,fit_method = "hmc"):
    start_time = time.time()
    write_post_process_data(campaign_start = campaign_start,model_name = model_name,yearly_seasonality = yearly_seasonality)
    #data = spark.read.load("/tmp/delta/data_anl_post_process_" + model_name).toPandas()
    #data = data.set_index("reference_date").astype(np.float32)
    if search_model == "search":
        trend_list = create_trend_list(campaign_start = campaign_start, model_name = model_name)
    else:
        trend_list = [search_model]
    mape = float('inf')
    final_model = {}
    model_type = None
    for i in trend_list:
        print(f'checking {i} model')
        ts_model = create_bsts_model(campaign_start = campaign_start,model_name = model_name, trend_type = i,
                                     monthly_seasonality = monthly_seasonality, yearly_seasonality = yearly_seasonality,
                                     fix_order = fix_order)
        ci_model = run_causal_impact_custom(campaign_start = campaign_start,model_name = model_name,monthly_seasonality = monthly_seasonality,yearly_seasonality = yearly_seasonality,ts_model = ts_model,alpha = alpha,fit_method = fit_method)
        ci_mape = np.nanmean(np.abs(ci_model["inferences"]["perc_error"]))
        if ci_mape < mape:
            mape = ci_mape
            final_model = ci_model
            model_type = i
    end_time = time.time()
    final_model["model_name"] = model_type
    final_model["elapsed_time_min"] = np.round((end_time - start_time)/60,0)
    save_model_pickle(model = final_model, model_name = model_name)
    return

#Define the fucntion to get referement of specificty test
def get_specificity_reference(start:str, end:str, campaign_start:str):
    #Retrieve campaing start from fitted model object
    campaign_start = datetime.strptime(campaign_start,"%Y-%m-%d")
    campaign_duration = (datetime.strptime(end,"%Y-%m-%d") - campaign_start).days
    #Compute end and campaign_start for specifity test
    spec_end = campaign_start - timedelta(days = 1)
    spec_campaign_start = spec_end - timedelta(days = campaign_duration)
    #Compute duration of pre-intervention period used in the analysis
    training_period = (campaign_start -  datetime.strptime(start,"%Y-%m-%d")).days
    #Compute start of observation period for specificity test
    spec_start = spec_campaign_start - timedelta(days =  training_period)
    spec_end = str(spec_end.date())
    spec_start = str(spec_start.date())
    spec_campaign_start = str(spec_campaign_start.date())
    return spec_start, spec_end, spec_campaign_start


#Define the function to run the specificty test
def run_specificity_test(start:str,end:str,target:str,monthly_seasonality:bool, yearly_seasonality:bool,
                        add_holidays:bool,fix_order:bool,model_name:str, alpha = 0.05,fit_method = "hmc",spec_thr = 0.95):
    #Get model object from S3 and 
    model = load_model_pickle(model_name = model_name,convert_pd = False)
    #Create config name for specifity test 
    model_name_total = model_name + "_spec"

    #Retrieve campaing start from fitted model object
    #campaign_start = datetime.strptime(model["campaign_start"],"%Y-%m-%d")
    #campaign_start = pd.to_datetime(model["campaign_start"],format = "%Y-%m-%d")
    #campaign_duration = (datetime.strptime(end,"%Y-%m-%d") - campaign_start).days
    #campaign_duration = (pd.to_datetime(end,format = "%Y-%m-%d") - campaign_start).days
    #Compute end and campaign_start for specifity test
    #spec_end = campaign_start - timedelta(days = 1)
    #spec_campaign_start = spec_end - timedelta(days = campaign_duration)
    #Compute duration of pre-intervention period used in the analysis
    #training_period = (campaign_start -  datetime.strptime(start,"%Y-%m-%d")).days
    #training_period = (campaign_start - pd.to_datetime(start,format = "%Y-%m-%d")).days
    #Compute start of observation period for specificity test
    #spec_start = spec_campaign_start - timedelta(days =  training_period)
    #spec_end = str(spec_end.date())
    #spec_start = str(spec_start.date())
    #spec_campaign_start = str(spec_campaign_start.date())
    
    spec_start, spec_end, spec_campaign_start =get_specificity_reference(start = start, end = end, campaign_start = model["campaign_start"])

    #Import paying student in perim for specifity test 
    #data_fun.write_target_metric_in_perim(start = spec_start,end = spec_end,target = target,config_name = config_name,model_name = model_name,spec = True)
    spec_target_metric_in_perim = spark.read.load("/tmp/delta/target_metric_in_perim_" + model_name)

    #Import paying student out perim for specifity test if the corresponding is used in the general analysis
    #data_fun.write_target_metric_out_perim(start = spec_start ,end = spec_end,target = target,
                                                                   #out_perim_usage = out_perim_usage,config_name = config_name,model_name = model_name,spec = True)
    spec_target_metric_out_perim = spark.read.load("/tmp/delta/target_metric_out_perim_" + model_name)

    #Import paying student control for specificity test
    #data_fun.write_target_metric_control(start = spec_start, end = spec_end, control = control,config_name = config_name,model_name = model_name, spec = True)
    spec_target_metric_control = spark.read.load("/tmp/delta/target_metric_control_" + model_name)

    #Import visitor control for specificity test if the corresponding is used in the general analysis
    #data_fun.write_visitor_control(start = spec_start, end = spec_end, control = control,visitor_usage = visitor_usage,config_name = config_name,model_name = model_name,spec = True)
    spec_visitor_control = spark.read.load("/tmp/delta/visitor_control_" + model_name)

    #Import click control for specificity test if the corresponding is used in the general analysis
    #data_fun.write_click_control(start = spec_start, end = spec_end,control = control,click_usage = click_usage,config_name = config_name,model_name = model_name,spec = True)
    spec_click_control = spark.read.load("/tmp/delta/click_control_" + model_name)

    #Import impression control for specificity test if the corresponding is used in the general analysis
    #data_fun.write_impression_control(start = spec_start,end = spec_end,control = control,impression_usage = impression_usage,config_name = config_name,model_name = model_name,spec = True)
    spec_impression_control = spark.read.load("/tmp/delta/impression_control_" + model_name)

    #Import npc control for specificity test if the corresponding is used in the general analysis
    #data_fun.write_npc_control(start = spec_start, end = spec_end,control = control,npc_usage = npc_usage,config_name = config_name,model_name = model_name, spec = True)
    spec_npc_control = spark.read.load("/tmp/delta/npc_control_" + model_name)

    #Import google data for specificity test if the corresponding is used in the general analysis
    #data_fun.write_all_gtrends(start = spec_start ,end = spec_end, kw_list = kw_list,geo = target,gtrend_usage = gtrend_usage,
                                          #gtrend_smooth = gtrend_smooth, model_name = model_name, disable_gtrend = disable_gtrend, spec = True)
    spec_google_data = spark.read.load("/tmp/delta/google_data_" + model_name)

    #Import covd_data for specificity test if the corresponding is used in the general analysis
    #data_fun.write_covid_data(start = spec_start,end = spec_end,target = target,covid_usage = covid_usage,
                                         #covid_smooth = covid_smooth,model_name = model_name,spec = True)
    spec_covid_data = spark.read.load("/tmp/delta/covid_data_" + model_name)

    #Get final dataset for specificity test
    data_fun.write_analytical_dataset(start = spec_start,end = spec_end,target = target, yearly_seasonality = yearly_seasonality,add_holidays = add_holidays,model_name = model_name,spec = True)
    #Post process analytical dataset for specificty test 
    write_post_process_data(campaign_start = spec_campaign_start,model_name = model_name, yearly_seasonality = yearly_seasonality, spec = True)
    #Create structural time series model for specificity test

    spec_ts_model = create_bsts_model(campaign_start = spec_campaign_start,model_name = model_name, trend_type = model["model_name"],
                                         monthly_seasonality = monthly_seasonality, yearly_seasonality = yearly_seasonality,
                                         fix_order = fix_order,spec = True)
    #Fit causal impact model for specificity test
    spec_ci_model = run_causal_impact_custom(campaign_start = spec_campaign_start,model_name = model_name,monthly_seasonality = monthly_seasonality,yearly_seasonality = yearly_seasonality,ts_model = spec_ts_model,alpha = alpha,fit_method = fit_method,spec = True)

    #Compute speficity test results
    if (1 - spec_ci_model["p_value"]) >= spec_thr:
        spec_res = "Negative"
    else:
        spec_res = "Positive"
    #Update specifity test inside model
    model["specificity_test"] = spec_res
    #Save original model
    save_model_pickle(model = model, model_name = model_name)
    #Save specificity model
    save_model_pickle(model = spec_ci_model, model_name = model_name + "_spec")
    return

#Define the function to print causal impact results
def print_model_summary(model_name,print_specificity = True,spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)
    avg_actual = np.round(model["summary_data"].filter(model["summary_data"].quantity == "actual").select(col("average")).collect()[0][0],2)
    avg_pred = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted").select(col("average")).collect()[0][0],2)
    avg_pred_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted_lower").select(col("average")).collect()[0][0],2)
    avg_pred_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted_upper").select(col("average")).collect()[0][0],2)
    avg_pred_sd = np.round((avg_pred_upp - avg_pred_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    avg_abs_eff = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect").select(col("average")).collect()[0][0],2)
    avg_abs_eff_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect_lower").select(col("average")).collect()[0][0],2)
    avg_abs_eff_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect_upper").select(col("average")).collect()[0][0],2)
    avg_abs_eff_sd = np.round((avg_abs_eff_upp - avg_abs_eff_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    avg_rel_eff = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect").select(col("average")).collect()[0][0]*100,2)
    avg_rel_eff_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect_lower").select(col("average")).collect()[0][0]*100,2)
    avg_rel_eff_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect_upper").select(col("average")).collect()[0][0]*100,2)
    avg_rel_eff_sd = np.round((avg_rel_eff_upp - avg_rel_eff_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    cum_actual = np.round(model["summary_data"].filter(model["summary_data"].quantity == "actual").select(col("cumulative")).collect()[0][0],2)
    cum_pred = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted").select(col("cumulative")).collect()[0][0],2)
    cum_pred_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted_lower").select(col("cumulative")).collect()[0][0],2)
    cum_pred_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "predicted_upper").select(col("cumulative")).collect()[0][0],2)
    cum_pred_sd= np.round((cum_pred_upp - cum_pred_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    cum_abs_eff = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect").select(col("cumulative")).collect()[0][0],2)
    cum_abs_eff_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect_lower").select(col("cumulative")).collect()[0][0],2)
    cum_abs_eff_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "abs_effect_upper").select(col("cumulative")).collect()[0][0],2)
    cum_abs_eff_sd = np.round((cum_abs_eff_upp - cum_abs_eff_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    cum_rel_eff = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect").select(col("cumulative")).collect()[0][0]*100,2)
    cum_rel_eff_low = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect_lower").select(col("cumulative")).collect()[0][0]*100,2)
    cum_rel_eff_upp = np.round(model["summary_data"].filter(model["summary_data"].quantity == "rel_effect_upper").select(col("cumulative")).collect()[0][0]*100,2)
    cum_rel_eff_sd = np.round((cum_rel_eff_upp - cum_rel_eff_low)/(np.round(scipy.stats.norm.ppf(1 - model["alpha"]/2),2) * 2),2)
    
    prob = np.ceil((1 - model["p_value"])*100)
    
    #mape = np.round(np.nanmean(np.abs(np.array(model["inferences"].select(col("perc_error")).collect()))) * 100,2)
    
    mape = np.round(np.nanmean(np.abs(np.array([np.nan if i[0] is None else i[0] for i in np.array(model["inferences"].select(col("perc_error")).collect())]))) * 100,2)
    
    table_summ = PrettyTable()
    table_summ.field_names = ["", "Average", "Cumulative"]
    
    table_summ.add_row(["Actual",str(avg_actual),str(cum_actual)])
    table_summ.add_row(["Prediction (s.d.)",str(avg_pred) + " (" + str(avg_pred_sd) + ")", str(cum_pred) + " (" + str(cum_pred_sd) + ")"])
    table_summ.add_row(["95% CI", "[" + str(avg_pred_low) + ", " + str(avg_pred_upp) + "]",  "[" + str(cum_pred_low) + ", " + str(cum_pred_upp) + "]"])
    table_summ.add_row(["", "", "" ])
    
    table_summ.add_row(["Absolute effect (s.d.)",str(avg_abs_eff) + " (" + str(avg_abs_eff_sd) + ")",str(cum_abs_eff) + " (" + str(cum_abs_eff_sd) + ")" ])
    table_summ.add_row(["95% CI", "[" + str(avg_abs_eff_low) + ", " + str(avg_abs_eff_upp) + "]","[" + str(cum_abs_eff_low) + ", " + str(cum_abs_eff_upp) + "]" ])
    table_summ.add_row(["", "", "" ])
    
    table_summ.add_row(["Relative effect (s.d.)",str(avg_rel_eff) + "% (" + str(avg_rel_eff_sd) + "%)",str(cum_rel_eff) + "% (" + str(cum_rel_eff_sd) + "%)"])
    table_summ.add_row(["95% CI","[" + str(avg_rel_eff_low) + "%, " + str(avg_rel_eff_upp) + "%]","[" + str(cum_rel_eff_low) + "%, " + str(cum_rel_eff_upp) + "%]"])
    
    #table_summ.set_style(DOUBLE_BORDER)
    
    print(table_summ)
    print("")
    print("Posterior probability of a causal effect: " + str(prob) + "%")
    print("Mean abcolute percentage error of model in pre-period: " + str(mape) + "%")
    if print_specificity == True:
        print("Result of specificity test =  " + model["specificity_test"])
    return

#Define the function to plot causal impact reuslts
def plot_result_causal_impact(model_name,spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)

    title = '<b> Causal Impact results </b>'

    min_y1 = np.min([model["inferences"].agg({"observed" : "min"}).collect()[0][0],model["inferences"].agg({"complete_preds_means" : "min"}).collect()[0][0],model["inferences"].agg({"complete_preds_lower" : "min"}).collect()[0][0]])

    max_y1 = np.max([model["inferences"].agg({"observed" : "max"}).collect()[0][0],model["inferences"].agg({"complete_preds_means" : "max"}).collect()[0][0],model["inferences"].agg({"complete_preds_upper" : "max"}).collect()[0][0]])

    min_y2 = np.min([model["inferences"].agg({"point_effects_means" : "min"}).collect()[0][0],model["inferences"].agg({"point_effects_lower" : "min"}).collect()[0][0]])

    max_y2 = np.max([model["inferences"].agg({"point_effects_means" : "max"}).collect()[0][0],model["inferences"].agg({"point_effects_upper" : "max"}).collect()[0][0]])

    min_y3 = np.min([model["inferences"].dropna().agg({"post_cum_effects_means" : "min"}).collect()[0][0],model["inferences"].dropna().agg({"post_cum_effects_lower" : "min"}).collect()[0][0]])

    max_y3 = np.min([model["inferences"].dropna().agg({"post_cum_effects_means" : "max"}).collect()[0][0],model["inferences"].dropna().agg({"post_cum_effects_upper" : "max"}).collect()[0][0]])

    shape_1 = dict(type = "line", xref = "x", yref ="y", x0 = model["post_period"][0] , y0 = min_y1, x1 = model["post_period"][0], y1 = max_y1,line=dict(color='rgb(58,60,57)',width=2,dash="dot",))
    shape_2 = dict(type = "line", xref = "x2", yref ="y2", x0 = model["post_period"][0] , y0 = min_y2, x1 = model["post_period"][0], y1 = max_y2,line=dict(color='rgb(58,60,57)',width=2,dash="dot",))
    shape_3 = dict(type = "line", xref = "x3", yref ="y3", x0 = model["post_period"][0] , y0 = min_y3, x1 = model["post_period"][0], y1 = max_y3,line=dict(color='rgb(58,60,57)',width=2,dash="dot",))

    shape = [shape_1,shape_2,shape_3]

    fig = make_subplots(rows=3, cols=1,subplot_titles= ("<b> Observed vs Counterfactual </b>","<b> Pointwise effect </b>", "<b> Cumulative effect </b>"),shared_xaxes=True,vertical_spacing = 0.05)

    fig.add_trace(go.Scatter(x=np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y=np.array(model["inferences"].select("observed").collect()).reshape(-1), showlegend = True, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = 'Observed', line_color = 'Crimson'),row =1, col= 1)

    fig.add_trace(go.Scatter(x=np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y=np.array(model["inferences"].select("complete_preds_means").collect()).reshape(-1), showlegend = True, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = 'Counterfactual', line_color = 'DarkViolet'),row =1, col= 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y = np.array(model["inferences"].select("complete_preds_upper").collect()).reshape(-1),showlegend = False,name = "Upper bound" , mode = "lines", hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0)),row = 1, col = 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y = np.array(model["inferences"].select("complete_preds_lower").collect()).reshape(-1),showlegend = False,name = "Lower bound", mode = "lines",hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}', marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0),fillcolor='rgba(84, 158, 11, 0.3)',fill='tonexty'),row = 1, col = 1)

    fig.add_trace(go.Scatter(x=np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y=np.array(model["inferences"].select("point_effects_means").collect()).reshape(-1), showlegend = True, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = 'Pointwise effect', line_color = 'DeepPink', line_dash = "dot"),row =2, col= 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y = np.array(model["inferences"].select("point_effects_upper").collect()).reshape(-1),showlegend = False,name = "Upper bound" , mode = "lines", hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0)),row = 2, col = 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y = np.array(model["inferences"].select("point_effects_lower").collect()).reshape(-1),showlegend = False,name = "Lower bound", mode = "lines",hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}', marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0),fillcolor='rgba(84, 158, 11, 0.3)',fill='tonexty'),row = 2, col = 1)

    fig.add_trace(go.Scatter(x= np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y=np.array(model["inferences"].select("post_cum_effects_means").fillna(0).collect()).reshape(-1), showlegend = True, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = 'Cumulative effect', line_color = 'Brown', line_dash = "dot"),row =3, col= 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y =np.array(model["inferences"].select("post_cum_effects_upper").fillna(0).collect()).reshape(-1),showlegend = False,name = "Upper bound" , mode = "lines", hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0)),row = 3, col = 1)

    fig.add_trace(go.Scatter(x = np.array(model["inferences"].select("reference_date").collect()).reshape(-1), y = np.array(model["inferences"].select("post_cum_effects_lower").fillna(0).collect()).reshape(-1),showlegend = False,name = "Lower bound", mode = "lines",hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}', marker=dict(color="rgb(84, 158, 11)"),line=dict(width=0),fillcolor='rgba(84, 158, 11, 0.3)',fill='tonexty'),row = 3, col = 1)

    fig.update_xaxes(showgrid = False,title_text="Time",tickfont_size = 10,title_font_size = 12,row = 3,col = 1)
    fig.update_yaxes(showgrid = False,title_text="Metric", tickfont_size = 10,title_font_size = 12,row = 1, col= 1)
    fig.update_yaxes(showgrid = False,title_text="Metric point-diff",tickfont_size = 10,title_font_size = 12, row = 2, col= 1)
    fig.update_yaxes(showgrid = False,title_text="Metric cum-diff",tickfont_size = 10,title_font_size = 12, row = 3, col= 1)
    fig.update_layout(shapes = shape,title_text = title,plot_bgcolor='rgb(255,255,255)',legend = dict(traceorder = 'normal'),width = 1200, height=600)
    fig.show()
    return

#Define the function to get component of structural time series model
def get_component_stand(model_name,spec):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    #Load model
    model = load_model_pickle(model_name = model_name_total)
    tf.random.set_seed(123)#set tf seed
    #Decompose time series by component
    one_step_dists = tfp.sts.decompose_by_component(model["model"], np.array(model["normed_pre_data"].select(model["normed_pre_data"].columns[1]).collect()).astype(np.float32), model["model_samples"])
    #Initialize dictionary
    temp_dict = {}
    #Fill dictionary
    for i in one_step_dists.keys():
        temp_data = [[v,float(h)] for v, h in zip(model["normed_pre_data"].select("reference_date").rdd.flatMap(lambda x : x).collect(), np.array(one_step_dists[i].mean()))]
        temp_dict[i.name[:-1]] = spark.createDataFrame(temp_data, StructType([
                                                                  StructField('reference_date', DateType(), True),
                                                                  StructField(i.name[:-1], DoubleType(), True)]) )
    temp_list = [temp_dict[h] for h in list(temp_dict.keys())]#transform the dictionary in list
    #Create final dataframe
    out = reduce(lambda x,y: x.join(y, on = "reference_date", how = "left").sort("reference_date"), temp_list)
    #Add observed values
    out = out.join(model["normed_pre_data"].select(*model["normed_pre_data"].columns[0:2]).withColumnRenamed(model["normed_pre_data"].columns[1],"Observed"), on = "reference_date", how = "left").sort("reference_date")
    return out

#Define the function to plot component of structural time series model
def plot_state_component_causal_impact(model_name, spec = False):
    
    color = ["DarkOrange","Chartreuse","HotPink","DarkOrchid","Sienna","MediumBlue","FireBrick","DimGray"]
    component = get_component_stand(model_name = model_name, spec = spec)
    title  =  '<b> Model state decomposition standardized scale </b>'
    columns = component.columns[1:][::-1]
    
    fig = go.Figure()
    for i in range(0,len(columns)):
        fig.add_trace(go.Scatter(x = np.array(component.select("reference_date").collect()).reshape(-1), y = np.array(component.select(columns[i]).collect()).reshape(-1), showlegend = True, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Value</b>: %{y}',name = columns[i], line_color = color[i]))
    
    fig.update_xaxes(showgrid = False,title_text="Time",tickfont_size = 10,title_font_size = 12)
    fig.update_yaxes(showgrid = False,title_text="NPC", tickfont_size = 10,title_font_size = 12)
    
    fig.update_layout(title_text = title,plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
    
    fig.show()
    return

#Define the function to get regressor importance of causal impact model
def get_regressor_importance(model_name, spec):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)
    temp_dict = {}
    #Get all model coefficient estimates
    if model["model_args"]["fit_method"] == "hmc":
        for (param, param_draws) in zip(model["model"].parameters, model["model_samples"]):
            temp_dict[param.name] = np.mean(param_draws, axis=0)
    else:
        for name, values in model["model_samples"].items():
            temp_dict[name] = np.mean(values,axis = 0)
    for i in list(temp_dict.keys()):
        check_weights = re.search("weights",i)
        if check_weights is not None :
            break#stop to the interesting i, in roder to retrieve regressor coefficient estimates
    #Create output data
    out_data = [[v,float(h)] for v, h in zip(model["data"].columns[2:],np.abs(temp_dict[i]))]
    #Create output dataset
    out = spark.createDataFrame(out_data, StructType([StructField('regressor_name', StringType(), True),
                                                  StructField("regressor_importance", DoubleType(), True)]) ).sort("regressor_importance", ascending = False)
    return out

#Define the function to plot regressor importance of causal impact model
def plot_regressor_importance_causal_impact(model_name,spec = False):
    
    title  =  '<b> Regressor Importance </b>'
    
    importance = get_regressor_importance(model_name = model_name, spec = spec)
    
    fig = go.Figure()
    fig.add_trace(go.Bar(x=np.array(importance.select("regressor_importance").collect()).reshape(-1), y = np.array(importance.select("regressor_name").collect()).reshape(-1),
                         hovertemplate = '<b>Regressor</b>: %{y}' '<b> Importance</b>: %{x}',
                         marker_color = "DodgerBlue",orientation = "h",
                         name = '', showlegend = False))
    
    fig.update_xaxes(showgrid = False,title_text="Importance",tickfont_size = 10,title_font_size = 12)
    fig.update_yaxes(showgrid = False,title_text="Regressor",tickfont_size = 10,title_font_size = 12)
    
    fig.update_layout(yaxis = dict(autorange = "reversed"),title_text = title,plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
    
    fig.show()
    return  

#Define the function to plot diagnostic of causl impact model
def plot_diagnostic_causal_impact(model_name, spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)
    
    title = '<b> Model diagnostic </b>'
    
    inference = model["inferences"]
    inference = inference.filter((inference.reference_date >= model["pre_period"][0]) & (inference.reference_date <= model["pre_period"][1]) ).select(*["reference_date","observed","error","perc_error"])
    
    samp = np.array(inference.sort("error").select("error").collect()).reshape(-1)
    fit = scipy.stats.norm.pdf(samp,np.mean(samp),np.std(samp,ddof = 0))
    qplot = scipy.stats.probplot(samp,fit = True,plot = None)
    fit_quant = qplot[1][1] + qplot[1][0] * qplot[0][0]
    
    err_acf = acf(np.array(inference.select("error").collect()),nlags = 10,fft = False)
    shape_1 = dict(type = "line", xref = "x4", yref ="y4",  x0=0, x1=len(err_acf), y0=-0.1, y1= -0.1,line=dict(color='DarkOrange',width=2,dash="dot"))
    shape_2 = dict(type = "line", xref = "x4", yref ="y4",  x0=0, x1=len(err_acf), y0=0.1, y1= 0.1,line=dict(color='DarkOrange',width=2,dash="dot"))
    
    shape = [shape_1,shape_2]
    
    fig = make_subplots(rows=2, cols=2,subplot_titles= ("<b> Residual time series </b>","<b> Residual histogram </b>", "<b> Residual qq-plot </b>", "<b> Residual auto correlation </b>"))
    
    fig.add_trace(go.Scatter(x = np.array(inference.select("reference_date").collect()).reshape(-1), y = np.array(inference.select("error").collect()).reshape(-1), showlegend = False, mode = 'lines',hovertemplate = '<b>Timestamp</b>: %{x}' '<b> Residual</b>: %{y}',name = "", line_color = 'SlateGrey'),row =1, col= 1)
    
    fig.add_trace(go.Histogram(x=np.array(inference.select("error").collect()).reshape(-1), histnorm='probability density',name = '',
                               showlegend = False,hovertemplate = '<b>Residual</b>: %{x}' '<b> Density</b>: %{y}',marker_color = "Pink"), row = 1, col = 2)
    
    fig.add_trace(go.Scatter(x = samp, y = fit, showlegend = False, mode = 'lines',hovertemplate = '<b>Residual</b>: %{x}' '<b> Normal density</b>: %{y}',name = "", line_color = 'Gold'),row =1, col= 2)
    
    fig.add_trace(go.Scatter(x = np.array(inference.agg({"error" : "avg"}).collect()[0][0]/inference.agg({"observed" : "avg"}).collect()[0][0]), y = np.array(0),showlegend = False, mode = "text", text = "Err. mean = " + str(np.round(inference.agg({"error" : "avg"}).collect()[0][0]/inference.agg({"observed" : "avg"}).collect()[0][0],6)),textposition = 'middle center',
                             textfont = dict(color = 'Black', size = 12), name = "", hoverinfo = "skip"),row = 1, col = 2)
    
    fig.add_trace(go.Scatter(x = qplot[0][0], y = qplot[0][1], showlegend = False, mode = 'markers',hovertemplate = '<b>Th. quantile</b>: %{x}' '<b> Obs. quantile</b>: %{y}',name = "", marker_color = 'DodgerBlue'),row =2, col= 1)
    fig.add_trace(go.Scatter(x = qplot[0][0], y = fit_quant, showlegend = False, mode = 'lines',hoverinfo = 'skip',name = "", line_color = 'Red'),row =2, col= 1)
    
    fig.add_trace(go.Bar(x=[i for i in range(0,len(err_acf))], y=err_acf,showlegend = False,name = " ",
                                 marker_color = "PaleGreen",hovertemplate = '<b>Lag</b>: %{x}' '<b> Auto corr</b>: %{y}'),row = 2, col = 2)
    
    fig.update_xaxes(showgrid = False,title_text="Time",tickfont_size = 10,title_font_size = 12,row = 1,col = 1)
    fig.update_yaxes(showgrid = False,title_text="Residual", tickfont_size = 10,title_font_size = 12,row = 1, col= 1)
    
    fig.update_xaxes(showgrid = False,title_text="Residual",tickfont_size = 10,title_font_size = 12,row = 1,col = 2)
    fig.update_yaxes(showgrid = False,title_text="Density", tickfont_size = 10,title_font_size = 12,row = 1, col= 2)
    
    fig.update_xaxes(showgrid = False,title_text="Theoretical quantiles",tickfont_size = 10,title_font_size = 12,row = 2,col = 1)
    fig.update_yaxes(showgrid = False,title_text="Observed quantiles", tickfont_size = 10,title_font_size = 12,row = 2, col= 1)
    
    fig.update_xaxes(showgrid = False,title_text="Lag",tickfont_size = 10,title_font_size = 12,row = 2,col = 2)
    fig.update_yaxes(showgrid = False,title_text="Auto correlation", tickfont_size = 10,title_font_size = 12,row = 2, col= 2)
    
    fig.update_layout(shapes = shape,title_text = title,plot_bgcolor='rgb(255,255,255)',width = 1200, height=600)
    
    fig.show()
    return

