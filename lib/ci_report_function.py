# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 16:20:20 2022

@author: aless
"""
#In this script we define all function that are need to visualize all elements of a trained and saved model

#Import configuration script

from config_dir import config

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql import DataFrame

from pyspark.sql.functions import *

import boto3
import imp
import warnings
import pickle
import re
import pyspark.pandas as ps
from statsmodels.tsa.stattools import acf,pacf
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings('ignore')
imp.reload(config)



#Initialize spark session for all funtions

spark = config.spark_session

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

#Create function for the visualization of input data
def plot_input_data(input_type, model_name, spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)
    data = model["data"]
    campaign_start = model["campaign_start"]
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
def plot_target_corr(model_name, spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    #Reduce data to pre-interventon period
    model = load_model_pickle(model_name = model_name_total)
    data = model["data"]
    campaign_start = model["campaign_start"]
    data_corr = data[data["reference_date"] < campaign_start]
    if np.std(np.array(data_corr.select(data_corr.columns[1]).collect()), ddof = 0) == 0:
        raise ValueError("Targte metric has 0 variance, no sense to visualize this plot")
    else:
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
def plot_target_feature_corr(model_name,spec = False):
    
    if spec == False:
        model_name_total = model_name
    else:
        model_name_total = model_name + "_spec"
    
    model = load_model_pickle(model_name = model_name_total)
    data_perc_change = model["data"]
    campaign_start = model["campaign_start"]
    yearly_seasonality = model["yearly_seasonality"]
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


