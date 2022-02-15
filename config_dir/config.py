import pyspark
from pyspark.sql import SparkSession

#Create spark session

#Create SparkSession

spark_session = SparkSession.builder \
                    .master("ML_cluster[1]") \
                    .appName("SparkCIsession") \
                    .getOrCreate()

covid_link = {"GB" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-uk.csv",
              "IT" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-it.csv",
              "DE" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-de.csv", #here there are problems
              "CH" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-ch.csv", #here there are problems
              "NL" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-nl.csv", #this is the one could be deleted because a sheet
              "BE" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-be.csv",
              "AT" : "https://raw.githubusercontent.com/covid19-eu-zh/covid19-eu-data/master/dataset/covid-19-at.csv"
    }