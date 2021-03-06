import sys
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *
 
spark =  SparkSession.builder.appName("nyc-taxi-demo").getOrCreate()
 
YellowTaxiDF =  spark.read.option("header",True).csv("s3://saama-anantha-bootcamp/nyc-taxi-records/yellow-taxi/")
GreenTaxiDF =  spark.read.option("header",True).csv("s3://saama-anantha-bootcamp/nyc-taxi-records/green-taxi/")
 
YellowTaxiDF.createOrReplaceTempView("yellow_taxi")
GreenTaxiDF.createOrReplaceTempView("green_taxi")
 
TaxSummaryDF = spark.sql("SELECT 'yellow' as type,  round(avg(trip_distance),2) AS avgDist,  round(avg(total_amount/trip_distance),2) AS avgCostPerMile,  round(avg(total_amount),2) avgCost from yellow_taxi WHERE trip_distance >  0 AND total_amount > 0 UNION SELECT 'green' as type, round(avg(trip_distance),2)  AS avgDist, round(avg(total_amount/trip_distance),2) AS avgCostPerMile,  round(avg(total_amount),2) avgCost from green_taxi WHERE trip_distance > 0  AND total_amount > 0")
 
TaxSummaryDF.write.mode("overwrite").parquet("s3://saama-anantha-bootcamp/nyc-taxi-records/output/")
 
spark.stop()
