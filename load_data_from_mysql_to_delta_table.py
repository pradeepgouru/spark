from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext, SQLContext
from datetime import datetime
from datetime import timedelta

conf = SparkConf()
spark = SparkSession.builder\
                    .appName("Sparkclient") \
                    .config(conf=conf) \
					.enableHiveSupport() \
                    .getOrCreate()


tnow = datetime.now()
tdelta = timedelta(hours=12)
delta = tnow - tdelta
starttime = str(delta)[0:13]+":00:00 "
endtime = str(tnow)[0:13]+":00:00"
jdbcUrl = "jdbc:mysql://database-3.xxxxxxxx.us-east-1.rds.amazonaws.com/qcred?user=admin&password=XXXXXXXX"
Query = " insert overwrite table payment_data_delta as select * from payment_data where created_at >= '"+starttime+"' or updated_at >= '"+endtime+"'"
print(Query)

df = spark.read.format("jdbc").option("url", jdbcUrl).option("query", Query).load()
