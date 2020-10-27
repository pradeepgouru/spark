#Below code is to load mysql payment_data table to hive S3 table path. Change format accordingly if writing to a table

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext, SQLContext
conf = SparkConf()
spark = SparkSession.builder\
                    .appName("Sparkclient") \
                    .config(conf=conf) \
					.enableHiveSupport() \
                    .getOrCreate()
jdbcUrl = "jdbc:mysql://<rds-hostname>/qcred?user=admin&password=*******"

df = spark.read.format("jdbc").option("url", jdbcUrl).option("query", "select * from payment_data").load()
df.write.format("csv").mode("overwrite").option("compression", "gzip").save("s3://qubole-sup/pradeepg/payment_data/")