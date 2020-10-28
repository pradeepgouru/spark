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
df = spark.sql("SELECT id,name,t.price,discount_price FROM (SELECT RANK() OVER w AS rank, ROW_NUMBER() OVER w AS row_num, t1.* FROM ( SELECT * FROM payment_data where from_unixtime(unix_timestamp(created_at,'yyyy-MM-dd hh:mm:ss'),'yyyy-MM-dd') = '2020-10-27' UNION ALL SELECT * FROM payment_data_delta ) t1 WINDOW w AS (PARTITION BY t1.id ORDER BY updated_at DESC)) t WHERE rank = 1 and row_num=1")

#Below is to fetch discount_price json string from mysql 
mydf = df.select("discount_price")
import json
def parse_json(array_str):
    json_obj = json.loads(array_str)
    print(array_str)
    for item in json_obj:
        yield (item["discount1"], item["discount2"])

# Define the schema
from pyspark.sql.types import ArrayType, IntegerType, StructType, StructField, StringType
json_schema = ArrayType(StructType([StructField('discount1', StringType(), nullable=False), StructField('discount2', StringType(), nullable=False)]))

# Define udf
from pyspark.sql.functions import udf
udf_parse_json = udf(lambda str: parse_json(str), json_schema)

# Generate a new data frame with the expected schema
df_new = mydf.select(udf_parse_json(mydf.discount_percent).alias("attr_2"))
df_new.show()

#To DO: Add df_new back to the 'df' and write it to output table path

df.write.format("csv").mode("overwrite").option("compression", "gzip").save("s3://qubole-sup/pradeepg/payment_data_history_temp/")
