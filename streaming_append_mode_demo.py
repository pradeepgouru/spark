from pyspark.sql.types import *
from pyspark.sql import SparkSession

if __name__ == "__main__":

	spark = SparkSession.builder.master("local")\
							.appName("SparkStreamingAppendMode")\
							.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")
  
  #Schema of the London crime dataset
  # True value here is for field is nullable or not
	schema = StructType([StructField("lsoa_code", StringType(), True),\
		    			 StructField("borough", StringType(),True),\
		    			 StructField("major_category", StringType(), True),\
		    			 StructField("minor_category", StringType(), True),\
		    			 StructField("value", StringType(), True),\
						 StructField("year", StringType(),True),\
						 StructField("month", StringType(),True)])

	fileStreamDF = spark.readStream\
						.option("header", "true")\
						.schema(schema)\
						.csv("/Users/pradeepg/spark/data/apache-spark-2-structured-streaming/02/demos/datasets/droplocation")
	print(" ")
	print("Is the stream ready?")
	print(fileStreamDF.isStreaming) #Gives if the data is streaming data

	print(" ")
	print("Schema of the input stream:")
	print(fileStreamDF.printSchema)

	trimmedDF = fileStreamDF.select(
										fileStreamDF.borough,
										fileStreamDF.year,
										fileStreamDF.month,
										fileStreamDF.value)\
							.withColumnRenamed(
									"value",
									"convictions"
									)

	query = trimmedDF.writeStream\
					  .outputMode("append")\
					  .format("console")\
					  .option("truncate", "false")\
					  .option("numRows", 30)\
					  .start()\
					  .awaitTermination()


