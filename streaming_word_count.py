import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

# Run 'nc -l <some port number like 9999' in a different tab.
# Type words in the console so that readStream can pick it

if __name__ == "__main__":
	if len(sys.argv) != 3:
		print("Usage: spark-submit m01_demo1_netcat.py <hostname> <port>", file=sys.stderr)
		exit(-1)

	host = sys.argv[1]
	port = int(sys.argv[2])
	spark = SparkSession\
		.builder\
		.appName("Netcatwordcount")\
		.getOrCreate()

	spark.sparkContext.setLogLevel("ERROR")

	lines = spark\
		.readStream\
		.format('socket')\
		.option('host',host)\
		.option('port',port)\
		.load()

	words = lines.select(
		explode(
			split(lines.value, ' ') #space delimiter to split lines as words
		).alias('word')
	)

	wordCounts = words.groupBy('word')\
					  .count()
 #format here is the data sink
	query = wordCounts.writeStream\
					  .outputMode('complete')\
					  .format('console')\
					  .start()
	query.awaitTermination()
