# spark
My spark code

*CreateDataFrame*

  SparkSession.createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True)¶
  Creates a DataFrame from an RDD, a list or a pandas.DataFrame.
  
>>> l = [('Pradeep',30)]
>>> spark.createDataFrame(l)
DataFrame[_1: string, _2: bigint]
>>> spark.createDataFrame(l,['name','age']).collect()
[Row(name='Pradeep', age=30)]
>>> # Dictionary mapping
>>> l = [{'name':'pradeep','age':30}]
>>> spark.createDataFrame(l).collect()
/Users/pradeepg/spark/spark-2.4.7-bin-hadoop2.7/python/pyspark/sql/session.py:346: UserWarning: inferring schema from dict is deprecated,please use pyspark.sql.Row instead
  warnings.warn("inferring schema from dict is deprecated,"
[Row(age=30, name='pradeep')]
>>> rdd = sc.parallelize(l)
>>> spark.createDataFrame(rdd).collect()
/Users/pradeepg/spark/spark-2.4.7-bin-hadoop2.7/python/pyspark/sql/session.py:366: UserWarning: Using RDD of dict to inferSchema is deprecated. Use pyspark.sql.Row instead
  warnings.warn("Using RDD of dict to inferSchema is deprecated. "
[Row(age=30, name='pradeep')]
>>> df = spark.createDataFrame(rdd, ['name','age'])
>>> df.collect()
[Row(name=30, age='pradeep')]

'explode' : While working with structured files like JSON, Parquet, Avro, and XML we often get data in collections like arrays, lists, and maps, In such cases, these explode functions are useful to convert collection columns to rows in order to process in Spark effectively.



