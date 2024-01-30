from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MapExample").getOrCreate()

data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data)
squared_rdd = rdd.map(lambda x: x * x)  # Apply a function to each element
print(squared_rdd.collect())
