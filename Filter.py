from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MapExample").getOrCreate()

numbers = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
numbers_rdd = spark.sparkContext.parallelize(numbers)

even_numbers_rdd = numbers_rdd.filter(lambda x: x % 2 == 0)  # Filter even numbers

print(even_numbers_rdd.collect())
