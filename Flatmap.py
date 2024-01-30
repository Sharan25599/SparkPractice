from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("MapExample").getOrCreate()

words = ["hello world", "how are you"]
words_rdd = spark.sparkContext.parallelize(words)

# Splits strings into words
words_flat_rdd = words_rdd.flatMap(lambda x: x.split(" "))

print(words_flat_rdd.collect())
