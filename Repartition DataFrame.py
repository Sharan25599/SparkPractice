from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RepartitionExample").getOrCreate()

# DataFrame with 100 rows and initially 5 partitions
df = spark.range(100).repartition(5)

# Increase partitions to 8 using repartition()
df_repartitioned = df.repartition(8)

print("Number of partitions after repartition:", df_repartitioned.rdd.getNumPartitions())  # Output: 8

spark.stop()
