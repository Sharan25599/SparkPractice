from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .master("local[5]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Create RDD with default partitions (5 in this case)
rdd = spark.sparkContext.parallelize(range(0, 20))
print("From local[5]", rdd.getNumPartitions())

# Create RDD with 6 partitions
rdd1 = spark.sparkContext.parallelize(range(0, 20), 6)
print("parallelize:", rdd1.getNumPartitions())


