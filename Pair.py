from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkByExample") \
    .master("local[*]") \
    .getOrCreate()

# Create an RDD
data = ["Germany India USA", "USA India Russia", "India Brazil Canada China"]
rdd = spark.sparkContext.parallelize(data)

# Transform RDDs
wordsRdd = rdd.flatMap(lambda line: line.split(" "))
pairRDD = wordsRdd.map(lambda word: (word, 1))

# Print the pairRDD elements
for element in pairRDD.collect():
    print(element)
