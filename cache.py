from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("SparkByExamples.com") \
    .getOrCreate()

# Create data
columns = ["Seqno", "Quote"]
data = [
    ("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy.")
]

# Create DataFrame
df = spark.createDataFrame(data, columns)

# Cache DataFrame
dfCache = df.cache()

# Show DataFrame
dfCache.show(truncate=False)

# Stop the SparkSession
spark.stop()
