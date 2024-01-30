from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a SparkSession
spark = SparkSession.builder.appName("RDDToDataFrame").getOrCreate()

# Sample data
data = [
    ("John", 30, {"city": "New York", "country": "USA"}),
    ("Jane", 25, {"city": "London", "country": "UK"})
]

# Define schema
schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("address", StructType([
        StructField("city", StringType(), True),
        StructField("country", StringType(), True)
    ]), True)
])

# Create DataFrame from RDD
df = spark.createDataFrame(data, schema)

# Display DataFrame
df.show()


spark.stop()
