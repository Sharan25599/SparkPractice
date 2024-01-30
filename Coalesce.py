import pyspark

# Create a SparkContext
sc = pyspark.SparkContext()

# Create an RDD with 5 elements and 3 partitions
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data, 3)

# Check the initial number of partitions
print("Initial number of partitions:", rdd.getNumPartitions())  # Output: 3

# Coalesce the RDD to 2 partitions
coalesced_rdd = rdd.coalesce(2)

# Check the number of partitions after coalescing
print("Number of partitions after coalescing:", coalesced_rdd.getNumPartitions())  # Output: 2

# Optionally, view the distribution of elements across partitions
print("Distribution of elements:", coalesced_rdd.glom().collect())
