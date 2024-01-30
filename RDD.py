from pyspark import SparkContext

sc = SparkContext("local", "PySpark Example")

# RDD Creation:
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# RDD Transformation:
squared_rdd = rdd.map(lambda x: x ** 2)

# RDD Action:
result_list = squared_rdd.collect()
print(result_list)

count = rdd.count()
print("Number of elements:", count)

sc.stop()
