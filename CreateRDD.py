from pyspark import SparkContext


sc = SparkContext("local[*]", "RDD_Example")

data = [1, 2, 3, 4, 5]
rdd1 = sc.parallelize(data)

text_file = sc.textFile("path/to/text/file.txt")

print(rdd1.collect())
print(text_file.collect())



