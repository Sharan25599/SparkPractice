from pyspark import SparkContext
sc = SparkContext("local[*]")

# Create an RDD with word counts for each book
word_counts = sc.parallelize([("Book 1", 100), ("Book 2", 250), ("Book 3", 150)])

# Define a function to add two word counts
def add_counts(a, b):
    return a + b

# reduce with the add_counts function to get the total word count
total_words = word_counts.reduce(add_counts)

print("Total words in all books:", total_words)
sc.stop()
