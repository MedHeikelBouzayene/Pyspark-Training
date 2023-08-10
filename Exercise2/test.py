from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("RepartitionExample").getOrCreate()

# Sample data
data = [
    (1, "Alice", "New York"),
    (2, "Bob", "San Francisco"),
    (3, "Charlie", "Los Angeles"),
    (4, "David", "New York"),
    (5, "Eva", "San Francisco")
]

columns = ["id", "name", "city"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

# Repartition the DataFrame based on the "city" column
num_partitions = 2
repartitioned_df = df.repartition(num_partitions, "city")

# Show the result
repartitioned_df.show()
repartitioned_df.write.mode('overwrite').options(header = 'True').csv('C:/Users/mhbouzayenne/Desktop/UDEMY COURSE/Pyspark-Training/Exercice2/test')
# Stop the Spark session
spark.stop()
