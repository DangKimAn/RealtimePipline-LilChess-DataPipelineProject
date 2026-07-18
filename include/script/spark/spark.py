from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName("ChessKafkaConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
    .master("local[*]") \
    .getOrCreate()

# Tắt bớt log rác của Spark để màn hình Console dễ nhìn hơn
spark.sparkContext.setLogLevel("WARN")
