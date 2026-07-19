from pyspark.sql import SparkSession


# spark = SparkSession.builder \
#     .appName("ChessKafkaConsumer") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0") \
#     .master("local[*]") \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("ChessRealTime") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.postgresql:postgresql:42.7.3") \
    .getOrCreate()
# Tắt bớt log rác của Spark để màn hình Console dễ nhìn hơn
spark.sparkContext.setLogLevel("WARN")
