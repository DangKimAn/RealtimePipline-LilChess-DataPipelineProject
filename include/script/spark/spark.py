import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
load_dotenv(env_path_file)

SCALA_VERSION = os.getenv('SCALA_VERSION')
SPARK_KAFKA_VERSION = os.getenv('SPARK_KAFKA_VERSION')
POSTGRES_JDBC_VERSION = os.getenv('POSTGRES_JDBC_VERSION')

spark = SparkSession.builder \
    .appName("ChessRealTime") \
    .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_KAFKA_VERSION},org.postgresql:postgresql:{POSTGRES_JDBC_VERSION}") \
    .getOrCreate()
# Tắt bớt log rác của Spark để màn hình Console dễ nhìn hơn
spark.sparkContext.setLogLevel("WARN")
