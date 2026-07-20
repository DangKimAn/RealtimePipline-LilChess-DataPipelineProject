from spark import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json , when, current_timestamp , expr, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType
from dotenv import load_dotenv
import os 


script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')

load_dotenv(env_path_file)


STOCKFISH_PATH = os.getenv('STOCKFISH_PATH') 

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
API_KEY = os.getenv('API_KEY')
API_SECRET = os.getenv('API_SECRET')

game_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("winner", StringType(), True),
    StructField("winner_id", StringType(), True),
    StructField("status_name", StringType(), True),
    StructField("status_id", IntegerType(), True),
    StructField("turns", IntegerType(), True),
    StructField("white_id", StringType(), True),
    StructField("black_id", StringType(), True),
    StructField("rated", BooleanType(), True),
    StructField("speed", StringType(), True),
    StructField("perf", StringType(), True),
    StructField("createdAt", LongType(), True),

])

df_kafka_move = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";') \
    .option("subscribe", "game") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed = df_kafka_move.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), game_schema).alias("data")).select("data.*") 

# df_raw = df_parsed.filter(
#     col('winner_id').isNull()
# )

df = df_parsed.filter(
    (col('game_id').isNotNull())&
    (col('turns') >= 0)
).select(
    [
        col('game_id'),
        col('winner'),
        col('status_id'),
        col('turns'),
        col('white_id'),
        col('black_id'),
        col('speed'),
        col('perf'),
        (col('createdAt') / 1000).cast('timestamp').alias('created_at'),

    ]
).withColumn(
    'winner', 
    when(col('winner') == 'white' , 0 ).
    when(col('winner') == 'black' , 1).
    otherwise(-1)
)

query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()


query.awaitTermination()
