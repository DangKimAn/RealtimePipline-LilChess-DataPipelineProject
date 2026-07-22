from spark import spark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json , when, current_timestamp , expr, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType

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

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "game") \
    .option("startingOffsets", "latest") \
    .load()

df_string = df_kafka.selectExpr("CAST(value AS STRING) as json_string")

df_parsed = df_string.select(from_json(col("json_string"), game_schema).alias("data")).select("data.*")


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