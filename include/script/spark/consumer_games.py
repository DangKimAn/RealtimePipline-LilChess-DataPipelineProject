from spark import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json , when, current_timestamp , expr, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType
from dotenv import load_dotenv
import os 
import argparse

spark = get_spark_session('batch')



script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
load_dotenv(env_path_file)



bucket_name = os.getenv('bucket_name')

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")



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



parser = argparse.ArgumentParser(description="Process game data for a specific hour.")
parser.add_argument("--year", required=True, help="Year (e.g. 2026)")
parser.add_argument("--month", required=True, help="Month (e.g. 07)")
parser.add_argument("--day", required=True, help="Day (e.g. 21)")
parser.add_argument("--hour", required=True, help="Hour (e.g. 06)")
args = parser.parse_args()

s3_path_dir = f'topics/game/year={args.year}/month={args.month}/day={args.day}/hour={args.hour}/'
s3_path = f's3a://{bucket_name}/{s3_path_dir}'
print(f"Đang đọc tất cả dữ liệu từ thư mục: {s3_path}")


def transform_game_schema(spark, s3_path, game_schema, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD):
    try:
        df_parsed = spark.read.format('json').schema(game_schema).load(s3_path)
        jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

        df = df_parsed.filter(
            (col('game_id').isNotNull()) &
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

        print("Dữ liệu sau khi transform từ S3:")
        df.show(truncate=False)

        df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "gold.fact_game") \
            .option("user", DB_USER) \
            .option("password", DB_PASSWORD) \
            .option("driver", "org.postgresql.Driver")\
            .mode("append")\
            .save()
            
        print("Đã lưu dữ liệu vào database thành công")
    except Exception as e:
        print(f"Lỗi khi xử lý dữ liệu và ghi vào database: {e}")

transform_game_schema(spark,s3_path, game_schema, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)