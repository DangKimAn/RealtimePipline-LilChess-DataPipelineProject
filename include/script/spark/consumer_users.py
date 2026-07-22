from spark import get_spark_session
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json , when, current_timestamp , expr, pandas_udf, trim ,lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, LongType, ArrayType
from pyspark.sql.functions import arrays_zip, explode
import argparse
import os 
from dotenv import load_dotenv



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



user_schema = StructType([
    StructField("id",           ArrayType(StringType()),  True),
    StructField("username",     ArrayType(StringType()),  True),
    StructField("title",        ArrayType(StringType()),  True),
    StructField("created_at",   ArrayType(LongType()),    True),   # epoch ms
    StructField("location",     ArrayType(StringType()),  True),
    StructField("real_name",    ArrayType(StringType()),  True),
    StructField("fide_rating",  ArrayType(IntegerType()),  True),   # có thể null / ""
    StructField("links",        ArrayType(StringType()),  True),
    StructField("bio",          ArrayType(StringType()),  True),
    StructField("play_time",    ArrayType(LongType()),    True),   # giây
    StructField("url",          ArrayType(StringType()),  True),
    StructField("all_count",    ArrayType(IntegerType()), True),
    StructField("rated_count",  ArrayType(IntegerType()), True),
    StructField("draw_count",   ArrayType(IntegerType()), True),
    StructField("loss_count",   ArrayType(IntegerType()), True),
    StructField("win_count",    ArrayType(IntegerType()), True),
])



parser = argparse.ArgumentParser(description="Process game data for a specific hour.")
parser.add_argument("--year", required=True, help="Year (e.g. 2026)")
parser.add_argument("--month", required=True, help="Month (e.g. 07)")
parser.add_argument("--day", required=True, help="Day (e.g. 21)")
parser.add_argument("--hour", required=True, help="Hour (e.g. 06)")
args = parser.parse_args()




s3_path_dir = f'topics/user/year={args.year}/month={args.month}/day={args.day}/hour={args.hour}/'
s3_path = f's3a://{bucket_name}/{s3_path_dir}'
print(f"Đang đọc tất cả dữ liệu từ thư mục: {s3_path}")


def transform_user_schema(spark, s3_path, user_schema, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD):
    df_parsed = spark.read.format('json').schema(user_schema).load(s3_path)
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"

    df_zipped = df_parsed.filter(col('id').isNotNull()).withColumn(
        "zipped",
        arrays_zip(
            col("id"),
            col("username"),
            col("title"),
            col("created_at"),
            col("location"),
            col("real_name"),
            col("fide_rating"),
            col("links"),
            col("bio"),
            col("play_time"),
            col("url"),
            col("all_count"),
            col("rated_count"),
            col("draw_count"),
            col("loss_count"),
            col("win_count")
        )
    )

    df_exploded = df_zipped.select(explode(col("zipped")).alias("data"))

    df = df_exploded.withColumn(
        'location',
        when(col('data.location').isNull() , 'N/A')\
        .when(trim(col('data.location')) == '' , 'N/A')\
        .otherwise(trim(col('data.location')))
    ).withColumn(
        'title',
        when(col('data.title').isNull(), 'N/A').otherwise(trim(col('data.title')))
    ).withColumn(
        'real_name',
        when(col('data.real_name').isNull() , 'N/A')\
        .when(trim(col('data.real_name')) == '' , 'N/A')\
        .otherwise(trim(col('data.real_name')))
    ).withColumn(
        'links',
        when(col('data.links').isNull() , 'N/A')\
        .when(trim(col('data.links')) == '' , 'N/A')\
        .otherwise(trim(col('data.links')))
    ).withColumn(
        'bio',
        when(col('data.bio').isNull() , 'N/A')\
        .when(trim(col('data.bio')) == '' , 'N/A')\
        .otherwise(trim(col('data.bio')))
    ).select(
        col('data.id').alias('user_id'),
        col("data.username").alias("username"),
        col("title").alias("title"),
        (col("data.created_at") / 1000).cast("timestamp").alias("created_at"),
        col("real_name").alias("real_name"),
        col("data.fide_rating").alias("fide_rating"),
        col("links").alias("links"),
        col("bio").alias("bio"),
        col("data.play_time").alias("play_time_seconds"),
        col("data.url").alias("url"),
        col("data.all_count").alias("all_count"),
        col("data.rated_count").alias("rated_count"),
        col("data.draw_count").alias("draw_count"),
        col("data.loss_count").alias("loss_count"),
        col("data.win_count").alias("win_count"),
        col('location').alias('location')
    )

    print("Dữ liệu sau khi transform từ S3:")


    df_dim = df.select(
        col('user_id'),
        col("username"),
        col("title"),
        col("real_name"),
        col("links"),
        col("bio"),
        col("url"),
        col('location')
    )

    df_fact = df.select(
        col("user_id"),
        col("created_at"),
        col("play_time_seconds"),
        col("fide_rating"),
        col("all_count"),
        col("rated_count"),
        col("draw_count"),
        col("loss_count"),
        col("win_count"),

    )

    # df_dim.show(truncate=False)
    # df_fact.show(truncate= False)
    df_dim.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "gold.dim_user") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver")\
        .mode("append")\
        .save()
    

    df_fact.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "gold.fact_user") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver")\
        .mode("append")\
        .save()
        
    print("Đã lưu dữ liệu vào database thành công")

transform_user_schema(spark=spark, s3_path=s3_path, user_schema=user_schema,DB_HOST=DB_HOST,DB_NAME=DB_NAME,DB_PORT=DB_PORT,DB_USER=DB_USER,  DB_PASSWORD=DB_PASSWORD)