from include.script.spark.spark import get_spark_session
from pyspark.sql.functions import *
from pyspark.sql.types import * 
import os 
from dotenv import load_dotenv
import argparse 
import traceback
# spark = get_spark_session('batch')

# script_dir = os.path.dirname(os.path.abspath(__file__))
# env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
# load_dotenv(env_path_file)



# bucket_name = os.getenv('bucket_name')

# DB_HOST = os.getenv("DB_HOST")
# DB_PORT = os.getenv('DB_PORT')
# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PASSWORD = os.getenv("DB_PASSWORD")




# parser = argparse.ArgumentParser(description="Process game data for a specific hour.")
# parser.add_argument("--year", required=True, help="Year (e.g. 2026)")
# parser.add_argument("--month", required=True, help="Month (e.g. 07)")
# parser.add_argument("--day", required=True, help="Day (e.g. 21)")
# parser.add_argument("--hour", required=True, help="Hour (e.g. 06)")
# args = parser.parse_args()




# s3_path_dir = f'topics/elo/year={args.year}/month={args.month}/day={args.day}/hour={args.hour}/'
# s3_path = f's3a://{bucket_name}/{s3_path_dir}'


def transform_elo_schema( s3_path, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD):
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    elo_schema = StructType(
        [
            StructField('username', ArrayType(StringType()), True),
            StructField('rd', ArrayType(IntegerType()), True),
            StructField('num_games', ArrayType(IntegerType()), True),
            StructField('rating', ArrayType(IntegerType()), True),
            StructField('type', ArrayType(StringType()), True),
            StructField('prov', ArrayType(StringType()), True),
            StructField('prog', ArrayType(IntegerType()), True),
        ]
    )
    try:
        spark = get_spark_session('batch')
        df_parsed = spark.read.format('json').schema(elo_schema).load(s3_path)
        df_zipped = df_parsed.filter(col('username').isNotNull()).withColumn(
            'zipped',
            arrays_zip(
                col('username'),
                col('rd'),
                col('num_games'),
                col('rating'),
                col('type'),
                col('prov'),
                col('prog')
            )
        )


        df_exploded = df_zipped.select(explode(col('zipped')).alias('data'))
        df = df_exploded.withColumn(
            'data.prov',
            when(trim(col('data.prov')) == 'true' , True).when(
                trim(col('data.prov')) == 'false', False).otherwise(
                    lit(None)
                )
        ).select(
            col('data.username').alias('username'),
                col('data.rd').alias('rd'),
                col('data.num_games').alias('num_games'),
                col('data.rating').alias('rating'),
                col('data.type').alias('type'),
                col('data.prov').cast('boolean').alias('prov'),
                col('data.prog').alias('prog')
        )
        df.show(truncate=False)

        df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", "gold.fact_elo") \
                .option("user", DB_USER) \
                .option("password", DB_PASSWORD) \
                .option("driver", "org.postgresql.Driver")\
                .mode("append")\
                .save()

        
        print('day du lieu len rds thanh cong ')
        return True
    except Exception as e:
        traceback.print_exc()
        return False

# transform_elo_schema(spark, s3_path, elo_schema, DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD)
    