import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import traceback
# Xử lý path chuẩn chỉnh của bro
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
load_dotenv(env_path_file)

def get_spark_session(mode="streaming"):

    SCALA_VERSION = os.getenv('SCALA_VERSION')
    SPARK_KAFKA_VERSION = os.getenv('SPARK_KAFKA_VERSION')
    POSTGRES_JDBC_VERSION = os.getenv('POSTGRES_JDBC_VERSION')
    env_type = os.getenv("ENVIRONMENT", "local")
    memory =os.getenv('MEMORY', 2)
    # Khởi tạo builder cơ bản
    builder = SparkSession.builder
    try:
        if mode == "streaming":
            # Nạp thư viện Kafka + Postgres cho Hot Path
            builder = builder.appName("ChessRealTime") \
                .config("spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_KAFKA_VERSION},org.postgresql:postgresql:{POSTGRES_JDBC_VERSION}")
                
        elif mode == "batch":
            aws_access_key = os.getenv("AWS_ACCESS_KEY")
            aws_secret_key = os.getenv("AWS_SECRET_KEY")
            region = os.getenv('region')
            
            builder = builder.appName("ColdPath-S3-to-RDS-Games") \
                .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.1,com.amazonaws:aws-java-sdk-bundle:1.12.367,org.postgresql:postgresql:42.6.0") \
                .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
                .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
                .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")

        if env_type == "local":
            builder = builder.config("spark.driver.host", "127.0.0.1") \
                                .config("spark.driver.bindAddress", "127.0.0.1")\
                                .config("spark.network.timeout", "800s") \
                                .config("spark.executor.heartbeatInterval", "60s")
        elif env_type == "production":
            pass

        builder = builder.config("spark.driver.memory", f"{memory}g") \
                            .config("spark.executor.memory", f"{memory}g")
        # Chốt cấu hình và tạo Session
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        
        return spark
    except Exception as e:
        traceback.print_exc()
