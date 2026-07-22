
# from kafka.producer import KAFKA_BOOTSTRAP_SERVER
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import chess.engine
import pandas as pd 
from dotenv import load_dotenv

from spark import spark
# 1. THÊM prev_move VÀO SCHEMA




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

engine = None


move_schema = StructType([
    StructField("game_id", StringType(), True),
    StructField("prev_fen", StringType(), True), # Đây chính là prev_fen của bạn
    StructField("move", StringType(), True),
    StructField("fen", StringType(), True),
    StructField("color_turn", StringType(), True),
    StructField("time_left", IntegerType(), True),
    StructField("num_of_moves", IntegerType(), True)
])

# spark = SparkSession.builder.appName("ChessRealTime").getOrCreate()

df_kafka_move = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{API_KEY}" password="{API_SECRET}";') \
    .option("subscribe", "move") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed_move = df_kafka_move.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), move_schema).alias("data")).select("data.*") \
    .withColumn("color_turn", when(col("color_turn") == "white", 1).when(col("color_turn") == "black", 0).otherwise(col("color_turn")).cast(IntegerType()))



def get_engine():
    global engine
    if engine is None:
        engine = chess.engine.SimpleEngine.popen_uci(STOCKFISH_PATH)
    return engine

@pandas_udf("struct<cp_loss:double,remark:int>")
def stockfish_udf(game_id_series: pd.Series, fen_series: pd.Series, uci_series: pd.Series) -> pd.DataFrame:
    # Mở Engine
    engine = get_engine()
    
    results = []
    
    for game_id, fen, uci in zip(game_id_series, fen_series, uci_series):
        try:
            if not fen or not uci:
                results.append({"cp_loss": 0.0, "remark": -1})
                continue
            
            board = chess.Board(fen)
            move = chess.Move.from_uci(uci)
            
            # Kiểm tra nước đi hợp lệ (Chặn đứng IllegalMoveError)
            if move not in board.legal_moves:
                results.append({"cp_loss": 0.0, "remark": -2})
                continue
            
            # Tính điểm TRƯỚC khi đi
            info_before = engine.analyse(board, chess.engine.Limit(depth=10))
            score_before = info_before["score"].white().score(mate_score=10000)
            
            # Thực hiện nước đi
            board.push(move)
            
            # Tính điểm SAU khi đi
            info_after = engine.analyse(board, chess.engine.Limit(depth=10))
            score_after = info_after["score"].white().score(mate_score=10000)
            
           
            if board.turn == chess.WHITE: 
                cp_loss = float(score_after - score_before) 
            else:
                cp_loss = float(score_before - score_after) 
            
            cp_loss = max(0.0, cp_loss)
            
            # Phân loại
            if cp_loss >= 300:
                remark = 3
            elif cp_loss >= 150:
                remark = 2
            elif cp_loss >= 50:
                remark = 1
            else:
                remark = 0
                
            results.append({"cp_loss": cp_loss, "remark": remark})
            
        except Exception:
            results.append({"cp_loss": 0.0, "remark": -3})
            
    return pd.DataFrame(results)



df_evaluated = df_parsed_move \
    .withColumn("eval", stockfish_udf(col('game_id'), col("prev_fen"), col("move"))) \
    .withColumn("cp_loss", col("eval.cp_loss")) \
    .withColumn("remark", col("eval.remark")) \
    .drop("eval")



def write_to_postgres(batch_df, epoch_id):
    jdbc_url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    batch_df.write \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", "gold.game_evaluations") \
        .option("user", DB_USER) \
        .option("password", DB_PASSWORD) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()


# Cấu hình Trigger để chống ngập lụt Database
query = df_evaluated.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='15 seconds') \
    .start()



query.awaitTermination()