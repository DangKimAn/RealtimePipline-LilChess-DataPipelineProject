
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, pandas_udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import chess.engine
import pandas as pd 
from dotenv import load_dotenv
from spark import spark
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

# 1. THÊM prev_move VÀO SCHEMA
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
    .option("kafka.bootstrap.servers", "localhost:9094") \
    .option("subscribe", "move") \
    .option("startingOffsets", "latest") \
    .load()

df_parsed_move = df_kafka_move.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), move_schema).alias("data")).select("data.*")




##################### IMPORT  STOCKFISH MODULE 




script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')

load_dotenv(env_path_file)


STOCKFISH_PATH = os.getenv('STOCKFISH_PATH') 

DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')


engine = None

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
            
    engine.quit()
    return pd.DataFrame(results)



df_evaluated = df_parsed_move \
    .withColumn("eval", stockfish_udf(col('game_id'), col("prev_fen"), col("move"))) \
    .withColumn("cp_loss", col("eval.cp_loss")) \
    .withColumn("remark", col("eval.remark")) \
    .drop("eval")

# Khởi tạo Connection Pool
pg_pool = None

def init_connection_pool():
    global pg_pool
    if pg_pool is None:
        try:
            pg_pool = pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                host=DB_HOST,
                port=DB_PORT,
                dbname=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            print("Connection pool created successfully")
        except Exception as e:
            print(f"Error creating connection pool: {e}")

def create_table_if_not_exists():
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        cursor = conn.cursor()
        create_table_query = """
        CREATE TABLE IF NOT EXISTS game_evaluations (
            game_id VARCHAR(50),
            prev_fen TEXT,
            move VARCHAR(20),
            fen TEXT,
            color_turn VARCHAR(10),
            time_left INTEGER,
            num_of_moves INTEGER,
            cp_loss DOUBLE PRECISION,
            remark INTEGER,
            PRIMARY KEY (game_id, num_of_moves)
        );
        """
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()
        print("Table 'game_evaluations' verified/created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")

def write_to_postgres(batch_df, epoch_id):
    if batch_df.isEmpty():
        return
        
    records = batch_df.collect()
    
    # Chuẩn bị dữ liệu để insert
    values = []
    for row in records:
        values.append((
            row['game_id'], 
            row['prev_fen'], 
            row['move'], 
            row['fen'], 
            row['color_turn'], 
            row['time_left'], 
            row['num_of_moves'],
            row['cp_loss'],
            row['remark']
        ))
        
    insert_query = """
    INSERT INTO game_evaluations (
        game_id, prev_fen, move, fen, color_turn, time_left, num_of_moves, cp_loss, remark
    ) VALUES %s
    ON CONFLICT (game_id, num_of_moves) DO NOTHING;
    """
    
    conn = None
    try:
        conn = pg_pool.getconn()
        cursor = conn.cursor()
        execute_values(cursor, insert_query, values)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Error inserting batch: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            pg_pool.putconn(conn)

init_connection_pool()
create_table_if_not_exists()

# Cấu hình Trigger để chống ngập lụt Database
query = df_evaluated.writeStream \
    .outputMode("append") \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='5 seconds') \
    .start()


# query = df_evaluated.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", "false") \
#     .start()

query.awaitTermination()