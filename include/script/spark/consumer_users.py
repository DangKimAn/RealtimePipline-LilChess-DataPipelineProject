

import os
from pyspark.sql.functions import     col,from_json,arrays_zip,explode,when,lit,to_timestamp,trim,length
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,LongType,ArrayType,

from spark import spark

# ──────────────────────────────────────────────
# 1. SCHEMA  (mỗi field là mảng vì producer gộp 2 user/message)
# ──────────────────────────────────────────────
user_schema = StructType([
    StructField("id",           ArrayType(StringType()),  True),
    StructField("username",     ArrayType(StringType()),  True),
    StructField("title",        ArrayType(StringType()),  True),
    StructField("created_at",   ArrayType(LongType()),    True),   # epoch ms
    StructField("location",     ArrayType(StringType()),  True),
    StructField("real_name",    ArrayType(StringType()),  True),
    StructField("fide_rating",  ArrayType(StringType()),  True),   # có thể null / ""
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

# ──────────────────────────────────────────────
# 2. TIỆN ÍCH
# ──────────────────────────────────────────────
def safe_cast(column_name: str, target_type: str):
    """
    Ép kiểu an toàn: nếu giá trị là null hoặc chuỗi rỗng → trả về null,
    ngược lại cast sang target_type.
    """
    return (
        when(
            col(column_name).isNull() | (trim(col(column_name)) == lit("")),
            lit(None)
        )
        .otherwise(col(column_name).cast(target_type))
    )


# ──────────────────────────────────────────────
# 3. ĐỌC TỪ KAFKA
# ──────────────────────────────────────────────
print("=" * 60)
print("  [USER CONSUMER] Khởi động PySpark — topic: 'user'")
print("=" * 60)

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9094")
    .option("subscribe", "user")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# ──────────────────────────────────────────────
# 4. PARSE JSON → FLATTEN MẢNG → ROWS
# ──────────────────────────────────────────────
# Bước 4.1 — chuyển binary value → string, parse sang struct
df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), user_schema).alias("data"))
    .select("data.*")
)

# Bước 4.2 — zip tất cả các mảng lại rồi explode → mỗi phần tử thành 1 row
columns_to_zip = [
    "id", "username", "title", "created_at", "location",
    "real_name", "fide_rating", "links", "bio", "play_time",
    "url", "all_count", "rated_count", "draw_count", "loss_count", "win_count",
]

df_flat = (
    df_parsed
    .withColumn("zipped", arrays_zip(*[col(c) for c in columns_to_zip]))
    .select(explode(col("zipped")).alias("row"))
    .select(
        col("row.id").alias("id"),
        col("row.username").alias("username"),
        col("row.title").alias("title"),
        col("row.created_at").alias("created_at_raw"),     # epoch ms
        col("row.location").alias("location"),
        col("row.real_name").alias("real_name"),
        col("row.fide_rating").alias("fide_rating"),
        col("row.links").alias("links"),
        col("row.bio").alias("bio"),
        col("row.play_time").alias("play_time"),           # giây
        col("row.url").alias("url"),
        col("row.all_count").alias("all_count"),
        col("row.rated_count").alias("rated_count"),
        col("row.draw_count").alias("draw_count"),
        col("row.loss_count").alias("loss_count"),
        col("row.win_count").alias("win_count"),
    )
)

# ──────────────────────────────────────────────
# 5. TRANSFORM & LÀM SẠCH
# ──────────────────────────────────────────────
df_users = (
    df_flat
    # Loại bỏ các bản ghi không có user_id hoặc username
    .filter(
        col("id").isNotNull()
        & col("username").isNotNull()
    )
    .select(
        # ── Định danh ──────────────────────────────────────────────────
        col("id").alias("user_id"),
        col("username"),

        # ── Hồ sơ — null thì cứ để null, Spark tự giữ ─────────────────
        col("title"),
        col("location"),
        col("real_name"),
        col("links"),
        col("bio"),
        col("url"),

        # fide_rating đến dưới dạng String (có thể ""), cần safe_cast sang integer
        safe_cast("fide_rating", "integer").alias("fide_rating"),

        # ── Thời gian ──────────────────────────────────────────────────
        # created_at: epoch ms → timestamp
        (col("created_at_raw") / 1000).cast("timestamp").alias("created_at"),

        # play_time: đơn vị giây
        col("play_time").cast("long").alias("play_time_seconds"),

        # ── Thống kê ván cờ ────────────────────────────────────────────
        col("all_count"),
        col("rated_count"),
        col("win_count"),
        col("draw_count"),
        col("loss_count"),

        # ── Tỷ lệ thắng (computed column) ──────────────────────────────
        when(
            col("all_count").isNotNull() & (col("all_count") > 0),
            (col("win_count").cast("double") / col("all_count").cast("double") * 100)
        )
        .otherwise(lit(None))
        .alias("win_rate_pct"),
    )
)

# ──────────────────────────────────────────────
# 6. OUTPUT — ghi ra console (debug / demo)
#    Có thể thay bằng sink khác:
#    .format("jdbc") → PostgreSQL
#    .format("delta") → Delta Lake
#    .format("kafka") → forward tới topic khác
# ──────────────────────────────────────────────
query = (
    df_users
    .writeStream
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 20)
    .trigger(processingTime="5 seconds")
    .start()
)

print("[USER CONSUMER] Đang lắng nghe dữ liệu từ Kafka topic 'user'...")
print("[USER CONSUMER] Nhấn Ctrl+C để dừng.\n")

query.awaitTermination()