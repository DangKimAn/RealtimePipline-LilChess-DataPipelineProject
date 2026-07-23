from datetime import datetime, timedelta
from airflow import DAG
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 7, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_gold_tables_partitioning',
    default_args=default_args,
    description='DAG chạy hàng ngày để tự động tạo partition cho các bảng schema gold',
    schedule='0 1 * * *', # Chạy vào lúc 1:00 AM mỗi ngày
    is_paused_upon_creation=False, 
    catchup=False, # Không chạy bù các ngày trong quá khứ
    tags=['postgres', 'partition', 'gold'],
) as dag:

    # Câu lệnh SQL thực thi hàm tạo partition cho tất cả các bảng
    # Hàm này tự động tính toán từ CURRENT_DATE
    create_partitions_sql = """
    -- 1. Bảng game_evaluations (Theo ngày)
    SELECT gold.create_partitions(
        'game_evaluations', 
        'daily', 
        CURRENT_DATE - 2, 
        CURRENT_DATE + 7
    );

    -- 2. Bảng fact_game (Theo tháng)
    SELECT gold.create_partitions(
        'fact_game', 
        'monthly', 
        (CURRENT_DATE - INTERVAL '1 month')::DATE, 
        (CURRENT_DATE + INTERVAL '2 months')::DATE
    );

    -- 3. Bảng dim_user (Theo tháng)
    SELECT gold.create_partitions(
        'dim_user', 
        'monthly', 
        (CURRENT_DATE - INTERVAL '1 month')::DATE, 
        (CURRENT_DATE + INTERVAL '2 months')::DATE
    );

    -- 4. Bảng fact_user (Theo tháng)
    SELECT gold.create_partitions(
        'fact_user', 
        'monthly', 
        (CURRENT_DATE - INTERVAL '1 month')::DATE, 
        (CURRENT_DATE + INTERVAL '2 months')::DATE
    );

    -- 5. Bảng fact_elo (Theo tháng)
    SELECT gold.create_partitions(
        'fact_elo', 
        'monthly', 
        (CURRENT_DATE - INTERVAL '1 month')::DATE, 
        (CURRENT_DATE + INTERVAL '2 months')::DATE
    );
    """

    execute_partition_creation = SQLExecuteQueryOperator(
        task_id='execute_create_partitions_function',
        conn_id='rds_connection', 
        sql=create_partitions_sql,
    )

    execute_partition_creation