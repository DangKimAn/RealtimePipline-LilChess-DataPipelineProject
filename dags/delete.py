from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

RETENTION_1_MONTH = 30  
RETENTION_3_MONTHS = 90       

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='aws_rds_purge_gold_schema',
    default_args=default_args,
    schedule='0 0 * * 0',
    start_date=datetime(2026, 7, 20),
    catchup=False,
    tags=['cleanup', 'aws-rds', 'gold'],
) as dag:

    # --- NHÓM 1: XÓA DỮ LIỆU 1 THÁNG ---
    purge_evaluations = SQLExecuteQueryOperator(
        task_id='purge_game_evaluations',
        conn_id='rds_connection',
        sql=f"DELETE FROM gold.game_evaluations WHERE collected_at < NOW() - INTERVAL '{RETENTION_1_MONTH} days';"
    )

    purge_fact_game = SQLExecuteQueryOperator(
        task_id='purge_fact_game',
        conn_id='rds_connection',
        sql=f"DELETE FROM gold.fact_game WHERE collected_at < NOW() - INTERVAL '{RETENTION_1_MONTH} days';"
    )

    # --- NHÓM 2: XÓA DỮ LIỆU 3 THÁNG ---
    purge_fact_user = SQLExecuteQueryOperator(
        task_id='purge_fact_user',
        conn_id='rds_connection',
        sql=f"DELETE FROM gold.fact_user WHERE collected_at < NOW() - INTERVAL '{RETENTION_3_MONTHS} days';"
    )

    purge_fact_elo = SQLExecuteQueryOperator(
        task_id='purge_fact_elo',
        conn_id='rds_connection',
        sql=f"DELETE FROM gold.fact_elo WHERE collected_at < NOW() - INTERVAL '{RETENTION_3_MONTHS} days';"
    )

    purge_dim_user = SQLExecuteQueryOperator(
        task_id='purge_dim_user',
        conn_id='rds_connection',
        sql=f"DELETE FROM gold.dim_user WHERE collected_at < NOW() - INTERVAL '{RETENTION_3_MONTHS} days';"
    )

    # --- NHÓM 3: TỐI ƯU HÓA Ổ ĐĨA VẬT LÝ ---
    vacuum_db = SQLExecuteQueryOperator(
        task_id='vacuum_analyze_all_tables',
        conn_id='rds_connection',
        autocommit=True, # Vẫn BẮT BUỘC phải giữ cái này
        sql=[            # ĐỔI THÀNH DẠNG LIST (DANH SÁCH) Ở ĐÂY
            "VACUUM ANALYZE gold.game_evaluations;",
            "VACUUM ANALYZE gold.fact_game;",
            "VACUUM ANALYZE gold.fact_user;",
            "VACUUM ANALYZE gold.fact_elo;",
            "VACUUM ANALYZE gold.dim_user;"
        ]
    )

    [
        purge_evaluations, 
        purge_fact_game, 
        purge_fact_user, 
        purge_fact_elo, 
        purge_dim_user
    ] >> vacuum_db