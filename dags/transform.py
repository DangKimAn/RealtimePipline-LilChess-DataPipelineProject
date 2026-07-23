from datetime import datetime, timedelta
from airflow.decorators import dag, task
from include.script.spark.consumer_games import transform_game_schema
from include.script.spark.consumer_users import transform_user_schema
from include.script.spark.consumer_elos import transform_elo_schema
import os 
from dotenv import load_dotenv
@dag(
    start_date=datetime(2026, 1, 1),
    schedule="10 * * * *",
    is_paused_upon_creation=False, 
    catchup=False,
    tags=["transform"],
)

def tranforms():
    
    @task(trigger_rule="all_done")
    def games():

        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
        
        # Load biến môi trường
        load_dotenv(env_path_file)

        # 2. Lấy thông tin DB ngay trong lúc task thực thi
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv('DB_PORT')
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        bucket_name = os.getenv('bucket_name')

        get_time = datetime.now() - timedelta(hours=1)
        print(f'year ={get_time.year}')
        print(f'month ={get_time.month}')
        print(f'day ={get_time.day}')
        print(f'hour ={get_time.hour}')
        s3_path_dir = f'topics/game/year={get_time.year}/month={get_time.month:02d}/day={get_time.day:02d}/hour={get_time.hour:02d}/'
        s3_path = f's3a://{bucket_name}/{s3_path_dir}'
        rs = transform_game_schema(s3_path=s3_path , DB_HOST=DB_HOST , DB_NAME=DB_NAME , 
                              DB_PORT=DB_PORT, DB_USER=DB_USER , DB_PASSWORD=DB_PASSWORD)
        if rs:
            print('Run task games success !!! ')
    @task(trigger_rule="all_done")
    def users():

        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
        
        # Load biến môi trường
        load_dotenv(env_path_file)

        # 2. Lấy thông tin DB ngay trong lúc task thực thi
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv('DB_PORT')
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        bucket_name = os.getenv('bucket_name')

        get_time = datetime.now() - timedelta(hours=1)
        print(f'year ={get_time.year}')
        print(f'month ={get_time.month}')
        print(f'day ={get_time.day}')
        print(f'hour ={get_time.hour}')

        s3_path_dir = f'topics/user/year={get_time.year}/month={get_time.month:02d}/day={get_time.day:02d}/hour={get_time.hour:02d}/'
        s3_path = f's3a://{bucket_name}/{s3_path_dir}'
        rs = transform_user_schema(s3_path=s3_path , DB_HOST=DB_HOST , DB_NAME=DB_NAME , 
                                DB_PORT=DB_PORT, DB_USER=DB_USER , DB_PASSWORD=DB_PASSWORD)
        if rs:
            print('Run task games success !!! ')
    @task(trigger_rule="all_done")
    def elos():

        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')
        
        # Load biến môi trường
        load_dotenv(env_path_file)

        # 2. Lấy thông tin DB ngay trong lúc task thực thi
        DB_HOST = os.getenv("DB_HOST")
        DB_PORT = os.getenv('DB_PORT')
        DB_NAME = os.getenv("DB_NAME")
        DB_USER = os.getenv("DB_USER")
        DB_PASSWORD = os.getenv("DB_PASSWORD")
        bucket_name = os.getenv('bucket_name')

        get_time = datetime.now() - timedelta(hours=1)
        print(f'year ={get_time.year}')
        print(f'month ={get_time.month}')
        print(f'day ={get_time.day}')
        print(f'hour ={get_time.hour}')
        s3_path_dir = f'topics/elo/year={get_time.year}/month={get_time.month:02d}/day={get_time.day:02d}/hour={get_time.hour:02d}/'
        s3_path = f's3a://{bucket_name}/{s3_path_dir}'
        rs = transform_elo_schema(s3_path=s3_path , DB_HOST=DB_HOST , DB_NAME=DB_NAME , 
                                DB_PORT=DB_PORT, DB_USER=DB_USER , DB_PASSWORD=DB_PASSWORD)
        if rs:
            print('Run task games success !!! ')
        
    games()>>  users() >> elos()

# Instantiate the DAG
tranforms()
