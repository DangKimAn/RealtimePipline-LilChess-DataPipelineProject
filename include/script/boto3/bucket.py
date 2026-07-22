import os 
import boto3
from dotenv import load_dotenv

import json

script_dir = os.path.dirname(os.path.abspath(__file__))
env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')

load_dotenv(env_path_file)

bucket_name = os.getenv('bucket_name')

AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY') 
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
print(AWS_ACCESS_KEY[:5])
print(AWS_SECRET_KEY[:5])
region = os.getenv('region')
print(region)
print(bucket_name)


def upload_data_to_s3( data, bucket_name, object_name):
    
    try:
        # put_object nhận tham số Body là bytes hoặc string

        s3 = boto3.client(
   's3',
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY,
    region_name=region
        )
        s3.put_object(
            Bucket=bucket_name, 
            Key=object_name, 
            Body=data,
            ContentType='application/json' # Tuỳ chọn loại file
        )
        print(f"Đã đẩy dữ liệu lên {bucket_name}/{object_name}")
    except Exception as e:
        print(f"Lỗi: {e}")

upload_data_to_s3('{"key_test" : "value_test"} ' , bucket_name , 'test.json')