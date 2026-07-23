from time import time
from confluent_kafka import Producer
import time

from dotenv import load_dotenv
import os




def delivery_callback(err, msg , key):
    if err:
        print(f'Gửi tin nhắn thất bại: {err}')
    else:
        # print(f'Đã gửi thành công tới topic {msg.topic()} :{key} [Partition: {msg.partition()}]')
        pass


class MyProducer():
    def __init__(self, topic_name):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        env_path_file = os.path.join('/'.join(script_dir.split('/')[:-3]), '.env')

        load_dotenv(env_path_file)

        KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
        API_KEY = os.getenv('API_KEY')
        API_SECRET = os.getenv('API_SECRET')

        
        self.conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': API_KEY,
            'sasl.password': API_SECRET ,
            'client.id': 'kafka-producer-1'
        }
        self.producer = Producer(self.conf)
        self.topic_name = topic_name

    def send_message(self , key , value):
        self.producer.produce(topic=self.topic_name, key=key, value=value , callback = lambda err , msg: delivery_callback(err , msg , key))
        self.producer.flush()

    def send_message_to_specific_topic(self , topic , key , value):
        self.producer.produce(topic=topic, key=key, value=value , callback = lambda err , msg: delivery_callback(err , msg , key))
        self.producer.flush()

    
    
