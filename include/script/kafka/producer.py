from time import time
from confluent_kafka import Producer
import time

def delivery_callback(err, msg , key):
    if err:
        print(f'Gửi tin nhắn thất bại: {err}')
    else:
        # print(f'Đã gửi thành công tới topic {msg.topic()} :{key} [Partition: {msg.partition()}]')
        pass


class MyProducer():
    def __init__(self, topic_name):
        # self.conf = { 'bootstrap.servers': 'localhost:9092' }
        self.conf = {
            'bootstrap.servers': 'localhost:9094', # Sửa đúng cổng này
            'client.id': 'chess-producer-1'
        }
        self.producer = Producer(self.conf)
        self.topic_name = topic_name

    def send_message(self , key , value):
        self.producer.produce(topic=self.topic_name, key=key, value=value , callback = lambda err , msg: delivery_callback(err , msg , key))
        self.producer.flush()

    def send_message_to_specific_topic(self , topic , key , value):
        self.producer.produce(topic=topic, key=key, value=value , callback = lambda err , msg: delivery_callback(err , msg , key))
        self.producer.flush()

    
    
