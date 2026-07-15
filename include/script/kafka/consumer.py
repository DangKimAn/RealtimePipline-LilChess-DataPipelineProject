from confluent_kafka import Consumer, KafkaError

# 1. Cấu hình Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-python-group',      # Các consumer cùng group sẽ chia sẻ việc đọc dữ liệu
    'auto.offset.reset': 'earliest'     # Đọc từ đầu nếu chưa có offset (hoặc dùng 'latest' để đọc cái mới nhất)
}

# 2. Khởi tạo Consumer
consumer = Consumer(conf)

# 3. Đăng ký nhận tin nhắn từ topic
consumer.subscribe(['bullet'])

print("Đang lắng nghe tin nhắn...")

try:
    # Vòng lặp vô hạn để liên tục đọc dữ liệu
    while True:
        # Chờ tối đa 1 giây để có tin nhắn mới
        msg = consumer.poll(timeout=1.0)

        if msg is None:
            continue
            
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # Đã đọc đến cuối partition
                continue
            else:
                print(f"Lỗi Consumer: {msg.error()}")
                break
                
        # 4. Xử lý tin nhắn nhận được
        print(f"Nhận được: Key={msg.key().decode('utf-8')} | Value={msg.value().decode('utf-8')}")

except KeyboardInterrupt:
    print("Dừng Consumer...")
finally:
    # 5. Luôn nhớ đóng Consumer để giải phóng tài nguyên và commit offset
    consumer.close()