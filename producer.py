from kafka import KafkaProducer
import json, time, random, datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    #string to bytes using utf-8
)

users = [101, 102, 103]

while True:
    data = {
        "user_id": random.choice(users),
        "steps": random.randint(1000, 15000),
        "timestamp": datetime.datetime.now().isoformat(),
        "heart_rate": random.randint(60, 180)
    }
    producer.send('fitness_events', value=data)
    print(f"Sent data: {data}")
    time.sleep(1)
    