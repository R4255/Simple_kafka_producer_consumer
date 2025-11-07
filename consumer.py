from kafka import KafkaConsumer
import boto3, json
from datetime import datetime
import uuid
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

bucket_name = 'fitness-data'

consumer = KafkaConsumer(
    'fitness_events',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='fitness-consumers'
)

print("Starting consumer...")

for msg in consumer:
    event = msg.value
    print(f"Processing message: {msg}")
    print(f"Received event: {event}")
    key = f"user_{event['user_id']}/{datetime.utcnow().isoformat()}_{uuid.uuid4().hex}.json"
    s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(event))
    print(f"📦 Stored event for user {event['user_id']} → {key}")
