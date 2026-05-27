import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv(
    r'C:\Users\nikhi\OneDrive\Desktop\target-data-engineering\data\target_data.csv'
)

for index, row in df.iterrows():
    producer.send('target-orders', row.to_dict())
    print(f"Sent row {index}")
    time.sleep(1)

producer.flush()