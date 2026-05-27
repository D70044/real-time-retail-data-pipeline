from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'target-orders',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='target-json-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

output_file = r"C:\Users\nikhi\OneDrive\Desktop\target-data-engineering\output\kafka_orders.json"

print("Consumer Started... Saving data to file")

with open(output_file, "a", encoding="utf-8") as file:
    for message in consumer:
        file.write(json.dumps(message.value) + "\n")
        file.flush()
        print("Saved:", message.value)
    
    
