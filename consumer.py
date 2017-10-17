from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(value_deserializer=lambda m: json.loads(m))
consumer.subscribe(['trump']) # haha

for msg in consumer:
    if 'user' in msg.value.keys():
        print(msg.value['user']['screen_name'])
