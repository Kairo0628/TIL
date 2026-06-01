from confluent_kafka import Producer
import json
import random
import time

def get_producer():
    config = {
        'bootstrap.servers': 'localhost:19092'
    }

    producer = Producer(config)

    return producer

def delivery_callback(err, msg):
    if err:
        print(f'Delivery Failed: {err}')
    else:
        print(f'Delivery Succeed. Topic: {msg.topic()}, Key: {msg.key()}, Value: {msg.value()}')


def create_topic(producer):
    topic = 'structured_streaming_tutorial'

    for _ in range(100):
        key = random.randint(1, 30)
        value = {
            'val1': random.random(),
            'val2': random.choice(['a', 'b', 'c', 'd', 'e'])
        }

        producer.produce(
            topic = topic,
            key = str(key).encode('utf-8'),
            value = json.dumps(value).encode('utf-8'),
            on_delivery = delivery_callback
        )

        producer.poll(0)

        time.sleep(0.5)

    producer.flush()

    print('Produce Complete')

if __name__ == '__main__':
    producer = get_producer()
    create_topic(producer)
