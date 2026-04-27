from confluent_kafka import Producer
import random

def get_producer():
    config = {
        'bootstrap.servers': 'localhost:19092'
    }

    producer = Producer(config)

    return producer

def delivery_callback(err, msg):
    if err:
        print(f'Message failed Delivery: {err}')
    else:
        print(f'Produce event. Topic: {msg.topic()}, Value: {msg.value()}, Key: {msg.key()}')

def create_topic(producer):
    topic = 'tutorial_topic'

    for i in range(100):
        value = str(random.random()).encode('utf-8')
        key = str((i + 1) % 10).encode('utf-8')

        producer.produce(
            topic = topic,
            value = value,
            key = key,
            callback = delivery_callback
        )

        producer.poll(0)

    producer.flush()

    print('Producer Done !')

if __name__ == '__main__':
    
    producer = get_producer()
    create_topic(producer)
