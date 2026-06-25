from confluent_kafka import Producer

import random
from datetime import datetime
import json

def delivery_callback(err, msg):
    if err:
        print('Delivery Error')

if __name__ == '__main__':
    
    config = {
        'bootstrap.servers': 'localhost:19092'
    }

    producer = Producer(config)

    try:
        while True:
            key = input('Input Key(A, B, C): ')
            value = {
                'value': random.random(),
                'timestamp': int(datetime.now().timestamp())
            }

            producer.produce(
                topic = 'stateful_processing',
                key = key.encode(),
                value = json.dumps(value).encode(),
                on_delivery = delivery_callback
            )

            print(f'{key}, {value}')

            producer.poll(0)

    except KeyboardInterrupt:
        producer.flush()

        print('Kafka Off')

    except Exception as e:
        print(f'Error. {e}')
