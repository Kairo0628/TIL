from confluent_kafka import Consumer

def get_consumer():
    config = {
        'bootstrap.servers': 'localhost:19092',
        'group.id': 'tutorial_consumer',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(config)

    return consumer

def consume_topic(consumer):
    topic = 'tutorial_topic'
    consumer.subscribe(
        topics = [topic]
    )

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                print('Waiting ...')
            elif msg.error():
                print('Error')
            else:
                print(f'Comsume event. Topic: {msg.topic()}, Value: {msg.value().decode('utf-8')}, Key: {msg.key().decode('utf-8')}')
    except KeyboardInterrupt:
        print('Comsumer Shutdown')
    finally:
        consumer.close()

if __name__ == '__main__':
    consumer = get_consumer()
    consume_topic(consumer)
