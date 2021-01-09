import asyncio

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient, NewTopic

N_MESSAGE = 5
GROUP_ID = '0'

async def consume(topic_name):
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9092',
        'group.id': '123456',
        'auto.offset.reset': 'smallest'
    })
    
    consumer.subscribe([topic_name])
    
    while True:
        message = consumer.poll(1.0)
        
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                print(message.value())
            except KeyError as e:
                print(f"Failed to unpack message {e}")
        await asyncio.sleep(1.0)

                
def run_consumer():
    try:
        asyncio.run(consume('police-service-calls'))
        
    except KeyboardInterrupt as e:
        print("Shutting down...")
        
if __name__ == '__main__':
    run_consumer()