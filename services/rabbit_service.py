import os
import aio_pika
from dotenv import load_dotenv
import json


load_dotenv()

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
QUEUE_NAME=os.getenv("QUEUE_NAME")
async def test_rabbitmq():
    """Test RabbitMQ connectivity"""
    try:
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        channel = await connection.channel()
        queue = await channel.declare_queue("test_queue", durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=b"Hello from FastAPI test!"),
            routing_key="test_queue"
        )
        await connection.close()
        return "RabbitMQ connected , test message sent"
    except Exception as e:
        return f"RabbitMQ test failed : {e}"

async def publish_to_queue(message: dict, queue_name: str):
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(queue_name, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode()),
            routing_key=queue.name
        )
    print(f"Message published to queue '{queue_name}'")
