import asyncio
import aio_pika
import json
import os
from dotenv import load_dotenv
from services.file_processing import process_file
from services.teams_services import send_teams_message

from services.db_services import database  

RABBITMQ_URL = os.getenv("RABBITMQ_URL")
FILE_QUEUE = os.getenv("QUEUE_FIRST")    
NOTIFICATION_QUEUE = os.getenv("QUEUE_SECOND")      

async def handle_file_message(message: aio_pika.IncomingMessage):
    """Process a single file message from the file queue and send the result to the notification queue."""
    async with message.process():
        msg = json.loads(message.body.decode())
        file_id = msg.get("file_id")
        file_content = msg.get("file_content")
        result = await process_file(file_id, file_content)

        # Push result to Notification queue
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(NOTIFICATION_QUEUE, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(result).encode()),
                routing_key=queue.name
            )

async def file_worker():
    """Continuously consume file messages from the file queue."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue(FILE_QUEUE, durable=True)
    await queue.consume(handle_file_message)

    while True:
        await asyncio.sleep(1)

async def notification_worker():
    """Continuously consume notification messages and send alerts to Teams."""
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(NOTIFICATION_QUEUE, durable=True)

        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    body = json.loads(message.body.decode())
                    file_id = body.get("file_id", "Unknown")
                    status = body.get("status", "Unknown")
                    errors = {}
                    for stage in ["template", "null_check", "data_type_check"]:
                        if stage in body.get("errors", {}):
                            errors[stage] = body["errors"][stage]

                    result = send_teams_message(file_id, status, errors)

async def main():
    """Connect to the database and start file and notification workers concurrently."""
    await database.connect()
    try:
        await asyncio.gather(
            file_worker(),
            notification_worker()
        )
    finally:
        await database.disconnect()


if __name__ == "__main__":
    """Start the asyncio event loop and run the main worker function."""
    asyncio.run(main())