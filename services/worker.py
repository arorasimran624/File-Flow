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
    async with message.process():
        msg = json.loads(message.body.decode())
        file_id = msg.get("file_id")
        file_content = msg.get("file_content")
        print(f"[File Worker] Processing file: {file_id}")
        result = await process_file(file_id, file_content)
        print(f"[File Worker] Result for {file_id}: {result}")

        # Push result to Notification queue
        connection = await aio_pika.connect_robust(RABBITMQ_URL)
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(NOTIFICATION_QUEUE, durable=True)
            await channel.default_exchange.publish(
                aio_pika.Message(body=json.dumps(result).encode()),
                routing_key=queue.name
            )
        print(f"[File Worker] Result sent to Notification queue for {file_id}")


async def file_worker():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    channel = await connection.channel()
    queue = await channel.declare_queue(FILE_QUEUE, durable=True)
    print(f"[File Worker] Waiting for messages in {FILE_QUEUE}...")
    await queue.consume(handle_file_message)

    while True:
        await asyncio.sleep(1)

async def notification_worker():
    connection = await aio_pika.connect_robust(RABBITMQ_URL)
    async with connection:
        channel = await connection.channel()
        queue = await channel.declare_queue(NOTIFICATION_QUEUE, durable=True)
        print(f"[Notification Worker] Waiting for messages in {NOTIFICATION_QUEUE}...")

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
                    print(f"[Notification Worker] {result}")

async def main():
    await database.connect()
    print("[DB] Connected")

    try:
        await asyncio.gather(
            file_worker(),
            notification_worker()
        )
    finally:
        await database.disconnect()
        print("[DB] Disconnected")


if __name__ == "__main__":
    print("Starting both workers...")
    asyncio.run(main())