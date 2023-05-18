import asyncio
import os

from aiokafka import AIOKafkaConsumer
from qu.la_internal import Configuration, ApiClient, EventsApi, HandlerPayload, Consumer


async def load_configuration(configuration):
    async with ApiClient(configuration) as api_client:
        api_instance = EventsApi(api_client)
        return await api_instance.get_event_consumers()


async def consume(consumer: Consumer):

    configuration = Configuration(
        host = "https://hosted.la.localhost/LiveAgent/server/public"
    )
    topics = set([event.topic for event in consumer.events])
    print(consumer.id + "subscribing to " + str(topics))
    kafka = AIOKafkaConsumer(*topics, bootstrap_servers='localhost:9092', group_id=consumer.id)
    try:
        await kafka.start()
        async with ApiClient(configuration) as api_client:
            api_instance = EventsApi(api_client)
            async for msg in kafka:
                print("Process %s consumed msg from partition %s" % (
                    os.getpid(), msg.partition))
                payload = HandlerPayload()
                value = msg.value.decode(encoding="UTF8")
                print(value)
                payload.payload = value
                payload.event = "event"
                payload.id = consumer.id
                await api_instance.execute_handler(handler_payload=payload)
    finally:
        await kafka.stop()




async def main():
    configuration = Configuration(
        host = "https://hosted.la.localhost/LiveAgent/server/public"
    )
    result = await load_configuration(configuration)
    tasks = set()
    for conf in result:
        tasks.add(consume(conf))
    await asyncio.gather(*tasks)
    pass

if __name__ == "__main__":
    asyncio.run(main())