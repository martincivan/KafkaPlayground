import asyncio
import logging
import signal

from aiokafka import AIOKafkaConsumer

from configuration import Configuration
from consumer.processor import MessageProcessor
from consumer.runner import Runner, Consumer, BackOffHandler

logging.basicConfig(level=logging.INFO)


def create_consumer(handler_params, configuration_params):
    def kafka_provider(topics, server, id):
        return AIOKafkaConsumer(*topics, bootstrap_servers=server, group_id=id)

    return Consumer(handler_params=handler_params,
                    configuration=configuration_params,
                    message_processor=MessageProcessor(
                        handler=handler_params,
                        config=configuration_params),
                    backoff_handler=BackOffHandler(
                        configuration_params.timeout, 0),
                    kafka_provider=kafka_provider)


async def main():
    runner = Runner(configuration=Configuration(),
                    consumer_factory=create_consumer)
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                                lambda: asyncio.ensure_future(runner.stop()))
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
