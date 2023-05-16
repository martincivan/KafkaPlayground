import asyncio
import logging

from aiokafka import AIOKafkaConsumer, ConsumerStoppedError

from configuration import Configuration, HandlerParams, ConfigurationParams
from processor import MessageProcessor


class Runner:

    def __init__(self, handler_params: HandlerParams, configuration: ConfigurationParams,
                 message_processor: MessageProcessor):
        self.message_processor = message_processor
        self.kafka_consumer = AIOKafkaConsumer(*handler_params.topics, bootstrap_servers=configuration.kafka_server,
                                               group_id=handler_params.id)
        self.handler_params = handler_params
        self._stopped = False

    async def run(self):
        try:
            await self.kafka_consumer.start()
            async for msg in self.kafka_consumer:
                if self._stopped:
                    break
                if await self._handle(msg):
                    pass  # TODO: rollback -1 message
        finally:
            try:
                await self.kafka_consumer.stop()
            except ConsumerStoppedError:
                pass

    async def stop(self):
        self._stopped = True
        await self.kafka_consumer.stop()

    async def _handle(self, msg):
        try:
            return self.message_processor.handle(msg, self.handler_params)
        except Exception as e:
            logging.error("Error while processing message %s : %s", msg, e)
            return False


class Consumer:

    def __init__(self, configuration: Configuration):
        self.configuration = configuration
        self.runners = set()

    async def run(self):
        logging.info("Loading configuration")
        configuration_params = await self.configuration.load_configuration()
        logging.info("Loading handlers")
        handlers = await self.configuration.load_handlers()
        tasks = set()

        for handler in handlers:
            logging.info("Starting consumer for handler %s, topics: %s", handler.id, ",".join(handler.topics))
            runner = Runner(handler, configuration_params, MessageProcessor())
            self.runners.add(runner)
            tasks.add(runner.run())
        await asyncio.gather(*tasks)

    async def stop(self, *_):
        logging.info("Stopping consumers")
        await asyncio.gather(*[runner.stop() for runner in self.runners])
