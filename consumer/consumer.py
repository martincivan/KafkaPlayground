import asyncio
import logging

from aiokafka import AIOKafkaConsumer, ConsumerStoppedError
from kafka import TopicPartition

from configuration import Configuration, HandlerParams, ConfigurationParams
from processor import MessageProcessor


class BackOffHandler:

    def __init__(self, retry_timeout: int, max_backoff: int = 5):
        self.max_backoff = max_backoff
        self.retry_timeout = retry_timeout
        self.retries = 0

    async def handle(self):
        self.retries = min(self.retries + 1, self.max_backoff)
        await asyncio.sleep(self.retries * self.retry_timeout)

    def reset(self):
        self.retries = 0


class Consumer:

    def __init__(self, handler_params: HandlerParams, configuration: ConfigurationParams,
                 message_processor: MessageProcessor, backoff_handler: BackOffHandler = None):
        self.backoff_handler = backoff_handler or BackOffHandler(0)
        self.message_processor = message_processor
        self.kafka_consumer = AIOKafkaConsumer(*handler_params.topics, bootstrap_servers=configuration.kafka_server,
                                               group_id=handler_params.id)
        self.handler_params = handler_params
        self._stopped = False

    async def run(self):
        try:
            await self.kafka_consumer.start()
            while not self._stopped:
                msg = await self.kafka_consumer.getone()
                if await self._handle(msg):
                    self.backoff_handler.reset()
                else:
                    logging.error("Error while processing message %s", msg)
                    self.kafka_consumer.seek(TopicPartition(msg.topic, msg.partition), msg.offset)
                    await self.backoff_handler.handle()
        except ConsumerStoppedError as error:
            if not self._stopped:
                raise error
        finally:
            if not self._stopped:
                try:
                    await self.kafka_consumer.stop()
                except ConsumerStoppedError:
                    pass

    async def stop(self):
        self._stopped = True
        await self.kafka_consumer.stop()

    async def _handle(self, msg):
        try:
            return await self.message_processor.handle(msg, self.handler_params)
        except Exception as e:
            logging.error("Error while processing message %s : %s", msg, e)
            return False


class Runner:

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
            runner = Consumer(handler, configuration_params, MessageProcessor(handler, configuration_params),
                              BackOffHandler(configuration_params.timeout, 5))
            self.runners.add(runner)
            tasks.add(runner.run())
        await asyncio.gather(*tasks)

    async def stop(self, *_):
        logging.info("Stopping consumers")
        await asyncio.gather(*[runner.stop() for runner in self.runners])
        logging.info("Consumers stopped")
