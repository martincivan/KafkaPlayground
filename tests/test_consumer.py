import asyncio
import dataclasses
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from aiokafka import ConsumerStoppedError, AIOKafkaConsumer
from kafka import TopicPartition

from consumer.configuration import HandlerParams, ConfigurationParams, Configuration
from consumer.processor import MessageProcessor
from consumer.runner import Consumer, Runner, BackOffHandler


def async_return(result):
    f = asyncio.Future()
    f.set_result(result)
    return f


class ConsumerTest(IsolatedAsyncioTestCase):
    async def test_consumer_happy(self):
        backoff_handler, handler_params, instance, message_mock, processor, kafka_mock = await self.create_consumer(
            True)
        await instance.run()
        processor.handle.assert_called()
        processor.handle.assert_called_with(message_mock, handler_params)
        backoff_handler.handle.assert_not_called()
        backoff_handler.reset.assert_called()
        backoff_handler.reset.assert_awaited()
        self.assertIsNone(kafka_mock.sought)
        self.assertTrue(kafka_mock.stopped)

    async def test_consumer_unhappy(self):
        backoff_handler, handler_params, instance, message_mock, processor, kafka_mock = await self.create_consumer(
            False)
        await instance.run()
        processor.handle.assert_called()
        processor.handle.assert_called_with(message_mock, handler_params)
        backoff_handler.reset.assert_not_called()
        backoff_handler.handle.assert_called()
        backoff_handler.handle.assert_awaited()
        self.assertEqual(kafka_mock.sought, (TopicPartition(topic='test', partition=0), 0))
        self.assertTrue(kafka_mock.stopped)

    async def test_runner(self):
        configuration = AsyncMock(Configuration())
        config = ConfigurationParams("kafka-server", "la-key-id", "la-url", "la-key", 10)
        configuration.load_configuration = AsyncMock(return_value=config)
        handler = HandlerParams("test_id", {"test-topic"}, {"test-type"})
        configuration.load_handlers = AsyncMock(return_value=[handler])
        consumer = AsyncMock(
            Consumer(handler_params=handler, configuration=config, message_processor=MessageProcessor(handler, config),
                     kafka_provider=lambda topics, server, id: KafkaMock()))
        consumer.run = AsyncMock()
        consumer.stop = AsyncMock()

        runner = Runner(configuration=configuration,
                        consumer_factory=lambda handler_params, configuration_params: consumer)
        await runner.run()
        consumer.run.assert_called()
        consumer.run.assert_awaited()
        consumer.stop.assert_called()
        consumer.stop.assert_awaited()

    async def create_consumer(self, handle):
        handler_params = HandlerParams("test_id", {"test-topic"}, {"test-type"})
        configuration = ConfigurationParams("kafka-server", "la-key-id", "la-url", "la-key", 10)
        processor = AsyncMock(MessageProcessor(handler_params, configuration))
        processor.handle = AsyncMock(return_value=handle)
        message_mock = MessageMock("test", 0, 0, "test")
        backoff_handler = AsyncMock(BackOffHandler(0))
        backoff_handler.handle = AsyncMock()
        backoff_handler.reset = AsyncMock()
        kafka_mock = KafkaMock(message_mock)
        instance = Consumer(handler_params=handler_params, configuration=configuration, message_processor=processor,
                            kafka_provider=lambda topics, server, id: kafka_mock,
                            backoff_handler=backoff_handler)
        return backoff_handler, handler_params, instance, message_mock, processor, kafka_mock


@dataclasses.dataclass
class MessageMock:
    topic: str
    partition: int
    offset: int
    value: str


class KafkaMock(AIOKafkaConsumer):

    def __init__(self, *args):
        self.data = list(args)
        self.stopped = False
        self.started = False
        self.sought = None

    async def start(self):
        self.started = True

    async def stop(self):
        self.stopped = True

    async def getone(self):
        try:
            return self.data.pop(0)
        except IndexError:
            raise ConsumerStoppedError()

    def seek(self, *args, **kwargs):
        self.sought = args
