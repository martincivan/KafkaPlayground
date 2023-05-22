import asyncio
import dataclasses
import unittest
from unittest.async_case import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from aiokafka import ConsumerStoppedError

from consumer.configuration import HandlerParams, ConfigurationParams
from consumer.processor import MessageProcessor
from consumer.runner import Consumer


class ConsumerTest(IsolatedAsyncioTestCase):
    async def test_something(self):
        handler_params = HandlerParams("test_id", {"test-topic"}, {"test-type"})
        configuration = ConfigurationParams("kafka-server", "la-key-id", "la-url", "la-key", 10)
        processor = AsyncMock(MessageProcessor(handler_params, configuration))
        instance = Consumer(handler_params=handler_params, configuration=configuration, message_processor=processor)
        await instance.kafka_consumer.stop()
        message_mock = MessageMock("test", 0, 0, "test")
        instance.kafka_consumer = KafkaMock(message_mock)
        # try:
        await instance.run()
        # except IndexError:
        #     pass
        processor.handle.assert_called()
        processor.handle.assert_called_with(message_mock, handler_params)


@dataclasses.dataclass
class MessageMock:
    topic: str
    partition: int
    offset: int
    value: str


class KafkaMock:

    def __init__(self, *args):
        self.data = list(args)

    async def start(self):
        pass

    async def stop(self):
        pass

    async def getone(self):
        try:
            return self.data.pop(0)
        except IndexError:
            raise ConsumerStoppedError()

    def seek(self, *args, **kwargs):
        pass


if __name__ == "__main__":
    asyncio.run(unittest.main())
