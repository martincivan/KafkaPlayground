import json
from unittest import IsolatedAsyncioTestCase
from unittest.mock import AsyncMock

from consumer.auth import JWTGenerator
from consumer.configuration import HandlerParams, ConfigurationParams
from consumer.processor import MessageProcessor
from consumer.sender import Sender


class TokenGenerator(JWTGenerator):

    def __init__(self, value: str = "token"):
        self.value = value

    def generate(self, account_id: str) -> str:
        return self.value


class MessageMock:

    def __init__(self, account: str, type: str, data: str) -> None:
        self.data = data
        self.type = type
        self.account = account

    @property
    def value(self):
        values = {
            "data": self.data,
            "type": self.type,
            "account": self.account
        }
        return json.dumps(values).encode("UTF8")


class ProcessorTest(IsolatedAsyncioTestCase):

    async def test_processor(self):
        params = HandlerParams("id1", {"topic"}, {"type"})
        config = ConfigurationParams("kafka_server", "key", "url", "key_id")

        sender = AsyncMock(Sender(30))
        sender.send = AsyncMock(return_value=True)
        processor = MessageProcessor(handler=params, config=config, token_generator=TokenGenerator(), sender=sender)
        message = MessageMock("account1", "type", "some_data...")
        self.assertTrue(await processor.handle(message, params))
        sender.send.assert_called_with(url="url", token="token", data=message.value.decode("UTF8"), id="id1")
