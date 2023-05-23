import json
import logging

from consumer.auth import JWTGenerator
from consumer.configuration import HandlerParams, ConfigurationParams
from consumer.sender import Sender


class MessageProcessor:

    def __init__(self, handler: HandlerParams, config: ConfigurationParams,
                 token_generator: JWTGenerator,
                 sender: Sender):
        self.sender = sender
        self.token_generator = token_generator
        self.handler = handler
        self.la_key_id = config.la_key_id
        self.la_url = config.la_url
        self.la_key = config.la_key
        self.timeout = config.timeout

    async def handle(self, msg, handler_params: HandlerParams) -> bool:
        logging.info("Processing message %s", msg)
        if not msg.value:
            logging.error("Message %s has no value", msg)
            return True
        decoded = msg.value.decode(encoding="UTF8")
        data = json.loads(decoded)
        try:
            account = data["account"]
            if not account:
                raise Exception("Url is empty")
            type = data["type"]
            if not type:
                raise Exception("Type is empty")
        except (KeyError, Exception) as error:
            logging.error("Message %s is malformed: %s", msg, error)
            return True

        if type not in self.handler.types:
            logging.info("Message %s is not for this handler", msg)
            return True

        return await self._send(account, handler_params.id, decoded)

    async def _send(self, account: str, id: str, value: str) -> bool:
        url = self.la_url.replace("{account}", account)
        return await self.sender.send(url=url, token=self.token_generator.generate(account_id=account), data=value,
                                      id=id)
