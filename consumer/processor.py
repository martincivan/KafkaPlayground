import json
import logging
import time

import jwt
from qu.la_internal import Configuration, ApiClient, EventsApi, HandlerPayload

from configuration import HandlerParams, ConfigurationParams


class MessageProcessor:

    def __init__(self, handler: HandlerParams, config: ConfigurationParams):
        self.handler = handler
        self.la_key_id = config.la_key_id
        self.la_url = config.la_url
        self.la_key = config.la_key
        self.timeout = config.timeout

    async def handle(self, msg, handler_params: HandlerParams):
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

    async def _send(self, account, id, value):
        url = self.la_url.replace("{account}", account)
        config = Configuration(host=url)
        config.access_token = self._generate_jwt(account)
        async with ApiClient(config) as api_client:
            api_instance = EventsApi(api_client)
            payload = HandlerPayload()
            payload.payload = value
            payload.id = id
            result = await api_instance.execute_handler_with_http_info(handler_payload=payload,
                                                                       _request_timeout=self.timeout)
            status = result.status_code
            r = 200 <= status < 300
            return r

    def _generate_jwt(self, account):
        iat = int(time.time())
        token = jwt.encode({
            'iss': 'kafka-consumer.la',
            'aud': account + '.kafka.la',
            'exp': iat + 60,
            'iat': iat
        }, self.la_key, algorithm="RS256", headers={'kid': self.la_key_id})
        return token
