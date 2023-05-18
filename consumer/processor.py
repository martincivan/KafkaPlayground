import json
import logging

from qu.la_internal import Configuration, ApiClient, EventsApi, HandlerPayload

from configuration import HandlerParams


class MessageProcessor:

    def __init__(self, la_key: str, la_url: str):
        self.la_url = la_url
        self.la_key = la_key

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
                raise ValueError("Url is empty")
        except (KeyError, ValueError):
            logging.error("Message %s has no account", msg)
            return True

        return await self._send(account, handler_params.id, decoded)

    async def _send(self, account, id, value):
        url = self.la_url.replace("{account}", account)
        config = Configuration(host=url, api_key=self._generate_jwt())
        async with ApiClient(config) as api_client:
            api_instance = EventsApi(api_client)
            payload = HandlerPayload()
            payload.payload = value
            payload.id = id
            result = await api_instance.execute_handler(handler_payload=payload)
            return result

    def _generate_jwt(self):
        return self.la_key
