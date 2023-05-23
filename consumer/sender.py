from qu.la_internal import Configuration, ApiClient, EventsApi, HandlerPayload


class Sender:

    def __init__(self, timeout: int):
        self.timeout = timeout

    async def send(self, url: str, token: str, data: str, id: str) -> bool:
        config = Configuration(host=url)
        config.access_token = token
        with ApiClient(config) as client:
            api_instance = EventsApi(client)
            payload = HandlerPayload()
            payload.payload = data
            payload.id = id
            result = await api_instance.execute_handler_with_http_info(handler_payload=payload,
                                                                       _request_timeout=self.timeout)
            status = result.status_code
            return 200 <= status < 300
