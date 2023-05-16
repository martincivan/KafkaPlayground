import os
from dataclasses import dataclass
from qu.la_internal import ApiClient, EventsApi
from qu.la_internal import Configuration as ClientConfiguration

@dataclass
class HandlerParams:
    id: str
    topics: set


@dataclass
class ConfigurationParams:
    kafka_server: str

    def __post_init__(self):
        if not self.kafka_server:
            raise Exception("Kafka server cannot be empty")


class Configuration:
    async def load_handlers(self):
        liveagent_url = os.environ.get("LA_URL")
        if liveagent_url:
            return await self._load_handlers_from_liveagent(url=liveagent_url)

    async def load_configuration(self):
        kafka_server = os.environ.get("KAFKA_SERVER")
        if not kafka_server:
            raise Exception("KAFKA_SERVER env variable is mandatory")

        return ConfigurationParams(kafka_server=kafka_server)

    async def _load_handlers_from_liveagent(self, url):
        configuration = ClientConfiguration(host=url)
        async with ApiClient(configuration) as client:
            api = EventsApi(client)
            consumers = await api.get_event_consumers()
        return [HandlerParams(id=consumer.id, topics=set([event.topic for event in consumer.events])) for consumer in consumers]