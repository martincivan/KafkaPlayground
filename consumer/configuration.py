import os
from dataclasses import dataclass
from typing import Iterable, Optional

from qu.la_internal import ApiClient, EventsApi, Consumer
from qu.la_internal import Configuration as ClientConfiguration


@dataclass
class HandlerParams:
    id: str
    topics: set
    types: set


@dataclass
class ConfigurationParams:
    kafka_server: str
    la_key: str
    la_url: str
    la_key_id: str
    timeout: int = 300

    def __post_init__(self):
        if not self.kafka_server:
            raise Exception("Kafka server cannot be empty")
        if not self.la_url:
            raise Exception("LiveAgent server URL cannot be empty")
        if not self.la_key_id:
            raise Exception("LiveAgent key ID cannot be empty")


class PathHandlersLoader:
    def __init__(self, path: str):
        self.path = path

    async def load(self) -> Optional[list[HandlerParams]]:
        configs = self._get_configs()
        if configs is None:
            return None
        result = {}
        serializer = ApiClient()
        for config in configs:
            with open(config, "r") as f:
                Object = lambda **kwargs: type("Object", (), kwargs)
                data: list[Consumer] = serializer.deserialize(Object(data=f.read()), "List[Consumer]")
                for consumer in data:
                    topics = {event.topic for event in consumer.events}
                    types = {event.id for event in consumer.events}
                    if consumer.id in result:
                        result[consumer.id].topics.update(topics)
                        result[consumer.id].types.update(types)
                    else:
                        result[consumer.id] = HandlerParams(consumer.id, topics, types)
        await serializer.close()
        return list(result.values())

    def _get_configs(self) -> Optional[Iterable[str]]:
        if not os.path.exists(self.path):
            return None
        if os.path.isfile(self.path):
            return [self.path]
        if not os.path.isdir(self.path):
            return []
        result = []
        for filename in os.scandir(self.path):
            if filename.is_file():
                result.append(filename.path)
        return result


class Configuration:

    async def load_handlers(self) -> list[HandlerParams]:
        loader = None
        liveagent_url = os.environ.get("LA_CONFIG_URL")
        if liveagent_url:
            loader = LiveAgentHandlersLoader(liveagent_url)
        config_path = os.environ.get("LA_CONFIG_PATH")
        if config_path:
            loader = PathHandlersLoader(config_path)
        if not loader:
            raise Exception("No LA_CONFIG_URL or MARIADB_CONFIG_URL env variable configured")
        return await loader.load()

    async def load_configuration(self) -> ConfigurationParams:
        kafka_server = os.getenv("KAFKA_SERVER")
        if not kafka_server:
            raise Exception("KAFKA_SERVER env variable is mandatory")
        la_key_path = os.getenv("LA_KEY")
        if not la_key_path:
            raise Exception("LA_KEY env variable is mandatory")
        la_url = os.getenv("LA_URL")
        if not la_url:
            raise Exception("LA_URL env variable is mandatory")
        la_key_id = os.getenv("LA_KEY_ID")
        if not la_key_id:
            raise Exception("LA_KEY_ID env variable is mandatory")

        with open(la_key_path, "r") as f:
            la_key = f.read()

        return ConfigurationParams(kafka_server=kafka_server, la_key=la_key, la_url=la_url, la_key_id=la_key_id,
                                   timeout=int(os.getenv("TIMEOUT", 300)))


class LiveAgentHandlersLoader:

    def __init__(self, url: str) -> None:
        super().__init__()
        self.url = url

    async def load(self) -> list[HandlerParams]:
        configuration = ClientConfiguration(host=self.url)
        async with ApiClient(configuration) as client:
            api = EventsApi(client)
            consumers = await api.get_event_consumers()
            handlers = []
            for consumer in consumers:
                topics = set()
                types = set()
                for event in consumer.events:
                    topics.add(event.topic)
                    types.add(event.id)
                handlers.append(HandlerParams(id=consumer.id, topics=topics, types=types))
            return handlers
