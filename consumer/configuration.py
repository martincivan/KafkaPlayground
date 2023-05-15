class HandlerParams:
    def __init__(self):
        self.id = None
        self.topics = []


class ConfigurationParams:
    def __init__(self):
        self.kafka_server = None


class Configuration:
    async def load_handlers(self):
        yield HandlerParams()

    async def load_configuration(self):
        yield ConfigurationParams()
