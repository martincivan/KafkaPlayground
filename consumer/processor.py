import logging

from configuration import HandlerParams


class MessageProcessor:
    def handle(self, msg, handler_params: HandlerParams):
        logging.info("Processing message %s", msg)
