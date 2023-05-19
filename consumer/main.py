import asyncio
import logging
import signal

from configuration import Configuration
from consumer import Runner

logging.basicConfig(level=logging.INFO)


async def main():
    runner = Runner(configuration=Configuration())
    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame),
                            lambda: asyncio.ensure_future(runner.stop()))
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
