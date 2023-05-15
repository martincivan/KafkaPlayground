import asyncio
import signal

from configuration import Configuration
from consumer import Consumer


async def main():
    runner = Consumer(configuration=Configuration())
    signal.signal(signal.SIGINT, runner.stop)
    signal.signal(signal.SIGTERM, runner.stop)
    await runner.run()


if __name__ == "__main__":
    asyncio.run(main())
