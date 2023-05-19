from typing import Iterable

import aiomysql

from configuration import HandlerParams


class MySQLHandlerParamsRepository:

    def __init__(self, mariadb: str):
        self.pool = await aiomysql.create_pool(host='127.0.0.1', port=3306,
                                               user='root', password='',
                                               db='mysql')
        # TODO: change

    def __del__(self):
        self.pool.close()
        await self.pool.wait_closed()

    async def load(self):
        pass

    async def add(self, data: Iterable[HandlerParams], version: str):
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.executemany("SELECT 42;")
                print(cur.description)
                (r,) = await cur.fetchone()
                assert r == 42
        for handler in data
            pass

    async def remove(self, version: str):
        pass
