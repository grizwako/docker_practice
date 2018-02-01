#!/usr/bin/env python

import asyncio
import aioredis

redis = None


async def connect():
    global redis
    redis = await aioredis.create_redis(
        'redis://localhost',
        loop=loop)


async def process_tasks():
    while True:
        msg = await redis.brpop('tasks', timeout=0)
        user, number = msg[1].decode('utf-8').split(':')
        await redis.set('user:'+user, number)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(connect())
    asyncio.async(process_tasks())
    loop.run_forever()