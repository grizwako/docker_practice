#!/usr/bin/env python

import asyncio
import aioredis

redis = None
publisher = None


async def connect():
    global redis, publisher
    redis = await aioredis.create_redis('redis://localhost')
    publisher = await aioredis.create_redis('redis://localhost')


async def process_tasks():
    while True:
        msg = await redis.brpop('tasks', timeout=0)
        decoded_msg = msg[1].decode('utf-8')
        user, number = decoded_msg.split(':')
        await redis.set('user:'+user, number)
        await publisher.publish('user_saved', decoded_msg)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(connect())
    asyncio.async(process_tasks())
    loop.run_forever()
