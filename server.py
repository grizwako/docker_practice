#!/usr/bin/env python

import asyncio
import websockets
import aioredis


active_connections = set()
favorite_number_by_user = {}

redis_pool = None
redis_sub = None
sub_channel = None


async def queue_task(msg):
    with (await redis_pool) as conn:
        await conn.lpush('tasks', msg)


async def ws_broadcast(message):
    for ws in active_connections:
        await  ws.send(message)


async def save(msg, websocket):
    try:
        user, favorite = msg.split(':')
        favorite = int(favorite)
        await queue_task(msg)
    except ValueError:
        await websocket.send('Could not save, got:' + msg)
    except Exception:
        await websocket.send('Wild error appears!')
    return True


async def consumer_handler(websocket):
    async for message in websocket:
        await save(message, websocket)


async def subscription():
    while await sub_channel.wait_message():
        msg = await sub_channel.get()
        return msg


async def producer_handler(websocket):
    while True:
        message = await subscription()
        if message:
            await ws_broadcast('Some user got updated')


async def handler(websocket, path):
    active_connections.add(websocket)
    try:
        tasks = [asyncio.ensure_future(consumer_handler(websocket)),
                 asyncio.ensure_future(producer_handler(websocket))]
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        active_connections.remove(websocket)


async def connect():
    global redis_pool, redis_sub, sub_channel
    redis_pool = await aioredis.create_redis_pool(
        'redis://localhost',
        minsize=5, maxsize=10)
    redis_sub = await aioredis.create_redis('redis://localhost')

    sub = await redis_sub.subscribe('user_saved')
    sub_channel = sub[0]
    asyncio.ensure_future(subscription())


async def main():
    await connect()

    start_server = websockets.serve(handler, '127.0.0.1', 5678)
    asyncio.ensure_future(start_server)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main())
    loop.run_until_complete(task)
    loop.run_forever()
