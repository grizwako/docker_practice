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


# sorted by username, sort is case sensitive
async def get_all_users():
    with (await redis_pool) as conn:
        cur = b'0'  # set initial cursor to 0
        all_keys = set()
        while cur:
            cur, keys = await conn.scan(cur, match='user:*')
            all_keys.update(keys)
        sorted_keys = sorted(all_keys)
        vals = await conn.mget(*sorted_keys)
        return dict(zip(
            map(lambda x: x.decode('utf-8')[5:], sorted_keys),
            map(int, vals)
        ))


async def save(msg, websocket):
    try:
        user, favorite = msg.split(':')
        favorite = int(favorite)
        await queue_task(msg)
    except ValueError:
        await websocket.send('Could not save, got:' + msg)
    except Exception:
        await websocket.send('Wild error appears!')


async def consumer_handler(websocket):
    async for message in websocket:
        await save(message, websocket)


# we could have just leave full redis key and capitalize
def stringify_users(user_map):
    return '\n'.join(
        user + ' has favorite number: ' + str(number)
        for user, number
        in user_map.items()
    )

async def producer_handler():
    while True:
        while await sub_channel.wait_message():
            # cache? preload users, and then apply changes to preloaded dict
            #        keep sorted invariant?
            _ = await sub_channel.get()
            users = await get_all_users()
            await ws_broadcast(stringify_users(users))


async def handler(websocket, path):
    active_connections.add(websocket)
    try:
        tasks = [asyncio.ensure_future(consumer_handler(websocket)),
                 asyncio.ensure_future(producer_handler())]
        done, pending = await asyncio.wait(tasks,
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


async def main():
    await connect()
    start_server = websockets.serve(handler, '127.0.0.1', 5678)
    asyncio.ensure_future(start_server)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main())
    loop.run_until_complete(task)
    loop.run_forever()
