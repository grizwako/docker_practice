#!/usr/bin/env python

import asyncio
import websockets
import aioredis


active_websockets = set()
favorite_number_by_user = {}

redis_pool = None
redis_sub = None
redis_sub_channel = None
redis_address = 'redis://redis'


async def queue_task(msg):
    with (await redis_pool) as conn:
        await conn.lpush('tasks', msg)


async def broadcast(message):
    for websocket in active_websockets:
        await  websocket.send(message)


# sorted by username, sort is case sensitive
async def get_all_users():
    with (await redis_pool) as conn:
        cur = b'0'  # set initial cursor to 0
        all_keys = set()
        while cur:
            cur, keys = await conn.scan(cur, match='user:*')
            all_keys.update(keys)
        sorted_keys = sorted(all_keys)
        # MGET probably blows with some number of keys, check limits in redis
        # aioredis might also handle it under the hood, probably not
        # for production batch reads (if there is such a limit)
        vals = await conn.mget(*sorted_keys)
        return dict(zip(
            map(lambda x: x.decode('utf-8')[5:], sorted_keys),
            map(int, vals)
        ))


async def save(msg, websocket):
    try:
        user, favorite = msg.split(':')
        _ = int(favorite)
        await queue_task(msg)
    except ValueError:
        await websocket.send('Could not save, got:' + msg)
    except Exception:
        await websocket.send('Wild error appears!')


async def input_handler(websocket):
    async for message in websocket:
        if message == 'LIST':
            users = await get_all_users()
            await websocket.send(stringify_users(users))
        else:
            await save(message, websocket)


def stringify_users(user_map):
    return '\n'.join(
        user + ' has favorite number: ' + str(number)
        for user, number
        in user_map.items()
    )


async def notification_handler():
    while True:
        while await redis_sub_channel.wait_message():
            # cache? preload users, and then apply changes to preloaded dict
            #        keep sorted invariant?
            _ = await redis_sub_channel.get()
            users = await get_all_users()
            await broadcast(stringify_users(users))


async def ws_handler(websocket, path):
    active_websockets.add(websocket)
    try:
        tasks = [asyncio.ensure_future(input_handler(websocket)),
                 asyncio.ensure_future(notification_handler())]
        done, pending = await asyncio.wait(
            tasks, return_when=asyncio.ALL_COMPLETED)
        for task in pending:
            task.cancel()
    finally:
        active_websockets.remove(websocket)


async def redis_connect():
    global redis_pool, redis_sub, redis_sub_channel
    redis_pool = await aioredis.create_redis_pool(
        redis_address,
        minsize=5, maxsize=10)
    redis_sub = await aioredis.create_redis(redis_address)
    sub = await redis_sub.subscribe('user_saved')
    redis_sub_channel = sub[0]


async def main():
    await redis_connect()
    ws_server = websockets.serve(ws_handler, '0.0.0.0', 5678)
    asyncio.ensure_future(ws_server)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    server = asyncio.ensure_future(main())
    loop.run_until_complete(server)
    loop.run_forever()
