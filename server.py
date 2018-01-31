#!/usr/bin/env python

import asyncio
import websockets
import aioredis


active_connections = set()
favorite_number_by_user = {}

redis_pool = None

async def queue_task(msg):
    with (await redis_pool) as conn:
        await conn.lpush('tasks', msg)



async def broadcast():
    for ws in active_connections:
        await  ws.send(str(favorite_number_by_user))


async def save(msg, websocket):
    try:
        user, favorite = msg.split(':')
        favorite = int(favorite)
        await queue_task(msg)
    except ValueError:
        await websocket.send('Could not save, got:' + msg)
    except Exxxception:
        await websocket.send('Wild error appears!')
    return True


async def consumer_handler(websocket):
    async for message in websocket:
        await save(message, websocket)


async def producer():
    await asyncio.sleep(3)
    return 'ping'


async def producer_handler(websocket):
    while True:
        message = await producer()
        if message:
            await websocket.send(message)


async def handler(websocket, path):
    active_connections.add(websocket)
    try:
        task_coros = [consumer_handler, producer_handler]
        tasks = [asyncio.ensure_future(coro(websocket))
                 for coro in task_coros]
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
    finally:
        active_connections.remove(websocket)


async def main():
    global redis_pool
    redis_pool = await aioredis.create_redis_pool(
        'redis://localhost',
        minsize=5, maxsize=10,
        loop=loop)
    start_server = websockets.serve(handler, '127.0.0.1', 5678)
    asyncio.ensure_future(start_server)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    task = asyncio.ensure_future(main())
    loop.run_until_complete(task)
    loop.run_forever()
