#!/usr/bin/env python

import asyncio
import websockets

active_connections = set()
favorite_number_by_user = {}


async def broadcast():
    for ws in active_connections:
        await  ws.send(str(favorite_number_by_user))


async def save(msg, websocket):
    try:
        user, favorite = msg.split(':')
        favorite = int(favorite)
        favorite_number_by_user[user] = favorite
        await broadcast()
    except ValueError:
        await websocket.send('Could not save, got:' + msg)
    except Exception:
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

start_server = websockets.serve(handler, '127.0.0.1', 5678)

asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()