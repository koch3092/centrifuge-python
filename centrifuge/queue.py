import asyncio
import time
from typing import Callable, Dict


class CBQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.dispatch_task = asyncio.create_task(self.dispatch())
        # @see https://docs.python.org/zh-cn/3.10/library/asyncio-task.html?highlight=discard#asyncio.create_task
        self._task = set()
        self._task.add(self.dispatch_task)
        self.dispatch_task.add_done_callback(self._task.discard)

    async def dispatch(self):
        while True:
            cb: Dict = await self.queue.get()
            if cb is None:
                continue
            await cb["fn"]()
            self.queue.task_done()

    async def push(self, f: Callable):
        await self.queue.put({"fn": f, "tm": time.time()})

    def close(self):
        asyncio.run_coroutine_threadsafe(self._close(), asyncio.get_event_loop())

    async def _close(self):
        await self.queue.put(None)
        await self.queue.join()
        self.dispatch_task.cancel()
