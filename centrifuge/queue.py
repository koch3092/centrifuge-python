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

    def _run_task(self, coro, *args):
        """
        @see https://github.com/python/cpython/issues/91887
        """
        task = asyncio.create_task(coro(*args))
        self._task.add(task)
        task.add_done_callback(self._task.discard)

    async def dispatch(self):
        while True:
            cb: Dict = await self.queue.get()
            if cb is None:
                return
            self._run_task(cb["fn"])
            self.queue.task_done()

    async def push(self, f: Callable):
        await self.queue.put({"fn": f, "tm": time.time()})

    async def close(self):
        await asyncio.shield(self._close())

    async def _close(self):
        await self.queue.put(None)
        await self.queue.join()
        self.dispatch_task.cancel()
