import asyncio
from typing import Callable, Awaitable

class batchSet:
    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self._stop_flag = False
        self._semaphore = asyncio.Semaphore(batch_size)

    def stop(self):
        """중단 신호 보내기"""
        self._stop_flag = True

    def reset(self):
        """다시 시작할 때 플래그 초기화"""
        self._stop_flag = False

    async def batch(self, func: Callable[[int], Awaitable[None]], total_tasks: int = 1):
        async def safe_func_wrapper(task_id):
            async with self._semaphore:
                if self._stop_flag:
                    print(f"[{task_id}] 중단됨")
                    return
                await func(task_id)

        tasks = [asyncio.create_task(safe_func_wrapper(i)) for i in range(total_tasks)]
        await asyncio.gather(*tasks)

scrap_list_batch = batchSet()