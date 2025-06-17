import asyncio
from typing import Callable, Awaitable

class batchSet:
    def __init__(self, batch_size: int):
        self.batch_size = batch_size
        self._stop_flag = False
        self._semaphore = asyncio.Semaphore(batch_size)
        self._running_tasks = set()  # 현재 실행 중인 task 목록

    def stop(self):
        self._stop_flag = True

    def reset(self):
        self._stop_flag = False

    def running_count(self) -> int:
        # 현재 실행 중인 task 수 반환
        return len(self._running_tasks)

    async def batch(self, func: Callable[[int], Awaitable[None]], total_tasks: int = 1):
        async def safe_func_wrapper(task_id):
            async with self._semaphore:
                if self._stop_flag:
                    print(f"[{task_id}] 중단됨")
                    return
                try:
                    await func(task_id)
                except Exception as e:
                    print(f"[{task_id}] ❌ 예외 발생: {e}")
                    self.stop()  # 전체 배치 중단
                    raise  # 작업 자체는 실패 처리 (gather가 취소 처리할 수 있게)

        if total_tasks < 0:
            # 무한 반복 실행
            task_id = 0
            while not self._stop_flag:
                if len(self._running_tasks) < self.batch_size:
                    task = asyncio.create_task(safe_func_wrapper(task_id))
                    self._running_tasks.add(task)
                    task.add_done_callback(lambda t: self._running_tasks.discard(t))
                    task_id += 1
                await asyncio.sleep(0.01)
        else:
            tasks = [asyncio.create_task(safe_func_wrapper(i)) for i in range(total_tasks)]
            self._running_tasks.update(tasks)
            for t in tasks:
                t.add_done_callback(lambda t: self._running_tasks.discard(t))
            await asyncio.gather(*tasks)

batch_list = {}