import asyncio
from modules import batch

count = 0
max = 10

async def batch_test_fn(task_id: int):
    global count, max
    cnt = count
    count += 1
    print(f"task_id:{task_id}")
    if(cnt >= max):
        batch.batch_list['test'].stop()
        count=0
    else:
        print(f"count: {cnt}-start")
        await asyncio.sleep(2)
        print(f"count: {cnt}-end")