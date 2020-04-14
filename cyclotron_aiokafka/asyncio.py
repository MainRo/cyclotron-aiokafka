import asyncio
import rx.operators as ops
from rx.core.notification import OnNext, OnError
from rx.scheduler.eventloop import AsyncIOScheduler


async def to_agen(obs, loop, get_feedback_observer):
    queue = asyncio.Queue()
    index = 0

    def on_next(i):
        nonlocal index
        queue.put_nowait(i)
        if isinstance(i, OnNext):
            index += 1
            if index == 500:
                index = 0
                obv = get_feedback_observer()
                if obv is not None:
                    obv.on_next((i.value[0], queue.qsize()))  # todo: mapper

    disposable = obs.pipe(ops.materialize()).subscribe(
        on_next=on_next,
        scheduler=AsyncIOScheduler(loop=loop)
    )

    while True:
        try:
            i = queue.get_nowait()
        except asyncio.QueueEmpty:
            i = await queue.get()

        if isinstance(i, OnNext):
            yield i.value
            queue.task_done()
        elif isinstance(i, OnError):
            disposable.dispose()
            raise(Exception(i.exception))
        else:
            disposable.dispose()
            break
