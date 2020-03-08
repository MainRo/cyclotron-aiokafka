import asyncio
import rx
import rx.operators as ops
from rx.core.notification import OnNext, OnError, OnCompleted
from rx.scheduler.eventloop import AsyncIOScheduler


async def to_agen(obs, loop):
    queue = asyncio.Queue()

    def on_next(i):
        queue.put_nowait(i)

    disposable = obs.pipe(ops.materialize()).subscribe(
        on_next=on_next,
        scheduler=AsyncIOScheduler(loop=loop)
    )

    while True:
        i = await queue.get()
        if isinstance(i, OnNext):
            yield i.value
            queue.task_done()
        elif isinstance(i, OnError):
            disposable.dispose()
            raise(Exception(i.value))
        else:
            disposable.dispose()
            break