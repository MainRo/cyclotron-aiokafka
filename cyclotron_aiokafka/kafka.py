import traceback
import asyncio
from collections import namedtuple

import rx
import rx.operators as ops
from rx.disposable import Disposable
from cyclotron import Component

from .asyncio import to_agen

from kafka.partitioner.hashed import murmur2
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

Sink = namedtuple('Sink', ['request'])
Source = namedtuple('Source', ['response'])

# Sink items
Consumer = namedtuple('Consumer', ['server', 'topics'])
Consumer.__doc__ += ": Creates a consumer client that can subscribe to multiple topics"
Consumer.server.__doc__ += ": Address of the boostrap server"
Consumer.topics.__doc__ += ": Observable emitting ConsumerTopic items"

Producer = namedtuple('Producer', ['server', 'topics', 'acks'])
Producer.__new__.__defaults__ = (1,)
Producer.__doc__ += ": Creates a producer client that can publish to pultiple topics"
Producer.server.__doc__ += ": Address of the boostrap server"
Producer.topics.__doc__ += ": Observable emitting ProducerTopic items"
Producer.acks.__doc__ += ": Records acknowledgement strategy, as documented in aiokafka"

ConsumerTopic = namedtuple('ConsumerTopic', ['topic', 'group'])
ConsumerTopic.__new__.__defaults__ = (None,)

ProducerTopic = namedtuple('ProducerTopic', ['topic', 'records', 'key_mapper'])
ProducerTopic.__new__.__defaults__ = (None,)


# Source items
ConsumerRecords = namedtuple('ConsumerRecords', ['topic', 'records'])


def choose_partition(key, partitions):
    idx = murmur2(key)
    idx &= 0x7fffffff
    idx %= len(partitions)
    return partitions[idx]


async def send_record(client, topic, key, value, partition_bytes):
    try:
        partitions = await client.partitions_for(topic)
        partition = choose_partition(key, list(partitions))

        await client.send(
            topic, key=key, value=value, partition=partition)
        '''
        producer.pending_records.append(fut)
        if len(producer.pending_records) > 20000:
            pending_records = producer.pending_records.copy()
            producer.pending_records = []
            await asyncio.gather(*pending_records)
        '''

    except Exception as e:
        print("exception: {}, {}".format(
            e, traceback.print_tb(e.__traceback__)),
        )


def run_consumer(loop, source_observer, server, topics):
    async def _run_consumer(topic_queue):
        clients = {}
        try:
            while True:
                if len(clients) == 0 or not topic_queue.empty():
                    cmd = await topic_queue.get()
                    if cmd[0] == 'add':
                        print('run consumer: add')
                        client = AIOKafkaConsumer(
                            cmd[1],
                            loop=loop,
                            bootstrap_servers=server,
                            group_id=cmd[2],
                            auto_offset_reset='earliest')
                        await client.start()
                        print("started")
                        if cmd[3] in clients:
                            source_observer.on_error(ValueError(
                                "topic already subscribed for this consumer: {}".format(cmd[3])))
                        else:
                            clients[cmd[3]] = client
                    elif cmd[0] == 'del':
                        print('run consumer: del')
                        client = clients.pop(cmd[1], None)
                        if client is not None:
                            await client.stop()
                            cmd[1].on_completed()
                    else:
                        source_observer.on_error(TypeError(
                            "invalid type for queue command: {}".format(cmd)))

                if len(clients) == 0:
                    print("no client")
                    break

                for observer, client in clients.items():
                    break  # take first entry for now
                print("selected client")
                msg = await client.getone()
                print("msg")
                observer.on_next(msg)

        except asyncio.CancelledError as e:
            print("cancelled {}".format(e))
        except Exception as e:
            print(e)

    topic_queue = asyncio.Queue()
    ''' for each topic consumer request, send a new ConsumerRecords on driver
     source, and forward the request to the consumer scheduler coroutine.
     The kafka consumer is stated when the application subscribes to the
     create observable, and stopped on disposal
    '''
    def on_next(i):
        print("consumer topic: {}".format(i))
        def on_subscribe(observer, scheduler):
            print("topic subscribe")
            def dispose():
                topic_queue.put_nowait(('del', observer))

            topic_queue.put_nowait(('add', i.topic, i.group, observer))
            return Disposable(dispose)

        source_observer.on_next(ConsumerRecords(
            topic=i.topic,
            records=rx.create(on_subscribe)))

    task = loop.create_task(_run_consumer(topic_queue))
    topics.subscribe(
        on_next=on_next,
        on_error=source_observer.on_error
    )

    return task


def run_producer(loop, source_observer, server, topics, acks):
    async def _run_producer(records):
        client = AIOKafkaProducer(
            loop=loop,
            bootstrap_servers=server,
            acks=acks)
        # pending_records = []

        await client.start()
        gen = to_agen(records, loop)
        print("started producer")
        async for record in gen:
            print("record: {}".format(record))
            await send_record(client, record[0], record[1], record[2], None)
            '''
            pending_records.append(fut)
            if len(pending_records) > 20000:
                _pending_records = pending_records.copy()
                pending_records = []
                await asyncio.gather(*_pending_records)
            '''

        await client.stop()

    records = topics.pipe(
        ops.flat_map(lambda topic: topic.records.pipe(
            ops.map(lambda i: (
                topic.topic,
                topic.key_mapper(i),
                i
            ))
        ))
    )

    loop.create_task(_run_producer(records))


def make_driver(loop=None):
    loop = loop or asyncio.get_event_loop()

    def driver(sink):
        def on_subscribe(observer, scheduler):
            consumer_tasks = []

            def on_next(i):
                if type(i) is Consumer:
                    print("consumer: {}".format(i))
                    task = run_consumer(loop, observer, i.server, i.topics)
                    consumer_tasks.append(task)
                elif type(i) is Producer:
                    run_producer(loop, observer, i.server, i.topics, i.acks)
                else:
                    e = "Unknown item type: {}".format(i)
                    print(e)
                    observer.on_error(TypeError(e))

            print("driver kafka subscribe")
            return sink.request.subscribe(
                on_next=on_next,
                on_error=observer.on_error,
            )
        return Source(
            response=rx.create(on_subscribe),
        )

    return Component(call=driver, input=Sink)
