import functools
import traceback
import asyncio
from collections import namedtuple

import rx
import rx.operators as ops
from rx.disposable import Disposable
from cyclotron import Component
from cyclotron.debug import trace_observable

from .asyncio import to_agen
from .consumer import ConsumerRebalancer

from kafka.partitioner import murmur2
import aiokafka
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

Sink = namedtuple('Sink', ['request'])
Source = namedtuple('Source', ['response', 'feedback'])

# Sink items
Consumer = namedtuple('Consumer', ['server', 'topics', 'group', 'max_partition_fetch_bytes'])
Consumer.__doc__ += ": Creates a consumer client that can subscribe to multiple topics"
Consumer.server.__doc__ += ": Address of the boostrap server"
Consumer.topics.__doc__ += ": Observable emitting ConsumerTopic items"
Consumer.__new__.__defaults__ = (1048576,)

Producer = namedtuple('Producer', ['server', 'topics', 'acks', 'max_request_size'])
Producer.__new__.__defaults__ = (1, 1048576)
Producer.__doc__ += ": Creates a producer client that can publish to pultiple topics"
Producer.server.__doc__ += ": Address of the boostrap server"
Producer.topics.__doc__ += ": Observable emitting ProducerTopic items"
Producer.acks.__doc__ += ": Records acknowledgement strategy, as documented in aiokafka"

ConsumerTopic = namedtuple('ConsumerTopic', ['topic', 'decode', 'control', 'start_from'])
ConsumerTopic.__new__.__defaults__ = (None, 'end')
ProducerTopic = namedtuple('ProducerTopic', ['topic', 'records', 'map_key', 'encode', 'map_partition'])

# Source items
ConsumerRecords = namedtuple('ConsumerRecords', ['topic', 'records'])


def choose_partition(key, partitions):
    if type(key) == int:
        idx = key
    else:
        idx = murmur2(key)
        idx &= 0x7fffffff
    idx %= len(partitions)
    return partitions[idx]


async def send_record(client, topic, key, value, partition_key):
    try:
        partitions = await client.partitions_for(topic)
        partition = choose_partition(partition_key, list(partitions))

        fut = await client.send(
        #await client.send(
            topic, key=key, value=value, partition=partition)

        return fut

    except Exception as e:
        print("exception: {}, {}".format(
            e, traceback.print_tb(e.__traceback__)),
        )


DelConsumerCmd = namedtuple('DelConsumerCmd', ['observer'])
AddConsumerCmd = namedtuple('AddConsumerCmd', ['observer', 'consumer'])
ConsumerProperties = namedtuple('ConsumerProperties', [
    'observer', 'topic', 'decode', 'start_from'
])


def run_consumer(loop, source_observer, server, group, topics):
    async def _run_consumer(topic_queue):
        consumers = {}
        control = {}
        control_dispose = {}

        def on_next_control(obv, i):
            nonlocal control
            control[obv] = i
            print("on_next_control: {}".format(i))

        try:
            client = AIOKafkaConsumer(
                loop=loop,
                bootstrap_servers=server,
                group_id=group,
                auto_offset_reset='none',
                enable_auto_commit=True,
            )
            print("start kafka consumer")
            await client.start()

            yield_countdown = 5000
            while True:
                if len(consumers) == 0 or not topic_queue.empty():
                    cmd = await topic_queue.get()
                    if type(cmd) is AddConsumerCmd:
                        print('run consumer: add {}'.format(cmd.consumer.topic))

                        if cmd.observer in consumers:
                            source_observer.on_error(ValueError(
                                "topic already subscribed for this consumer: {}".format(cmd.consumer.decode))
                            )
                            break

                        if cmd.consumer.control is not None:
                            control_dispose[cmd.observer] = cmd.consumer.control.subscribe(
                                on_next=functools.partial(on_next_control, cmd.observer),
                                on_error=source_observer.on_error,
                            )
                        print("started")

                        consumers[cmd.observer] = ConsumerProperties(
                            observer=cmd.observer,
                            topic=cmd.consumer.topic, decode=cmd.consumer.decode,
                            start_from=cmd.consumer.start_from,
                        )
                        start_positions = {}
                        topics = []
                        for c in consumers.items():
                            topics.append(c[1].topic)
                            start_positions[c[1].topic] = c[1].start_from
                        topics = set(topics)
                        client.subscribe(topics=topics, listener=ConsumerRebalancer(client, start_positions))

                    elif type(cmd) is DelConsumerCmd:
                        dispose = control_dispose.pop(cmd.observer, None)
                        if dispose is not None:
                            dispose()

                        consumer = consumers.pop(cmd.observer, None)
                        if consumer is not None:
                            start_positions = {}
                            topics = []
                            for c in consumers.items():
                                topics.append(c[1].topic)
                                start_positions[c[1].topic] = c[1].start_from
                            topics = set(topics)
                            if len(topics) > 0:
                                client.subscribe(topics=topics, listener=ConsumerRebalancer(client, start_positions))
                            cmd.observer.on_completed()
                    else:
                        source_observer.on_error(TypeError(
                            "invalid type for queue command: {}".format(cmd)))

                if len(consumers) == 0:
                    print("no consumer")
                    break

                regulated = False
                for observer, consumer in consumers.items():                    
                    if observer in control:
                        await asyncio.sleep(control[observer])
                        regulated = True
                        yield_countdown = 5000
                        break  # limitation only one controllable topic for now
                
                yield_countdown -= 1
                if yield_countdown == 0 and regulated is False:
                    await asyncio.sleep(0)
                    yield_countdown = 5000

                msg = await client.getone()
                for observer, consumer in consumers.items():
                    if consumer.topic == msg.topic:
                        decoded_msg = consumer.decode(msg.value)
                        observer.on_next(decoded_msg)

            await client.stop()

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
    def on_subscribe(i, observer, scheduler):
        print("topic subscribe: {}".format(i))

        def dispose():
            topic_queue.put_nowait(DelConsumerCmd(observer))

        topic_queue.put_nowait(AddConsumerCmd(observer, i))
        return Disposable(dispose)

    def on_next(i):
        print("consumer topic: {}".format(i))
        source_observer.on_next(ConsumerRecords(
            topic=i.topic,
            records=rx.create(functools.partial(on_subscribe, i))))

    task = loop.create_task(_run_consumer(topic_queue))
    topics.subscribe(
        on_next=on_next,
        on_error=source_observer.on_error
    )

    return task


def run_producer(loop, source_observer, server, topics, acks, max_request_size, get_feedback_observer):
    async def _run_producer(records):
        client = AIOKafkaProducer(
            loop=loop,
            bootstrap_servers=server,
            acks=acks,
            max_request_size=max_request_size)
        pending_records = []

        await client.start()
        gen = to_agen(records, loop, get_feedback_observer)
        print("started producer")
        async for record in gen:
            #print("record: {}".format(record))
            fut = await send_record(client, record[0], record[1], record[2], record[3])

            pending_records.append(fut)
            if len(pending_records) > 10000:
                _pending_records = pending_records.copy()
                pending_records = []
                await asyncio.gather(*_pending_records)

        # flush pending writes on completion
        print("producer completed")
        _pending_records = pending_records.copy()
        pending_records = []
        await asyncio.gather(*_pending_records)

        await client.flush()
        await client.stop()
        print("producer closed")

    records = topics.pipe(
        ops.flat_map(lambda topic: topic.records.pipe(
            ops.map(lambda i: (
                topic.topic,
                topic.map_key(i),
                topic.encode(i),
                topic.map_partition(i),
            )),
        ))
    )

    loop.create_task(_run_producer(records))


def make_driver(loop=None):
    loop = loop or asyncio.get_event_loop()

    def driver(sink):
        feedback_observer = None

        def get_feedback_observer():
            return feedback_observer

        def on_feedback_subscribe(observer, scheduler):
            def dispose():
                nonlocal feedback_observer
                feedback_observer = None

            nonlocal feedback_observer
            feedback_observer = observer
            return Disposable(dispose)

        def on_subscribe(observer, scheduler):
            consumer_tasks = []

            def on_next(i):
                if type(i) is Consumer:
                    print("consumer: {}".format(i))
                    task = run_consumer(loop, observer, i.server, i.group, i.topics)
                    consumer_tasks.append(task)
                elif type(i) is Producer:
                    run_producer(
                        loop, observer,
                        i.server, i.topics, i.acks, i.max_request_size,
                        get_feedback_observer)
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
            feedback=rx.create(on_feedback_subscribe),
        )

    return Component(call=driver, input=Sink)
