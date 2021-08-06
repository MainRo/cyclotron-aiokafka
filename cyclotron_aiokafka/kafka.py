import functools
from enum import Enum
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
from aiokafka.structs import TopicPartition


DataFeedMode = Enum('DataFeedMode', ['PUSH', 'PULL'])
DataSourceType = Enum('DataFeedMode', ['STREAM', 'BATCH'])

Sink = namedtuple('Sink', ['request'])
Source = namedtuple('Source', ['response', 'feedback'])

# Sink items
Consumer = namedtuple('Consumer', ['server', 'topics', 'group', 'max_partition_fetch_bytes', 'source_type', 'feed_mode'])
Consumer.__doc__ += ": Creates a consumer client that can subscribe to multiple topics"
Consumer.server.__doc__ += ": Address of the boostrap server"
Consumer.topics.__doc__ += ": Observable emitting ConsumerTopic items"
Consumer.__new__.__defaults__ = (1048576, DataSourceType.STREAM, DataFeedMode.PUSH)

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


DelConsumerCmd = namedtuple('DelConsumerCmd', ['topic'])
AddConsumerCmd = namedtuple('AddConsumerCmd', ['observer', 'consumer'])
PullTopicPartitionCmd = namedtuple('PullTopicPartition', ['topic_partition', 'count'])
PushRecordCmd = namedtuple('PushRecordCmd', [])
TopicContext = namedtuple('ConsumerProperties', [
    'observer', 'topic', 'decode', 'start_from', 'partitions',
])
AssignedCmd = namedtuple('AssignedCmd', [])
RevokedCmd = namedtuple('RevokedCmd', [])

class TopicPartitionContext(object):
    def __init__(self):
        self.tp = None
        self.observer = None
        self.completed = False


def run_consumer(loop, source_observer, server, group, topics, source_type, feed_mode):
    topic_queue = asyncio.Queue()

    def on_partition_back(tp_context, i):
        topic_queue.put_nowait(PullTopicPartitionCmd(tp_context, i))

    async def _run_consumer(topic_queue):
        control = {}
        control_dispose = {}
        topics = {} # context of each subscribed topic

        def on_next_control(obv, i):
            nonlocal control
            control[obv] = i
            print("on_next_control: {}".format(i))

        def on_partition_subscribe(tp_context, observer, scheduler):
            tp_context.observer = observer
            if feed_mode is DataFeedMode.PULL:
                observer.on_next(functools.partial(on_partition_back, tp_context.tp))

        def on_revoked(tps):
            for tp in tps:
                topics[tp.topic].partitions[tp.partition].observer.on_completed()
                del topics[tp.topic].partitions[tp.partition]
            if len(topics[tp.topic].partitions) == 0:
                topic_queue.put_nowait(RevokedCmd())

        def on_assigned(tps):
            for tp in tps:
                context = TopicPartitionContext()
                context.tp = tp
                topics[tp.topic].partitions[tp] = context
                topics[tp.topic].observer.on_next(
                    rx.create(functools.partial(on_partition_subscribe, context))
                )

            topic_queue.put_nowait(AssignedCmd())

        async def tp_is_completed(topic_partition):
            if source_type is DataSourceType.BATCH:
                highwater = client.highwater(topic_partition)
                if highwater:
                    position = await client.position(topic_partition)
                    if highwater == position:
                        print("no more lag on {}-{}".format(topic_partition.topic, topic_partition.partition))
                        topics[topic_partition.topic].partitions[topic_partition].completed = True
                        return True
            return False

        async def process_next_batch(topic_partition, count):
            tp = [topic_partition] if topic_partition else []
            read_count = 0
            if count == 1:
                msg = await client.getone(*tp)
                if topic_partition is None:
                    topic_partition = TopicPartition(msg.topic, msg.partition)
                topic = topics[topic_partition.topic]

                decoded_msg = topic.decode(msg.value)
                topic.partitions[topic_partition].observer.on_next(decoded_msg)
                read_count += 1
            else:
                data = await client.getmany(*tp, timeout_ms=5000, max_records=count)
                if len(data) > 0:
                    msgs = data[topic_partition]
                    topic = topics[topic_partition.topic]
                    for msg in msgs:
                        decoded_msg = topic.decode(msg.value)
                        topic.partitions[topic_partition].observer.on_next(decoded_msg)
                        read_count += 1

            return read_count

        try:
            client = AIOKafkaConsumer(
                loop=loop,
                bootstrap_servers=server,
                group_id=group,
                auto_offset_reset='latest',
                enable_auto_commit=True,
            )
            print("start kafka consumer")
            await client.start()

            partition_assigned = False
            yield_countdown = 5000
            prev_partition = None
            pcount = 0
            while True:
                try:
                    cmd = topic_queue.get_nowait()
                except asyncio.QueueEmpty as e:
                    print("queue empty")
                    cmd = await topic_queue.get()

                #if len(topics) == 0 or not topic_queue.empty():
                #cmd = await topic_queue.get()
                if type(cmd) is AddConsumerCmd:
                    print('run consumer: add {}'.format(cmd.consumer.topic))

                    if cmd.consumer.topic in topics:
                        source_observer.on_error(ValueError(
                            "topic already subscribed for this consumer: {}".format(cmd.consumer.decode))
                        )
                        break

                    if cmd.consumer.control is not None:
                        control_dispose[cmd.observer] = cmd.consumer.control.subscribe(
                            on_next=functools.partial(on_next_control, cmd.observer),
                            on_error=source_observer.on_error,
                        )

                    topics[cmd.consumer.topic] = TopicContext(
                        observer=cmd.observer,
                        topic=cmd.consumer.topic, decode=cmd.consumer.decode,
                        start_from=cmd.consumer.start_from,
                        partitions={}
                    )
                    sub_start_positions = {}
                    sub_topics = []
                    for k, c in topics.items():
                        sub_topics.append(c.topic)
                        sub_start_positions[c.topic] = c.start_from
                    sub_topics = set(sub_topics)
                    client.subscribe(topics=sub_topics, listener=ConsumerRebalancer(
                        client, sub_start_positions,
                        on_revoked=on_revoked,
                        on_assigned=on_assigned,
                    ))

                elif type(cmd) is DelConsumerCmd:
                    print('run consumer: del {}'.format(cmd))
                    topic = topics[cmd.topic]
                    dispose = control_dispose.pop(topic.observer, None)
                    if dispose is not None:
                        dispose()

                    topics.pop(cmd.topic)
                    sub_start_positions = {}
                    sub_topics = []
                    for k, c in topics.items():
                        sub_topics.append(c.topic)
                        sub_start_positions[c.topic] = c.start_from
                    sub_topics = set(sub_topics)
                    if len(sub_topics) > 0:
                        client.subscribe(topics=sub_topics, listener=ConsumerRebalancer(
                            client, sub_start_positions,
                            on_revoked=on_revoked,
                            on_assigned=on_assigned,
                        ))
                    topic.observer.on_completed()
                elif type(cmd) is PullTopicPartitionCmd:
                    no_lag = await tp_is_completed(cmd.topic_partition)
                    if source_type is DataSourceType.BATCH and no_lag == True:
                        topic = topics[cmd.topic_partition.topic]
                        topic.partitions[cmd.topic_partition].observer.on_completed()
                        if all([i.completed for _, i in topic.partitions.items()]):
                            print("completed processing topic {}".format(cmd.topic_partition.topic))
                            topic.observer.on_completed()
                    else:
                        await process_next_batch(cmd.topic_partition, cmd.count)
                elif type(cmd) is PushRecordCmd:
                    read_count = await process_next_batch(None, 1)
                    if read_count > 0:
                        topic_queue.put_nowait(PushRecordCmd())
                elif type(cmd) is AssignedCmd:
                    if partition_assigned is False:
                        partition_assigned = True
                        if feed_mode is DataFeedMode.PUSH:
                            topic_queue.put_nowait(PushRecordCmd())
                elif type(cmd) is RevokedCmd:
                    partition_assigned = False
                else:
                    source_observer.on_error(TypeError(
                        "invalid type for queue command: {}".format(cmd)))

                if len(topics) == 0:
                    print("no more topic subscribed, ending consumer task")
                    break

                regulated = False
                for topic, consumer in topics.items():
                    if consumer.observer in control:
                        await asyncio.sleep(control[consumer.observer])
                        regulated = True
                        yield_countdown = 5000
                        break  # limitation only one controllable topic for now
                
                yield_countdown -= 1
                if yield_countdown == 0 and regulated is False:
                    await asyncio.sleep(0)
                    yield_countdown = 5000

            await client.stop()

        except asyncio.CancelledError as e:
            print("cancelled {}".format(e))
        except Exception as e:
            print("consummer exception: {}:{}".format(type(e), e))
            print(traceback.format_list(traceback.extract_tb(e.__traceback__)))
            raise e

    ''' for each topic consumer request, send a new ConsumerRecords on driver
     source, and forward the request to the consumer scheduler coroutine.
     The kafka consumer is stated when the application subscribes to the
     create observable, and stopped on disposal
    '''
    def on_topic_subscribe(i, observer, scheduler):
        print("topic subscribe: {}".format(i))

        def dispose():
            topic_queue.put_nowait(DelConsumerCmd(i.topic))

        topic_queue.put_nowait(AddConsumerCmd(observer, i))
        return Disposable(dispose)

    def on_topic_next(i):
        print("consumer topic: {}".format(i))
        source_observer.on_next(ConsumerRecords(
            topic=i.topic,
            records=rx.create(functools.partial(on_topic_subscribe, i))))

    task = loop.create_task(_run_consumer(topic_queue))
    topics.subscribe(
        on_next=on_topic_next,
        on_error=source_observer.on_error,
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
                    print("starting consumer: {}".format(i))
                    task = run_consumer(loop, observer, i.server, i.group, i.topics, i.source_type, i.feed_mode)
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
