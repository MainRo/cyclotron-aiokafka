import aiokafka

'''
on_partitions_revoked: set()
on_partitions_assigned: {TopicPartition(topic='bench1_feature', partition=11), TopicPartition(topic='bench1_feature', partition=17), TopicPartition(topic='bench1_feature', partition=5), TopicPartition(topic='bench1_feature', partition=35), TopicPartition(topic='bench1_feature', partition=23), TopicPartition(topic='bench1_feature', partition=29)}
Heartbeat failed for group makinage_benchmark because it is rebalancing
on_partitions_revoked: frozenset({TopicPartition(topic='bench1_feature', partition=35), TopicPartition(topic='bench1_feature', partition=23), TopicPartition(topic='bench1_feature', partition=29), TopicPartition(topic='bench1_feature', partition=11), TopicPartition(topic='bench1_feature', partition=17), TopicPartition(topic='bench1_feature', partition=5)})
on_partitions_assigned: {TopicPartition(topic='bench1_feature', partition=27), TopicPartition(topic='bench1_feature', partition=20), TopicPartition(topic='bench1_feature', partition=34), TopicPartition(topic='bench1_feature', partition=13), TopicPartition(topic='bench1_feature', partition=6)}
Heartbeat failed for group makinage_benchmark because it is rebalancing
on_partitions_revoked: frozenset({TopicPartition(topic='bench1_feature', partition=27), TopicPartition(topic='bench1_feature', partition=20), TopicPartition(topic='bench1_feature', partition=34), TopicPartition(topic='bench1_feature', partition=13), TopicPartition(topic='bench1_feature', partition=6)})
on_partitions_assigned: {TopicPartition(topic='bench1_feature', partition=11), TopicPartition(topic='bench1_feature', partition=17), TopicPartition(topic='bench1_feature', partition=5), TopicPartition(topic='bench1_feature', partition=35), TopicPartition(topic='bench1_feature', partition=23), TopicPartition(topic='bench1_feature', partition=29)}



Heartbeat failed for group makinage_benchmark because it is rebalancing
on_partitions_revoked: frozenset({TopicPartition(topic='bench1_feature', partition=35), TopicPartition(topic='bench1_feature', partition=23), TopicPartition(topic='bench1_feature', partition=29), TopicPartition(topic='bench1_feature', partition=11), TopicPartition(topic='bench1_feature', partition=17), TopicPartition(topic='bench1_feature', partition=5)})
on_partitions_assigned: {TopicPartition(topic='bench1_feature', partition=1), TopicPartition(topic='bench1_feature', partition=14), TopicPartition(topic='bench1_feature', partition=20), TopicPartition(topic='bench1_feature', partition=34), TopicPartition(topic='bench1_feature', partition=29), TopicPartition(topic='bench1_feature', partition=10), TopicPartition(topic='bench1_feature', partition=16), TopicPartition(topic='bench1_feature', partition=25), TopicPartition(topic='bench1_feature', partition=6), TopicPartition(topic='bench1_feature', partition=15), TopicPartition(topic='bench1_feature', partition=21), TopicPartition(topic='bench1_feature', partition=35), TopicPartition(topic='bench1_feature', partition=2), TopicPartition(topic='bench1_feature', partition=11), TopicPartition(topic='bench1_feature', partition=17), TopicPartition(topic='bench1_feature', partition=30), TopicPartition(topic='bench1_feature', partition=36), TopicPartition(topic='bench1_feature', partition=7), TopicPartition(topic='bench1_feature', partition=12), TopicPartition(topic='bench1_feature', partition=26), TopicPartition(topic='bench1_feature', partition=32), TopicPartition(topic='bench1_feature', partition=3), TopicPartition(topic='bench1_feature', partition=8), TopicPartition(topic='bench1_feature', partition=22), TopicPartition(topic='bench1_feature', partition=31), TopicPartition(topic='bench1_feature', partition=4), TopicPartition(topic='bench1_feature', partition=18), TopicPartition(topic='bench1_feature', partition=13), TopicPartition(topic='bench1_feature', partition=27), TopicPartition(topic='bench1_feature', partition=33), TopicPartition(topic='bench1_feature', partition=0), TopicPartition(topic='bench1_feature', partition=9), TopicPartition(topic='bench1_feature', partition=23), TopicPartition(topic='bench1_feature', partition=28), TopicPartition(topic='bench1_feature', partition=5), TopicPartition(topic='bench1_feature', partition=19), TopicPartition(topic='bench1_feature', partition=24)}

'''
class ConsumerRebalancer(aiokafka.ConsumerRebalanceListener):

    def __init__(self, client, start_positions):
        self.client = client # leak on cyclic reference ?
        self.start_positions = start_positions

    async def on_partitions_revoked(self, revoked):
        print("on_partitions_revoked: {}".format(revoked))

    async def on_partitions_assigned(self, assigned):
        print("on_partitions_assigned: {}".format(assigned))

        seek_to_beginning_partitions = []
        seek_to_end_partitions = []
        for tp in assigned:
            if self.start_positions[tp.topic] == 'beginning':
                seek_to_beginning_partitions.append(tp)
            elif self.start_positions[tp.topic] == 'end':
                seek_to_end_partitions.append(tp)

        if len(seek_to_beginning_partitions) > 0:
            print("seeking these partition to beginning offet: {}".format(seek_to_beginning_partitions))
            await self.client.seek_to_beginning(*seek_to_beginning_partitions)
        if len(seek_to_end_partitions) > 0:
            print("seeking these partition to end offet: {}".format(seek_to_end_partitions))
            await self.client.seek_to_end(*seek_to_end_partitions)
