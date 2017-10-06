package com.tygrash.reset;

import com.tygrash.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OffsetResetter {
    private Map<String, KafkaConsumer> consumerMap = new HashMap<>();

    private List<GroupPartition> partitions;
    private ConsumerFactory consumerFactory;

    public OffsetResetter(List<GroupPartition> partitions, ConsumerFactory consumerFactory) {
        this.partitions = partitions;
        this.consumerFactory = consumerFactory;
    }

    public void resetPartitionOffsets() {
        for (GroupPartition partition : partitions) {
            KafkaConsumer consumer = consumerMap.computeIfAbsent(partition.getConsumerGroup(), key -> createNewConsumer(partition.getConsumerGroup()));
            partition.doReset(consumer);
        }
    }

    private KafkaConsumer createNewConsumer(String consumerGroupId) {
        return consumerFactory.getConsumer(consumerGroupId);
    }
}
