package com.tygrash.reset;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

@Getter
@AllArgsConstructor
public class GroupPartition {
    private final Logger logger = LoggerFactory.getLogger(GroupPartition.class.getName());

    private String consumerGroup;
    private String kafkaTopic;
    private Integer partitionNumber;
    private Long newOffset;

    public void doReset(KafkaConsumer consumer) {
        List<PartitionInfo> topicPartitionsInKafka = consumer.partitionsFor(kafkaTopic);

        if (isPartitionPresent(topicPartitionsInKafka, partitionNumber)) {
            Map<TopicPartition, OffsetAndMetadata> offsets = createOffsetsAndMetadata(partitionNumber, kafkaTopic, newOffset);
            consumer.commitSync(offsets);

//            Set<TopicPartition> topicPartitions = offsets.keySet();
//            for (TopicPartition partition : topicPartitions) {
//                consumer.subscribe(Arrays.asList(partition.topic()));
//                consumer.poll(100);
//                consumer.commitSync(offsets);
////                consumer.seek(partition, offsets.get(partition).offset());
//            }
            logger.info("Successfully reset the newOffset to {} for topic {} partition {}", newOffset, kafkaTopic, partitionNumber);
            return;
        }

        logger.error("GroupPartition {} for topic {} does not exist", partitionNumber, kafkaTopic);
    }

    private Map<TopicPartition, OffsetAndMetadata> createOffsetsAndMetadata(Integer partition, String kafkaTopic, Long offset) { // SUPPRESS CHECKSTYLE
        Map<TopicPartition, OffsetAndMetadata> topicPartitionAndOffset = new HashMap<>();

        TopicPartition topicPartition = new TopicPartition(kafkaTopic, partition);
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
        topicPartitionAndOffset.put(topicPartition, offsetAndMetadata);

        return topicPartitionAndOffset;
    }

    private boolean isPartitionPresent(List<PartitionInfo> topicPartitionsInKafka, Integer partitionNumberForReset) {
        for (PartitionInfo partitionInfo : topicPartitionsInKafka) {
            if (partitionInfo.partition() == partitionNumberForReset) {
                return true;
            }
        }
        return false;
    }
}
