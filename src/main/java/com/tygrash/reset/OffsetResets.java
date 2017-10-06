package com.tygrash.reset;

import com.tygrash.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class OffsetResets {

    private ConsumerFactory consumerFactory;

    public OffsetResets(ConsumerFactory consumerFactory) {
        this.consumerFactory = consumerFactory;
    }

    public Stream<GroupPartition> create(Stream<String> offsetInfos) {
        return offsetInfos.flatMap(this::toOffsetResetDetail);
    }

    private Stream<GroupPartition> toOffsetResetDetail(String csvRecord) {
        String[] records = csvRecord.split(",");
        if (records.length == 4) { // SUPPRESS CHECKSTYLE
            return Arrays.asList(new GroupPartition(records[0], records[1], // SUPPRESS CHECKSTYLE
                    Integer.parseInt(records[2]), Long.parseLong(records[3]))).stream(); // SUPPRESS CHECKSTYLE
        }
        List<GroupPartition> details = new ArrayList<>();
        KafkaConsumer consumer = consumerFactory.getConsumer(records[0]);
        List<PartitionInfo> topicPartitionsInKafka = consumer.partitionsFor(records[1]); // SUPPRESS CHECKSTYLE
        for (PartitionInfo info : topicPartitionsInKafka) {
            details.add(new GroupPartition(records[0], records[1], info.partition(), Long.parseLong(records[2]))); // SUPPRESS CHECKSTYLE
        }
        return details.stream();
    }
}
