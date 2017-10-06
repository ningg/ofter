package com.tygrash.launch;

import com.google.common.base.Strings;
import com.tygrash.config.ConsumerConfig;
import com.tygrash.factory.ConsumerFactory;
import com.tygrash.reset.GroupPartition;
import com.tygrash.reset.OffsetResets;
import com.tygrash.reset.OffsetResetter;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Main {

    public static void main(String[] args) throws IOException {
        final String kafkaBrokerAddress = System.getenv("KAFKA_ADDRESS");
        final String filePath = System.getenv("FILE");

        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaBrokerAddress);
        ConsumerFactory consumerFactory = new ConsumerFactory(consumerConfig.getProps());

        OffsetResets offsetResets = new OffsetResets(consumerFactory);

        List<GroupPartition> partitions = Strings.isNullOrEmpty(filePath)
                ? getOffsetResetDetailFromEnvVars(kafkaBrokerAddress)
                : offsetResets.create(Files.lines(Paths.get(filePath))).collect(Collectors.toList());

        new OffsetResetter(partitions, consumerFactory).resetPartitionOffsets();
    }

    private static List<GroupPartition> getOffsetResetDetailFromEnvVars(String kafkaBrokerAddress) {
        String consumerGroup = System.getenv("CONSUMER_GROUP");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");

        Long offset = Long.parseLong(System.getenv("OFFSET"));

        List<GroupPartition> details = new ArrayList<>();
        Integer partitionNumber = !Strings.isNullOrEmpty(System.getenv("PARTITION"))
                ? Integer.parseInt(System.getenv("PARTITION"))
                : null;
        if (partitionNumber == null) {
            ConsumerConfig consumerConfig = new ConsumerConfig(kafkaBrokerAddress, consumerGroup);
            KafkaConsumer consumer = new ConsumerFactory(consumerConfig.getProps()).getConsumer();
            List<PartitionInfo> topicPartitionsInKafka = consumer.partitionsFor(kafkaTopic);
            for (PartitionInfo info : topicPartitionsInKafka) {
                details.add(new GroupPartition(consumerGroup, kafkaTopic, info.partition(), offset));
            }
        } else {
            details.add(new GroupPartition(consumerGroup, kafkaTopic, partitionNumber, offset));
        }

        return details;
    }
}
