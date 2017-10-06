package com.tygrash.factory;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumerFactory {

    private Properties properties;

    public ConsumerFactory(Properties properties) {
        this.properties = properties;
    }

    public KafkaConsumer getConsumer() {
        return new KafkaConsumer<>(properties);
    }

    public KafkaConsumer getConsumer(String consumerGroupId) {
        this.properties.setProperty("group.id", consumerGroupId);
        return new KafkaConsumer<>(properties);
    }
}
