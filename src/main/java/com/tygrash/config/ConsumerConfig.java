package com.tygrash.config;

import lombok.Getter;

import java.util.Properties;

@Getter
public class ConsumerConfig {
    private Properties props;

    public ConsumerConfig(String bootstrapServers, String consumerGroup) {
        props = new Properties();
        props.put("group.id", consumerGroup);
        createConfig(bootstrapServers);
    }

    public ConsumerConfig(String bootstrapServers) {
        props = new Properties();
        createConfig(bootstrapServers);
    }

    private void createConfig(String bootstrapServers) {
        props.put("bootstrap.servers", bootstrapServers);
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
    }
}
