package com.tygrash.reset;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.initMocks;

public class GroupPartitionTest {
    private GroupPartition groupPartition;

    @Mock
    private KafkaConsumer consumer;
    private int expectedPartitionNumber = 10;
    private long expectedOffset = 100L;

    @Before
    public void setup() {
        initMocks(this);
        groupPartition = new GroupPartition("consumer-group-1", "topic-1", expectedPartitionNumber, expectedOffset);
    }

    @Test
    public void checkResettingOffsets() {
        PartitionInfo info = new PartitionInfo("topic-1", expectedPartitionNumber, null, null, null);

        when(consumer.partitionsFor(groupPartition.getKafkaTopic())).thenReturn(Arrays.asList(info));

        groupPartition.doReset(consumer);

        HashMap<TopicPartition, OffsetAndMetadata> expectedPartitionMap = new HashMap<TopicPartition, OffsetAndMetadata>() {{
            put(new TopicPartition("topic-1", expectedPartitionNumber), new OffsetAndMetadata(expectedOffset));
        }};

        verify(consumer).commitSync(expectedPartitionMap);
    }

    @Test
    public void checkResettingOffsetsWhenPartitionNotThere() {
        int nonExistentPartitionNumber = 1;
        PartitionInfo info = new PartitionInfo("topic-1", nonExistentPartitionNumber, null, null, null);

        when(consumer.partitionsFor(groupPartition.getKafkaTopic())).thenReturn(Arrays.asList(info));

        groupPartition.doReset(consumer);

        verify(consumer, never()).commitSync(any());
    }
}
