package com.tygrash.reset;

import com.tygrash.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class OffsetResetterTest {

    private OffsetResetter offsetResetter;

    @Mock
    private GroupPartition groupPartition;

    private List<GroupPartition> partitions;

    @Mock
    private ConsumerFactory consumerFactory;

    @Mock
    private KafkaConsumer consumer;

    @Before
    public void setup() {
        initMocks(this);
        partitions = Arrays.asList(groupPartition);
        offsetResetter = new OffsetResetter(partitions, consumerFactory);
    }

    @Test
    public void testResetPartitionOffset() {
        when(consumerFactory.getConsumer("consumer-group-1")).thenReturn(consumer);
        offsetResetter.resetPartitionOffsets();
    }
}
