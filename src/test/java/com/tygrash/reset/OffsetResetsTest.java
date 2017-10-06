package com.tygrash.reset;

import com.tygrash.factory.ConsumerFactory;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.initMocks;

public class OffsetResetsTest {

    @Mock
    private ConsumerFactory consumerFactory;

    @Mock
    private KafkaConsumer consumer;

    @Before
    public void setup() {
        initMocks(this);
    }

    @Test
    public void shouldParseOneLineWithThreeParametersToResetDetail() {
        OffsetResets offsetResets = new OffsetResets(consumerFactory);

        when(consumerFactory.getConsumer("ConsumerGrp1")).thenReturn(consumer);
        PartitionInfo info = new PartitionInfo("topic2", 1, null, null, null); // SUPPRESS CHECKSTYLE
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(info));
        List<GroupPartition> offsetResetDetails = offsetResets.create(
                Arrays.asList("ConsumerGrp1,topic2,11222").stream()).collect(Collectors.toList());
        assertOffsetResetDetail(offsetResetDetails.size(), 1,
                offsetResetDetails.get(0).getConsumerGroup(), "ConsumerGrp1",
                offsetResetDetails.get(0).getKafkaTopic(), "topic2",
                offsetResetDetails.get(0).getPartitionNumber(), new Integer(1), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getNewOffset(), new Long(11222)); // SUPPRESS CHECKSTYLE
    }

    @Test
    public void shouldParseTwoLineWithThreeParametersToResetDetail() {
        OffsetResets offsetResets = new OffsetResets(consumerFactory);

        when(consumerFactory.getConsumer("ConsumerGrp1")).thenReturn(consumer);
        PartitionInfo info1 = new PartitionInfo("topic2", 1, null, null, null); // SUPPRESS CHECKSTYLE
        PartitionInfo info2 = new PartitionInfo("topic2", 2, null, null, null); // SUPPRESS CHECKSTYLE
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(info1, info2));
        List<GroupPartition> offsetResetDetails = offsetResets.create(
                Arrays.asList("ConsumerGrp1,topic1,11222", "ConsumerGrp1,topic2,11223").stream())
                .collect(Collectors.toList());
        assertOffsetResetDetail(offsetResetDetails.size(), 2, // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getConsumerGroup(), "ConsumerGrp1", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getKafkaTopic(), "topic2", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getPartitionNumber(), new Integer(1), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getNewOffset(), new Long(11223)); // SUPPRESS CHECKSTYLE
        assertOffsetResetDetail(offsetResetDetails.size(), 2, // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getConsumerGroup(), "ConsumerGrp1", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getKafkaTopic(), "topic2", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getPartitionNumber(), new Integer(2), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getNewOffset(), new Long(11223)); // SUPPRESS CHECKSTYLE
    }

    @Test
    public void shouldParseOneLineWithFourParametersToResetDetail() {
        OffsetResets offsetResets = new OffsetResets(consumerFactory);

        when(consumerFactory.getConsumer("ConsumerGrp1")).thenReturn(consumer);
        PartitionInfo info = new PartitionInfo("topic2", 2, null, null, null); // SUPPRESS CHECKSTYLE
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(info));
        List<GroupPartition> offsetResetDetails = offsetResets.create(
                Arrays.asList("ConsumerGrp1,topic2,2,11222").stream()).collect(Collectors.toList());
        assertOffsetResetDetail(offsetResetDetails.size(), 1, // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getConsumerGroup(), "ConsumerGrp1", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getKafkaTopic(), "topic2", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getPartitionNumber(), new Integer(2), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getNewOffset(), new Long(11222)); // SUPPRESS CHECKSTYLE
    }

    @Test
    public void shouldParseTwoLineWithFourParametersToResetDetail() {
        OffsetResets offsetResets = new OffsetResets(consumerFactory);

        when(consumerFactory.getConsumer("ConsumerGrp1")).thenReturn(consumer);
        PartitionInfo info1 = new PartitionInfo("topic2", 1, null, null, null); // SUPPRESS CHECKSTYLE
        PartitionInfo info2 = new PartitionInfo("topic2", 2, null, null, null); // SUPPRESS CHECKSTYLE
        when(consumer.partitionsFor("topic2")).thenReturn(Arrays.asList(info1, info2));
        List<GroupPartition> offsetResetDetails = offsetResets.create(
                Arrays.asList("ConsumerGrp1,topic2,1,11222", "ConsumerGrp1,topic2,2,11223").stream())
                .collect(Collectors.toList());
        assertOffsetResetDetail(offsetResetDetails.size(), 2, // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getConsumerGroup(), "ConsumerGrp1", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getKafkaTopic(), "topic2", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getPartitionNumber(), new Integer(1), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(0).getNewOffset(), new Long(11222)); // SUPPRESS CHECKSTYLE
        assertOffsetResetDetail(offsetResetDetails.size(), 2, // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getConsumerGroup(), "ConsumerGrp1", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getKafkaTopic(), "topic2", // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getPartitionNumber(), new Integer(2), // SUPPRESS CHECKSTYLE
                offsetResetDetails.get(1).getNewOffset(), new Long(11223)); // SUPPRESS CHECKSTYLE
    }

    private void assertOffsetResetDetail(int size, int actualSize, String consumerGroup, String actualConsumerGroup, // SUPPRESS CHECKSTYLE
                                         String kafkaTopic, String actualTopic, Integer partitionNo, Integer actualPartitionNo,
                                         Long offset, Long actualOffset) {
        assertEquals(size, actualSize);
        assertEquals(consumerGroup, actualConsumerGroup);
        assertEquals(kafkaTopic, actualTopic);
        assertEquals(partitionNo, actualPartitionNo);
        assertEquals(offset, actualOffset);
    }
}
