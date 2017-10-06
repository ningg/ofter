## Introduction
This library is to reset the kafka offset to the desired value for a combination of kafka-server, consumer-group-id, kafka-topic and partition.

#### Command line parameters required for single partition offset reset are:
* KAFKA_ADDRESS
* CONSUMER_GROUP
* KAFKA_TOPIC
* PARTITION
* OFFSET

##### Run the aplication
To run the application for single partition offset reset, run the command (e.g.)

* For all the partitions of a topic-

```
KAFKA_ADDRESS={kafka_host}:{kafka_port} CONSUMER_GROUP={consumer_group_id} KAFKA_TOPIC={kafka_topic} OFFSET={numeric_offset_to_set} gradle run
```

* For a particular partition of a topic-

```
KAFKA_ADDRESS={kafka_host}:{kafka_port} CONSUMER_GROUP={consumer_group_id} KAFKA_TOPIC={kafka_topic} PARTITION={numeric_partiotion_for_which_offset_to_set} OFFSET={numeric_offset_to_set} gradle run
```

#### Command line parameters required for bulk partition offset reset are:
* KAFKA_ADDRESS
* FILE

##### Run the aplication
To run the application for bulk partition offset reset, run the command (e.g.)

* Sample data inside csv file (for partitions specified):

  `test_group_id,test-topic,9,10`

  `test_group_id,test-topic,10,10`

  `test_group_id,test-topic-1,11,10`

  `test_group_id_1,test-topic-1,12,10`

```
KAFKA_ADDRESS={kafka_host}:{kafka_port} FILE=/Users/XYZ/Downloads/sample.csv gradle run
```

* Sample data inside csv file (for all partitions of a consumer topic):

  `test_group_id,test-topic,10`

  `test_group_id,test-topic-1,10`

```
KAFKA_ADDRESS={kafka_host}:{kafka_port} FILE=/Users/XYZ/Downloads/sample.csv gradle run
```