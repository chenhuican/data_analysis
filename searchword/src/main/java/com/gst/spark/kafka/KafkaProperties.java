package com.gst.spark.kafka;

public class KafkaProperties {
    public static final String ZK = "hadoop.node1:2181,hadoop.node2:2181,hadoop.node3:2181";
    public static final String TOPIC = "test-topic";

    public static final String BROKER_LIST = "hadoop.node1:9092,hadoop.node2:9092,hadoop.node3:9092";

    public static final String GROUP_ID = "test_group1";

}
