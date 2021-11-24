package com.venkat.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class AddressConsumerGroups {

    public static Logger logger = LoggerFactory.getLogger(AddressConsumerGroups.class.getName());

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_NAME = "ADDRESS_TOPIC_3";
    public static final String KEY_PREFIX = "KEY_";
    public static final String GROUP_NAME = "kafka-practice-address-1";
    public static final String OFFSET_POLICY = "earliest";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_POLICY);

        KafkaConsumer<String, String> addressConsumer = new KafkaConsumer<>(properties);
        addressConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        while (true) { // Fix this, temporary
            ConsumerRecords<String, String> consumerRecords = addressConsumer.poll(Duration.ofMillis(100));
            consumerRecords.forEach(aConsumerRecord -> logger.info("Key : " + aConsumerRecord.key() + " Value : " + aConsumerRecord.value()
                    + " Partition : " + aConsumerRecord.partition())
            );
        }


    }
}
