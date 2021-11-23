package com.venkat.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class AddressProducer {

    public static Logger logger = LoggerFactory.getLogger(AddressProducer.class.getName());

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_NAME = "ADDRESS_TOPIC_3";
    public static final String KEY_PREFIX="KEY_";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> addressProducer = new KafkaProducer<>(properties);

        for (Integer repetition = 0; repetition <= 20; repetition++) {
            produce(addressProducer, repetition);
        }
        addressProducer.flush();
        addressProducer.close();
    }

    private static void produce(KafkaProducer<String, String> addressProducer, Integer repetition) {


        ProducerRecord<String, String> anAddressRecord = new ProducerRecord<>(TOPIC_NAME, KEY_PREFIX+repetition, "Hello world-" + repetition);

        addressProducer.send(anAddressRecord, (metadata, exception) -> {

            if (exception == null) {
                logger.info("Received new MetaData :: Topic :" + metadata.topic() + " Partition : " + metadata.partition() + " Offset : " + metadata.offset()
                        + " Time stamp : " + metadata.timestamp());
            } else {
                logger.info("Error while producing message ");
            }

        });
    }
}
