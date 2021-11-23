package com.venkat.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class AddressProducer {

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_NAME= "ADDRESS_TOPIC";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String, String> addressProducer = new KafkaProducer(properties);
        ProducerRecord<String,String> anAddressRecord = new ProducerRecord<>(TOPIC_NAME,"Helloworld-2");
        addressProducer.send(anAddressRecord);
        addressProducer.flush();
        addressProducer.close();
    }
}
