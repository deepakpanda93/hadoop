package com.koitoer.bd.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * Created by koitoer on 9/23/16.
 */
public class BasicProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.0.22:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String key1= "first";
        String message1 = "First message";
        String key2= "second";
        String message2 = "Second message from Intellijidea";

        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("test", key1, message1);
        producer.send(record);
        record = new ProducerRecord<String, String>("test", key2, message2);
        producer.send(record);
        record = new ProducerRecord<String, String>("test", message1);
        producer.send(record);
        producer.close();


    }
}
