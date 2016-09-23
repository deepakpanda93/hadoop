package com.koitoer.bd.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Properties;

/**
 * Created by koitoer on 9/23/16.
 */
public class ProducerExample {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(properties);

        Producer<String, String > producer = new Producer<String, String>(config);

        String key1= "first";
        String message1 = "First message";
        String key2= "second";
        String message2 = "Second message";

        String topic = "test";
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, key1, message1);
        producer.send(data);

        data = new KeyedMessage<String, String>(topic, key2, message2);
        producer.send(data);

        producer.close();
    }
}

