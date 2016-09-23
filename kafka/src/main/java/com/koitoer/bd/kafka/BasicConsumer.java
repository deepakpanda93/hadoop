package com.koitoer.bd.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.KafkaStreams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by koitoer on 9/23/16.
 */
public class BasicConsumer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("zookeeper.connect", "localhost:2181");
        properties.put("group.id", "mygroup");
        ConsumerConfig config = new ConsumerConfig(properties);

        ConsumerConnector consumerConnector = kafka.consumer.Consumer.createJavaConsumerConnector(config);

        String topic = "test";
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]> streams = consumerMap.get(topic);

        for(final KafkaStream stream : streams){
            ConsumerIterator <byte[], byte[]> it = stream.iterator();
            while(it.hasNext()){
                System.out.println(new String(it.next().message()));
            }
        }

        consumerConnector.shutdown();
     }
}
