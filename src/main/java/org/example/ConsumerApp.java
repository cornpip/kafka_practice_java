package org.example;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class ConsumerApp {
    public static void main(String args[]) throws IOException {

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "localhost:9092");
        conf.setProperty("group.id", "ConsumerGroup1");
        conf.setProperty("enable.auto.commit", "false");
        conf.setProperty("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        conf.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try(Consumer<Integer, String> consumer = new KafkaConsumer<>(conf)) {

            consumer.subscribe(Collections.singletonList("MSG_TOPIC"));

            while(true) {

                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(1));

                records.forEach(record -> {
                    System.out.printf("Pulled message, key [%d] message [%s] \n", record.key(), record.value());

                    TopicPartition tp = new TopicPartition(record.topic(), record.partition());
                    OffsetAndMetadata om = new OffsetAndMetadata(record.offset() + 1);
                    Map<TopicPartition, OffsetAndMetadata> commitObjMap = Collections.singletonMap(tp, om);

                    consumer.commitSync(commitObjMap);
                });

                try {
                    Thread.sleep(500);
                } catch (Exception e) {
                }
            }

        } catch (Exception e) {
            System.err.println(e.getMessage());
        };

    }
}