package org.example;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class App {

    private static String topicName = "test";

    public static void main(String[] args) {

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", "localhost:9093");

        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = new KafkaProducer<>(conf);

        int key = 1;
        String message = "Hello world!! this message was from Java application";
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topicName, key, message);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (metadata != null) {
                    System.out.printf("Success sending message, partition [%d], offset [%d]", metadata.partition(), metadata.offset());
                } else {
                    System.out.printf("Failed to send message: %s", exception.getMessage());
                }
            }
        });

        producer.close();

    }
}
