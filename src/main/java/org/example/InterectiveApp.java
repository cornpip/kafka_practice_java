package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;
import java.util.concurrent.Future;

public class InterectiveApp {
    public static void main(String args[]) throws IOException {

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        String message;

        try ( KafkaStringMessageSender kafkaSender = new KafkaStringMessageSender("localhost", 9092) ) {

            while(true) {
                message = br.readLine();
                kafkaSender.sendMessage("MSG_TOPIC", message);

                if ( "exit".equals(message) ) {
                    break;
                }
            }

        } catch(Exception e) {
            System.err.println(e.getMessage());
        }

    }
}

class KafkaStringMessageSender implements AutoCloseable {

    private Producer<Integer, String> producer;
    private int key = 0;

    public KafkaStringMessageSender(String brokerIp, int brokerPort) {

        Properties conf = new Properties();
        conf.setProperty("bootstrap.servers", String.format("%s:%d", brokerIp, brokerPort));
        conf.setProperty("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        conf.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(conf);

    }

    public Future<RecordMetadata> sendMessage(String topic, String message) {
        key++;
        ProducerRecord<Integer, String> record = new ProducerRecord<Integer,String>(topic, key, message);

        return this.producer.send(record);
    }

    @Override
    public void close() throws Exception {
        this.producer.close();
    }

}