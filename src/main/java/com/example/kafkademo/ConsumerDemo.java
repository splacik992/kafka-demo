package com.example.kafkademo;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class);

    public static void main(String[] args) {

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7GvIMXPt69DoANrlV66r9l\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3R3ZJTVhQdDY5RG9BTnJsVjY2cjlsIiwib3JnYW5pemF0aW9uSWQiOjcxNjU1LCJ1c2VySWQiOjgzMDk4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI0NGFmNmMyZS05ZjEzLTQxZDctODcxOS1mZTM1YmE5OThiOTUifX0.5dD4LO7Qs_IcTH-dm5O9I2EsEhx1zHrbA2f5IsYkz5k\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        //offset reset - none/earliest/latest
        properties.setProperty("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        final Thread mainThread = Thread.currentThread();

        //adding the shutdown hack
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown , let's exit by calling consumer.wakeup()....");
                consumer.wakeup();

                // join a main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        try {
            while (true) {
                //poll for data
                log.info("Polling");
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());

                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down");
        }catch (Exception e) {
            log.error("Unexpected error in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("The consumer is now gracefully shut down");
        }
    }
}
