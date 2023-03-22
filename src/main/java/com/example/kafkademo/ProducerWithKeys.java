package com.example.kafkademo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

    public static void main(String[] args) {
        log.info("Hello World");

        //connect to Conduktor Playground
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"7GvIMXPt69DoANrlV66r9l\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI3R3ZJTVhQdDY5RG9BTnJsVjY2cjlsIiwib3JnYW5pemF0aW9uSWQiOjcxNjU1LCJ1c2VySWQiOjgzMDk4LCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI0NGFmNmMyZS05ZjEzLTQxZDctODcxOS1mZTM1YmE5OThiOTUifX0.5dD4LO7Qs_IcTH-dm5O9I2EsEhx1zHrbA2f5IsYkz5k\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        //set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 30; i++) {
            String key = "key" + i;
            String value = "value" + i;

            //create a Producer Record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>("demo_java", key, value);

            //send data
            producer.send(producerRecord, (recordMetadata, e) -> {
                //executed everytime a record sucessfully sent or an excception thrown
                if (e == null) {
                    //the record sucessfully sent
                    log.info("Key : " + key + " | " + "Partition : " + recordMetadata.partition());
                } else {
                    log.error("Error while sending message: " + e);
                }
            });

        }

        //tell producer to send all data and block until done - sync
        producer.flush();
        //flush and close
        producer.close();
    }
}
