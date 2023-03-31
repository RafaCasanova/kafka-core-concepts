package org.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SimpleProducer {
    public static void main(String args[]){

        System.out.println("Kafka simple producer");

        String bootstrapserver = "localhost:19092,localhost:19093,localhost:19094";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapserver);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);

        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("simple-topic","envio de uma mensagem pro um simples producer em kafka");
        System.out.println("enviado mensagem para o topico " +producerRecord.topic());
        producer.send(producerRecord);
        producer.close();

    }
}
