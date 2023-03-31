package org.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class SimpleConsumer {
    public static void main (String args[]){

        System.out.println("Kafka simples consumer");

        String bootstrapserver = "localhost:19092,localhost:19093,localhost:19094";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapserver);
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer",StringDeserializer.class.getName());
        properties.setProperty("group.id","java-app");
        //none não existe consumer group antes de começar a aplicação
        //earliest le desde a primeira mensagem
        //latest ler apartir de ultima mensagem
        properties.setProperty("auto.offset.reset","earliest");

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList("simple-topic"));

        while (true){
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                System.out.println("Key :"+ record.key() +" - " + "Value : " + record.value());
                System.out.println("Partition :"+ record.partition() +" - " + "Offset : " + record.offset());
            }

        }
    }
}
