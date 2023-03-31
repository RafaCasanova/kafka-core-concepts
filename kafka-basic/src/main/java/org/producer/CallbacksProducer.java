package org.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Objects;
import java.util.Properties;

public class CallbacksProducer {
    public static void main(String args[]){

        System.out.println("Kafka producer with callbacks");

        String bootstrapserver = "localhost:19092,localhost:19093,localhost:19094";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapserver);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);


        for(int i =0;i<100;i++){
            for(int j =0;j<100;j++){
                ProducerRecord<String,String> producerRecord = new ProducerRecord<>("simple-topic",Integer.toString(i)+Integer.toString(j),"envio de uma mensagem pro um simples producer em kafka");
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(Objects.equals(e,null)){
                            System.out.println("enviado mensagem para o topico: " +recordMetadata.topic() + "\n" +
                                    "Partição :" + recordMetadata.partition() + "\n" +
                                    "Offsert: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp()
                            );
                        }else{
                            System.out.println(e.getLocalizedMessage());
                        }
                    }
                });
            }
        }

        producer.close();

    }
}
