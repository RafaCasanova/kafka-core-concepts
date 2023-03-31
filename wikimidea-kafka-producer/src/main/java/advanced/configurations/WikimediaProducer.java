package advanced.configurations;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Objects;
import java.util.Properties;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Wikimedia Kafka producer!");

        String bootstrapserver = "localhost:19092,localhost:19093,localhost:19094";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",bootstrapserver);
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);



        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(URI.create(url));
        EventSource eventSource = builder.build();

        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                System.out.println("Detected a shutdown, close producer and eventSource");
                try{
                    mainThread.join();
                }catch (Exception e){
                    System.out.println(e.getLocalizedMessage());
                }finally {
                    producer.close();
                    eventSource.close();
                }
            }
        });


        eventSource.messages().forEach(msg -> {

            ProducerRecord<String,String> producerRecord = new ProducerRecord<>("wikimedia",msg.getData());

            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(Objects.equals(e,null)){
                        System.out.println("enviado mensagem para o topico: " +recordMetadata.topic() + "\n" +
                                "Partição :" + recordMetadata.partition() + "\n" +
                                "Offsert: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp() + "\n"+
                                "Data :" + msg.getData()
                        );
                    }else{
                        System.out.println(e.getLocalizedMessage());
                    }
                }
            });
        });



    }
}