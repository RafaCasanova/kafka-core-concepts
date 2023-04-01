package advanced.configurations;

import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaProducer {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Wikimedia Kafka producer!");

        String bootstrapserver = "localhost:19092,localhost:19093,localhost:19094";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapserver);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //configurações de seguraça, geralmente vem pro padrão na verções atuais(verções 3+)
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");// -1 = all
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));

        //compressão e tamanhado de dados maiores
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");//delay de 20 milissegundos
        //aumentando o tamanho de por padrão e 16 kbytes, estamos colocando para 32 kbytes
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));//obs: numero total em bytes
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");

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