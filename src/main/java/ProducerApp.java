import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private int counter=0;
    private static String KAFKA_BROKER_URL ="localhost:9092";
    private static String TOPIC_NAME ="test4";
    private static String clientID ="client_prod_1";

    public ProducerApp(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA_BROKER_URL);
        properties.put("client.id", clientID);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(()->{

            String key=String.valueOf(++counter);
            String value = String.valueOf(Math.random()*1000);
            producer.send(new ProducerRecord<Integer, String>(TOPIC_NAME,counter,value),
                    (metadata,ex)->{
                        System.out.println("Sending Message key=>"
                                +counter
                                +" Value =>"
                                +value);
                        System.out.println("Partition => "+metadata.partition()+" Offset=>"+metadata.offset());
                    });
        },1000,1000, TimeUnit.MILLISECONDS);
    }
    public static void main(String[] args) {
            new ProducerApp();
    }
}
