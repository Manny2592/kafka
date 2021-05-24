import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //Set the producer
        Properties properties=new Properties();

        //Creating the producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Creating the producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);

        //Create the producer record
        ProducerRecord<String,String> record=new ProducerRecord<String,String>("first_topic","Hello World");


        //send data
        producer.send(record);

        producer.flush();

    }
}