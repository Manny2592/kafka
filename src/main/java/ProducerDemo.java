import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemo {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger loggerFactory=LoggerFactory.getLogger(ProducerDemo.class);
        //Set the producer
        Properties properties=new Properties();


        //Creating the producer properties
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Creating the producer
        KafkaProducer<String,String> producer= new KafkaProducer<String, String>(properties);


        for(int i=0;i<=10;i++){
            //Create the producer record

            String key="id"+Integer.toString(i);
            ProducerRecord<String,String> record=new ProducerRecord<String,String>("first_topic",key,"Heylo World"+i);

            loggerFactory.info("key"+key);
            //send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //executes everytime a record is successfully sent or an exception is throw
                    if(e==null){
                        loggerFactory.info("\n"+"Received new metadata Topic "+
                                recordMetadata.topic()+ "\n"+
                                "Partition "+ recordMetadata.partition() +"\n"
                                +" Offset"+recordMetadata.offset()+ "\n"+
                                "TimeStamp"+recordMetadata.timestamp()
                        );
                    }   else {
                        e.printStackTrace();
                    }


                }
            });

        }

        producer.flush();




    }
}
