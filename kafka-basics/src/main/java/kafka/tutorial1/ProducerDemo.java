package kafka.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerDemo {
  public static void main(String[] args) {

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

    // create producer record
    ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic",
        "hello world");

    // send data - takes type ProducerRecord as input
    // sends data asynchronously
    producer.send(record);

    // wait for all data to be produced then flush it then close it
    producer.flush();
    producer.close();

  }
}
