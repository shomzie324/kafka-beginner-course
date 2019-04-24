package kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {
  public static void main(String[] args) {

    // set up logger
    final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);

    // set up properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fourth-application");
    // auto reset offset to
    // earliest: always back to beginning
    // latest: always from new messages onwards
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

    // subscribe consumer to topics
    // Collections.singleton - subscribe to one topic
    // Arrays.asList("topic1", "topic2")
    consumer.subscribe(Collections.singleton("first_topic"));

    // poll for new data
    // consumer doesn't get data until it asks
    while(true){
      ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // old
      // version took long, new kafka takes duration object

      // examine records
      for(ConsumerRecord<String,String> record: records){
        logger.info("key: " + record.key() + ", Value: " + record.value());
        logger.info(", Partition: " + record.partition() + ", Offset: " + record.offset());
      }

    }

  }
}
