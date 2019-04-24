package kafka.tutorial1;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoAssignSeek {
  public static void main(String[] args) {

    // set up logger
    final Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class);

    // set up properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    // auto reset offset to
    // earliest: always back to beginning
    // latest: always from new messages onwards
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create consumer
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

    // assign and seek are mostly used to replay data
    // or fetch a specific message

    // asign
    TopicPartition partitionToReadFrom = new TopicPartition("first_topic", 0);
    consumer.assign(Arrays.asList(partitionToReadFrom));

    // seek
    long offset = 15L;
    consumer.seek(partitionToReadFrom, offset);

    // poll for new data
    // consumer doesn't get data until it asks
    int numToRead = 5;
    boolean keepReading = true;
    int numRead = 0;

    while(keepReading){
      ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // old
      // version took long, new kafka takes duration object

      // examine records
      for(ConsumerRecord<String,String> record: records){
        numRead += 1;
        logger.info("key: " + record.key() + ", Value: " + record.value());
        logger.info(", Partition: " + record.partition() + ", Offset: " + record.offset());
        if(numRead >= numToRead){
          keepReading = false;
          break;
        }
      }

    }

  }
}
