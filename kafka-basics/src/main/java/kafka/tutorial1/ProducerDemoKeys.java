package kafka.tutorial1;

import java.util.Properties;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {
  public static void main(String[] args) {

    // set up logger for this class
    final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create producer
    KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

    // send lots of data
    for(int i = 1; i <= 10; i++){


      // create producer record, every gets its own key
      String topic = "first_topic";
      String value = "hello world " + Integer.toString(i);
      String key = "id_" + Integer.toString(i);

      ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,key,value);

      logger.info("key: " + key); // log the key

      // send data - takes type ProducerRecord as input
      // sends data asynchronously
      producer.send(record, new Callback() {
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
          // executes when record is successful sent
          if(e == null){
            // data successfully sent
            logger.info("received meta data:  \n" +
                "Topic: " + recordMetadata.topic() + "\n" +
                "Partition: " + recordMetadata.partition() + "\n" +
                "Offset: " + recordMetadata.offset() + "\n" +
                "Timestamp: " + recordMetadata.timestamp());
          } else {
            // handle error
            logger.error("Error while producing: ", e);
          }
        }
      });

    }

    // wait for all data to be produced then flush it then close it
    producer.flush();
    producer.close();

  }
}
