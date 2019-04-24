package kafka.tutorial1;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoWithThreads {
  public static void main(String[] args) {
    new ConsumerDemoWithThreads().run();
  }

  private ConsumerDemoWithThreads(){}

  private void run(){
    final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    // latch for dealing for multiple threads
    CountDownLatch latch = new CountDownLatch(1);

    // create runnable needed for thread
    logger.info("creating consumer thread...");
    Runnable myConsumerRunnable = new ConsumerRunnable("first_topic", latch);

    // start thread
    Thread myThread = new Thread(myConsumerRunnable);
    myThread.start();


    // add a shutdown hook
    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> {
          logger.info("caught shutdown hook");
          ((ConsumerRunnable) myConsumerRunnable).shutdown();
          try {
            latch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
    ));

    try {
      latch.await();
    } catch (InterruptedException e) {
      logger.error("Application got interrupted ",e);
    } finally{
      logger.info("application is closing...");
    }
  }

  public class ConsumerRunnable implements Runnable{

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;
    // set up logger
    private final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    public ConsumerRunnable(String topic, CountDownLatch latch){
      this.latch = latch;

      // set up properties
      Properties properties = new Properties();
      properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
      properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
          .getName());
      properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
          .getName());
      properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-fifth-application");
      // auto reset offset to
      // earliest: always back to beginning
      // latest: always from new messages onwards
      properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


      // create consumer
      consumer = new KafkaConsumer<String, String>(properties);

      // subscribe consumer to topics
      // Collections.singleton - subscribe to one topic
      // Arrays.asList("topic1", "topic2")
      consumer.subscribe(Collections.singleton("first_topic"));
    }

    @Override
    public void run() {
      try{
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
      } catch (Exception e){
        logger.info("Received shut down signal");
      } finally{
        consumer.close();
        // tell main we are done with consumer
        latch.countDown();
      }
    }

    public void shutdown(){
      // special method to interrupt consumer.poll()
      // it will throw wake up exception
      this.consumer.wakeup();
    }
  }

}
