package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TwitterProducer {
  Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
  String consumerKey = "";
  String consumerSecret = "";
  String token = "";
  String tokenSecret = "";
  List<String> terms = Lists.newArrayList("bitcoin", "usa", "politics");

  public TwitterProducer(){}

  public static void main(String[] args) {
    new TwitterProducer().run();
  }

  public void run(){
    logger.info("setup");
    // set up msg queue for twitter client
    /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100000);

    // create twitter client
    Client client = createTwitterClient(msgQueue);

    // Attempts to establish a connection.
    client.connect();

    // create kafka producer
    KafkaProducer<String,String> producer = createKafkaProducer();

    // add shutdown hook for cleanup
    Runtime.getRuntime().addShutdownHook(new Thread(
        () -> {
          logger.info("stopping application...");
          logger.info("shutting down client from twitter...");
          client.stop();
          logger.info("closing producer...");
          producer.close();
          logger.info("Done!");
        }
    ));

    // loop to send tweets to kafka
    // on a different thread, or multiple different threads....
    while (!client.isDone()) {
      String msg = null;
      try {
        msg = msgQueue.poll(5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
        client.stop();
      }

      if(msg != null){
        logger.info(msg);
        // send rend and register a callback
        producer.send(new ProducerRecord<String, String>("twitter_tweets", null, msg),
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e != null){
                  logger.error("something went wrong: ", e);
                }
              }
            });
      }
    }
    logger.info("end of application");
  }


  public Client createTwitterClient( BlockingQueue<String> msgQueue){

    /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
    Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
    // Optional: set up some followings and track terms
    List<Long> followings = Lists.newArrayList(1234L, 566788L);
    hosebirdEndpoint.trackTerms(terms);

    // These secrets should be read from a config file
    Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

    ClientBuilder builder = new ClientBuilder()
        .name("Hosebird-Client-01")                              // optional: mainly for the logs
        .hosts(hosebirdHosts)
        .authentication(hosebirdAuth)
        .endpoint(hosebirdEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    Client hosebirdClient = builder.build();
    return hosebirdClient;
  }

  public KafkaProducer<String, String> createKafkaProducer(){
    // create producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    // create safe producer
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true"); // does below by
    // default
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
    //kafka 2.0 > 1.1 so this can stay at 5 without impacting throughput negativley. Use 1 otherwise
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

    // add high throughput settings (at expense of a bit of cpu usage and latency)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); //32kb

    // create producer
    KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

    return producer;
  }
}
