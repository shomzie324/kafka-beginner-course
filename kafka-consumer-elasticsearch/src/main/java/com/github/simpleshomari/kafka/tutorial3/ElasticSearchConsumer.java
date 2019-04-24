package com.github.simpleshomari.kafka.tutorial3;

import com.google.gson.JsonParser;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ElasticSearchConsumer {

  public static RestHighLevelClient createClient(){

    // repalce with your own credentials
    String hostname = "";
    String username = "";
    String password = "";

    // used for cloud connection
    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,
        password));

    RestClientBuilder builder = RestClient.builder(new HttpHost(hostname, 443, "https"))
    .setHttpClientConfigCallback(new HttpClientConfigCallback() {
      @Override
      public HttpAsyncClientBuilder customizeHttpClient(
          HttpAsyncClientBuilder httpClientBuilder) {
        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
      }
    });

    RestHighLevelClient client = new RestHighLevelClient(builder);
    return client;
  }

  public static KafkaConsumer<String,String> createConsumer(String topic){
    // set up logger
    final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    // set up property values
    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "kafka-demo-elasticsearch";

    // set up properties
    Properties properties = new Properties();
    properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class
        .getName());
    properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
    // auto reset offset to
    // earliest: always back to beginning
    // latest: always from new messages onwards
    properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // manually commit offsets to avoid potential duplicate errors cause by auto commit + async code
    properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

    // create consumer and subscribe it to topics
    KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
    consumer.subscribe(Arrays.asList(topic));

    return consumer;
  }

  private static JsonParser jsonParser = new JsonParser();
  private static String extractIdFromTweet(String tweetJson){
    // use gson library
    return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
  }

  public static void main(String[] args) throws IOException {
    Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    // get client to elastic search
    RestHighLevelClient client = createClient();

    // get a consumer
    KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");

    // poll for new data
    // consumer doesn't get data until it asks
    while(true){
      ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100)); // old
      // version took long, new kafka takes duration object

      // examine records
      Integer recordCount = records.count();
      logger.info("Received " + recordCount + " records");

      // set up bulk request ofr batching instead of inserting one by one
      BulkRequest bulkRequest = new BulkRequest();

      for(ConsumerRecord<String,String> record: records){
        // insert data into elastic search

        String tweet = record.value(); // this is the tweet

        try{
          // 2 ways to set up idempotence - make sure same id used to avoid duplicate inserts
          // 1. kafka generic id
          //String id = record.topic() + "_" + record.partition() + "_" + record.offset();
          // 2. twitter specific id
          String tweetId = extractIdFromTweet(record.value());

          // set up request for insertion
          // provide id to make consumer idempotent
          IndexRequest indexRequest = new IndexRequest("twitter","tweets", tweetId)
              .source(tweet, XContentType.JSON);

          // add request to batch of requests
          bulkRequest.add(indexRequest);
        } catch (NullPointerException e ){
          logger.warn("skipping bad data: " + record.value());
        }
      }

      if(recordCount > 0){
        // send batch of requests together
        BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);

        logger.info("Comitting the offsets...");
        consumer.commitSync();
        logger.info("Offsets have been committed");

        try {
          Thread.sleep(1000); // add small delay for testing
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }

    }

    // close client gracefully
    //client.close();
  }
}
