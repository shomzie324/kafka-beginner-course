package com.github.simpleshomari.kafka.tutorial4;

import com.google.gson.JsonParser;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class StreamsFilterTweets {
  public static void main(String[] args) {

    // create properties
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
    // app id similar to consumer group id but for streams api
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"demo-kafka-streams");
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class
        .getName()); // how to serialize and deserialize keys
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class
        .getName()); // how to serialize and deserialize values

    // create a topology with input stream from topic, transformations and output topic
    StreamsBuilder streamsBuilder = new StreamsBuilder();

    // define input topic
    KStream<String,String> inputTopic =  streamsBuilder.stream("twitter_tweets");

    // apply operations to the kstream and get back modified kstreams
    // tweets are formatted as text/json so gson lib needed
    KStream<String,String> filteredStream = inputTopic.filter(
        // filter for tweets with a user that has over 10,000 followers
        (k,jsonTweet) ->  extractUserFollowersFromTweet(jsonTweet) > 10000
    );

    // send filtered stream data to new topic
    filteredStream.to("important_tweets");

    // build topology with the stream builder and properties
    KafkaStreams kafkaStreams = new KafkaStreams(
        streamsBuilder.build(),
        properties
    );

    // start streams app
    kafkaStreams.start();
  }

  private static JsonParser jsonParser = new JsonParser();
  private static Integer extractUserFollowersFromTweet(String tweetJson){
    // use gson library
    try{
      return jsonParser.parse(tweetJson).getAsJsonObject()
          .get("user").getAsJsonObject()
          .get("followers_count").getAsInt();
    } catch (NullPointerException e){
      return 0;
    }
  }

}
