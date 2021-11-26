package com.venkat.kafka;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsDemo {

    private static JsonParser gson;

    public static void main(String[] args) {

        gson = new JsonParser();

        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "streams-demo");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        StreamsBuilder streamsBuilder = new StreamsBuilder();


        KStream<String, String> inputTopicStream = streamsBuilder.stream("twitter-tweets-topic-new");
        KStream<String, String> filteredStream = inputTopicStream.filter((k, jsonTweetMessage) -> extractFollowers(jsonTweetMessage) > 1000);
        filteredStream.to("important-tweets-topic");

        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);
        kafkaStreams.start();


    }

    private static Integer extractFollowers(String messageJson) {

        return gson.parse(messageJson).getAsJsonObject().get("followers").getAsInt();
    }


}
