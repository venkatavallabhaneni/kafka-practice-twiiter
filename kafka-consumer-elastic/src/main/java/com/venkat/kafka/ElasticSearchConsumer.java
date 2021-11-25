package com.venkat.kafka;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumer {


    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_NAME = "twitter-tweets-topic5";
    public static final String GROUP_NAME = "twitter-tweets-venkat-group4";
    public static final String OFFSET_POLICY = "earliest";

    public static void main(String[] args) {

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        RestHighLevelClient client = createElkClient();

        while (true) { // Fix this, temporary
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));


            consumerRecords.forEach(aConsumerRecord -> {
                        logger.info("####   Record Value " + aConsumerRecord.value());
                        putIntoElasticDb(client, aConsumerRecord.value());
                    }
            );
        }

    }

    private static void putIntoElasticDb(RestHighLevelClient client, String value) {
        IndexRequest indexRequest = new IndexRequest("twitter-venkat").source(value, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            logger.error("Message", e);
        }
        logger.info("Index created  Id : " + indexResponse.getId() + " status " + indexResponse.status());

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_POLICY);

        KafkaConsumer<String, String> stringKafkaConsumer = new KafkaConsumer<>(properties);
        stringKafkaConsumer.subscribe(Arrays.asList(TOPIC_NAME));

        return stringKafkaConsumer;
    }

    private static RestHighLevelClient createElkClient() {

        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(AppPropertyReader.getValue("elastic.username"), AppPropertyReader.getValue("elastic.password")));
        RestClientBuilder clientBuilder = RestClient.builder(new HttpHost(AppPropertyReader.getValue("elastic.host"), 443, "https")).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            }
        });

        return new RestHighLevelClient(clientBuilder);

    }
}