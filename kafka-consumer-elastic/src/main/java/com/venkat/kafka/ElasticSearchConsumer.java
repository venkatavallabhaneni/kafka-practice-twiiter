package com.venkat.kafka;

import com.google.gson.JsonParser;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
    public static final String TOPIC_NAME = "twitter-tweets-topic7";
    public static final String GROUP_NAME = "twitter-tweets-venkat-group4";
    public static final String OFFSET_POLICY = "earliest";

    private static JsonParser gson = null;

    public static void main(String[] args) throws IOException {

        gson = new JsonParser();

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        RestHighLevelClient client = createElkClient();

        BulkRequest bulkRequest = new BulkRequest();

        while (true) { // Fix this, temporary
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));

            logger.info("3####  Records recieved count :: "+consumerRecords.count());

            consumerRecords.forEach(aConsumerRecord -> {
                        logger.info("####   Record Value " + aConsumerRecord.value());


                        putIntoElasticDb(client, bulkRequest,aConsumerRecord.value());
                    }
            );

            BulkResponse bulkResponse = client.bulk(bulkRequest,RequestOptions.DEFAULT);
            logger.info("Committing offsets .......");
            kafkaConsumer.commitSync();
            logger.info(" offsets has been committed .......");
        }

    }

    private static void putIntoElasticDb(RestHighLevelClient client, BulkRequest bulkRequest, String value) {

        if (value == null || value == "") {
            return;
        }

        Long id = gson.parse(value).getAsJsonObject().get("tweetId").getAsLong();
        IndexRequest indexRequest = new IndexRequest("twitter-venkat").id(Long.toString(id)).source(value, XContentType.JSON);
        bulkRequest.add(indexRequest);

    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_NAME);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_POLICY);
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"50");

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