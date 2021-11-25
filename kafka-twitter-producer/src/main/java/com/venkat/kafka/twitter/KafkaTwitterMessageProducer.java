package com.venkat.kafka.twitter;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaTwitterMessageProducer {

    public static Logger logger = LoggerFactory.getLogger(KafkaTwitterMessageProducer.class.getName());

    public static final String BOOTSTRAP_SERVER = "localhost:9092";
    public static final String TOPIC_NAME = "twitter-tweets-topic";

    public void sendMessage(String message) {

        KafkaProducer<String, String> kafkaProducer = createProducer();
        produce(kafkaProducer, message);
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private KafkaProducer<String, String> createProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Safe Producer at the cost of throughput and latency
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,String.valueOf(Boolean.TRUE));
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,String.valueOf(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,String.valueOf(5));

        //Higher throughput

        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,Integer.toString(20));
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
        return kafkaProducer;
    }

    private void produce(KafkaProducer<String, String> addressProducer, String message) {

        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(TOPIC_NAME, message);

        addressProducer.send(producerRecord, (metadata, exception) -> {

            if (exception == null) {
                logger.info("Received new MetaData :: Topic :" + metadata.topic() + " Partition : " + metadata.partition() + " Offset : " + metadata.offset()
                        + " Time stamp : " + metadata.timestamp());
            } else {
                logger.info("Error while producing message ");
            }

        });
    }
}
