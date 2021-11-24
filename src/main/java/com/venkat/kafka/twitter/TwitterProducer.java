package com.venkat.kafka.twitter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterProducer {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONSUMER_KEY = "dbQ06JPYDWGY0uVvixTux3DOQ";
    private static final String CONSUMER_SECRET = "RMgxNJ6fiNB0wdYqyAIEO1Cw01WbFBWQD4abr6ZjonVevLKth9";
    private static final String ACCESS_TOKEN = "1463387303762563073-bVFMQVqEVSNVNBHKZRo69nBFSXgAeh";
    private static final String ACCESS_SECRET = "g5ly6VFF8UOBqMNuLoi1TGOBHDZPXUaQcrKr9guzkUIsB";

    public static void main(String[] args) throws TwitterException {

        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        Twitter client = createClient();
        Query query = new Query("bitcoin");
        QueryResult result = client.search(query);
        List<Status> statuses = result.getTweets();

        logger.info(";; "+ statuses.size());

    }

    private static Twitter createClient() {

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_SECRET);
        TwitterFactory tf = new TwitterFactory(cb.build());
        return tf.getInstance();

    }
}
