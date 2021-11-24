package com.venkat.kafka.twitter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterProducer {

    private static Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private static final String CONSUMER_KEY = "dbQ06JPYDWGY0uVvixTux3DOQ";
    private static final String CONSUMER_SECRET = "RMgxNJ6fiNB0wdYqyAIEO1Cw01WbFBWQD4abr6ZjonVevLKth9";
    private static final String ACCESS_TOKEN = "1463387303762563073-bVFMQVqEVSNVNBHKZRo69nBFSXgAeh";
    private static final String ACCESS_SECRET = "g5ly6VFF8UOBqMNuLoi1TGOBHDZPXUaQcrKr9guzkUIsB";

    public static void main(String[] args) throws TwitterException {

        TwitterStream twitterStream = createClient();

        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track("bitcoin");

        twitterStream.addListener(new TwitterStatusListener());
        twitterStream.filter(filterQuery);

    }

    private static TwitterStream createClient() {

        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(CONSUMER_KEY)
                .setOAuthConsumerSecret(CONSUMER_SECRET)
                .setOAuthAccessToken(ACCESS_TOKEN)
                .setOAuthAccessTokenSecret(ACCESS_SECRET);
        TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
        return twitterStream;

    }
}
