package com.venkat.kafka.twitter;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;

public class TwitterStreamReader {

    private static Logger logger = LoggerFactory.getLogger(TwitterStreamReader.class);
    private static final String CONSUMER_KEY = "consumer.key";
    private static final String CONSUMER_SECRET = "consumer.secret";
    private static final String ACCESS_TOKEN = "access.token";
    private static final String ACCESS_SECRET = "access.secret";

    public void readTweets(String[] term) throws TwitterException {

        TwitterStream twitterStream = createClient();

        FilterQuery filterQuery = new FilterQuery();
        filterQuery.track(term);

        twitterStream.addListener(new TwitterStatusListener());
        twitterStream.filter(filterQuery);

    }

    private TwitterStream createClient() {

        ConfigurationBuilder configBuilder = new ConfigurationBuilder();
        configBuilder.setDebugEnabled(true)
                .setOAuthConsumerKey(AppPropertyReader.getValue(CONSUMER_KEY))
                .setOAuthConsumerSecret(AppPropertyReader.getValue(CONSUMER_SECRET))
                .setOAuthAccessToken(AppPropertyReader.getValue(ACCESS_TOKEN))
                .setOAuthAccessTokenSecret(AppPropertyReader.getValue(ACCESS_SECRET));
        TwitterStream twitterStream = new TwitterStreamFactory(configBuilder.build()).getInstance();
        return twitterStream;

    }
}
