package com.venkat.kafka.twitter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStatusListener implements StatusListener   {

    private static Logger logger = LoggerFactory.getLogger(TwitterStatusListener.class);

    KafkaTwitterMessageProducer messageProducer = new KafkaTwitterMessageProducer();
    @Override
    public void onStatus(Status status) {

       messageProducer.sendMessage("@"+status.getUser().getScreenName()+" :: Message : "+status.getText());
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {

    }
}
