package com.venkat.kafka.twitter;

import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;

public class TwitterStatusListener implements StatusListener {

    private static Logger logger = LoggerFactory.getLogger(TwitterStatusListener.class);

    KafkaTwitterMessageProducer messageProducer = new KafkaTwitterMessageProducer();

    private Gson gson = new Gson();

    @Override
    public void onStatus(Status status) {

        Message message = new Message(status.getUser().getScreenName(), status.getText(),status.getId(),Long.valueOf(status.getUser().getFollowersCount()));
        String jsonMessage = gson.toJson(message);
        messageProducer.sendMessage(jsonMessage);
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
