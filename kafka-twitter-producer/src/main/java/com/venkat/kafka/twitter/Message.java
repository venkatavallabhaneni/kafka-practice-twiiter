package com.venkat.kafka.twitter;

public class Message {

    private String userName;
    private String message;
    private Long tweetId;
    private Long followers;

    public Long getTweetId() {
        return tweetId;
    }

    public void setTweetId(Long tweetId) {
        this.tweetId = tweetId;
    }

    public Message(String userName, String message, Long tweetId,Long followers) {
        this.userName = userName;
        this.message = message;
        this.tweetId = tweetId;
        this.followers=followers;
    }

    public Message(String userName, String message) {
        this.userName = userName;
        this.message = message;
    }

    public Long getFollowers() {
        return followers;
    }

    public void setFollowers(Long followers) {
        this.followers = followers;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
