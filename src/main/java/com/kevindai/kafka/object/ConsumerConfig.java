package com.kevindai.kafka.object;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;
import java.util.Map;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:18
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConsumerConfig {

    private String name;
    private List<String> owners;
    private int pollInterval = 100;
    private Map<String, SubscriberConfig> subscribed;
    private SubscriberConfig defaultOne = new SubscriberConfig();

    public SubscriberConfig query(final String topic) {
        if (subscribed == null || !subscribed.containsKey(topic)) {
            return defaultOne;
        }
        return subscribed.get(topic);
    }

    public int getPollInterval() {
        return pollInterval;
    }

    public void setPollInterval(int pollInterval) {
        this.pollInterval = pollInterval;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setSubscribed(Map<String, SubscriberConfig> subscribed) {
        this.subscribed = subscribed;
    }

    public void setDefaultOne(SubscriberConfig defaultOne) {
        this.defaultOne = defaultOne;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }
}
