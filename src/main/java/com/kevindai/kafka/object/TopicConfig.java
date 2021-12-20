package com.kevindai.kafka.object;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:15
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TopicConfig {

    private String topic;

    private String clusterName;

    private String compressor = "lz4";

    private int acks = 1;
    private int maxTPS = 1000;
    private int maxSize = 5 * 1000 * 1000;
    private int linger = 20;
    private int retries = 0;
    private int bufferSize = 33554432;
    private long gmtCreated;
    private boolean multicast = false;

    private List<String> owners;

    public static TopicConfig createDefault(final String topic, final String cluster) {
        final TopicConfig tc = new TopicConfig();
        tc.setTopic(topic);
        tc.setRetries(1);
        tc.setClusterName(cluster);
        return tc;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getGmtCreated() {
        return gmtCreated;
    }

    public void setGmtCreated(long gmtCreated) {
        this.gmtCreated = gmtCreated;
    }

    public int getMaxSize() {
        return maxSize;
    }

    public void setMaxSize(int maxSize) {
        this.maxSize = maxSize;
    }

    public int getMaxTPS() {
        return maxTPS;
    }

    public void setMaxTPS(int maxTPS) {
        this.maxTPS = maxTPS;
    }

    public int getAcks() {
        return acks;
    }

    public void setAcks(int acks) {
        this.acks = acks;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public int getLinger() {
        return linger;
    }

    public void setLinger(int linger) {
        this.linger = linger;
    }

    public int getRetries() {
        return retries;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public String getCompressor() {
        return compressor;
    }

    public void setCompressor(String compressor) {
        this.compressor = compressor;
    }

    public List<String> getOwners() {
        return owners;
    }

    public void setOwners(List<String> owners) {
        this.owners = owners;
    }

    public boolean isMulticast() {
        return multicast;
    }

    public void setMulticast(boolean multicast) {
        this.multicast = multicast;
    }
}
