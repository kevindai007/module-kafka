package com.kevindai.kafka.object;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:08
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClusterConfig {

    private String name;
    private String dc;
    private String brokers;
    private String zookeeper;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDc() {
        return dc;
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public String getBrokers() {
        return brokers;
    }

    public void setBrokers(String brokers) {
        this.brokers = brokers;
    }

    public String getZookeeper() {
        return zookeeper;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }
}
