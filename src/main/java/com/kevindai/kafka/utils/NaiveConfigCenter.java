package com.kevindai.kafka.utils;

import com.kevindai.kafka.object.ClusterConfig;
import com.kevindai.kafka.object.ConsumerConfig;
import com.kevindai.kafka.object.SubscriberConfig;
import com.kevindai.kafka.object.TopicConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 *
 * 幼稚好用的配置中心
 *
 * 用户直接配置对接相应的kafka集群名称或broker地址即可使用
 * 同时保证了一定的可配置性和灵活性，业务常用的配置如失败重试次数、缓冲区内存大小等均可在这里统一配置
 *
 * 配置指导原则：
 *
 *      一般情况请保持默认即可，不要画蛇添足自以为是
 *
 *      若业务对数据可靠性要求非常高，建议增大失败重试次数（retries），最大可以到3
 *
 *      若业务单机每秒消息发送量较大（消息数据的字节大小）超过10MB，则应适当增加buffSize
 *      一般可以增加到每秒消息总字节大小的2.4倍左右
 *      默认情况下这个值为32MB，不得低于这个值
 * @author kevindai
 * @Desc
 * @date 2021/12/14 10:58
 */
public class NaiveConfigCenter implements IConfigCenter {

    protected static final Logger log = LoggerFactory.getLogger("module-kafka");
    private final static int DEFAULT_PORT = 9092;

    private String bootstrap;
    private String zookeeper;
    private String dc;
    private int retries = 1;
    /**
     * should be sufficient for most scenarios
     */
    private int buffSize = 33554432;
    private int maxPollRecords = 200;

    private String clusterName = "default";

    public void init() throws Exception {
        if (isEmpty(bootstrap) || StringUtils.containsAny(bootstrap, ',', ' ')) {
            throw new IllegalArgumentException("invalid bootstrap config");
        }

        if (isEmpty(dc)) {
            dc = StringUtils.lowerCase(System.getenv("DC"));
        }

        int port = DEFAULT_PORT;
        String host = bootstrap;
        if (StringUtils.contains(bootstrap, ':')) {
            port = NumberUtils.toInt(StringUtils.substringAfterLast(bootstrap, ":"));
            host = StringUtils.substringBefore(bootstrap, ":");
        }

        if (!StringUtils.containsAny(host,  '.')) {
            //bootstrap is cluster name so convert it to domain while DC be must be set first
            if (isEmpty(dc)) {
                throw new IllegalArgumentException("env variable [DC] must be set");
            }
            clusterName = host;
            //ex kafka110-main.hz.td
            host = String.format("%s.%s.td", host, dc);
        }

        try {
            final InetAddress ia = InetAddress.getByName(host);
            log.info("succeed to resolve kafka broker address: {}->{}", host, ia.getHostAddress());
        } catch (UnknownHostException e) {
            throw new Exception("contact admin for support due to dns failure: " + host, e);
        }

        if (isEmpty(dc)) {
            log.warn("env variable [DC] not set");
        }


        if (isEmpty(zookeeper)) {
            log.info("multi-cast unavailable due to empty zookeeper config");
        }

        bootstrap = String.format("%s:%d", host, port);
        log.info("kafka client config-center initiated with bootstrap: {}", bootstrap);
    }

    @Override
    public boolean isNaive() {
        return true;
    }

    @Override
    public ClusterConfig queryCluster(final String cluster) {
        final ClusterConfig cc = new ClusterConfig();
        cc.setDc(dc);
        cc.setBrokers(bootstrap);
        cc.setZookeeper(zookeeper);
        cc.setName(cluster);
        return cc;
    }

    @Override
    public TopicConfig queryTopic(final String topic) {
        final TopicConfig topicConfig = TopicConfig.createDefault(topic, clusterName);
        topicConfig.setRetries(retries);
        topicConfig.setBufferSize(buffSize);
        return topicConfig;
    }

    @Override
    public ConsumerConfig queryConsumer(final String name) {
        final SubscriberConfig sc = new SubscriberConfig();
        sc.put("max.poll.records", maxPollRecords);
        final ConsumerConfig cc = new ConsumerConfig();
        cc.setName(name);
        cc.setPollInterval(200);
        cc.setDefaultOne(sc);
        return cc;
    }

    @Override
    public void close() throws IOException {
        //nothing to do
    }

    public void setDc(String dc) {
        this.dc = dc;
    }

    public void setBootstrap(String bootstrap) {
        this.bootstrap = bootstrap;
    }

    public void setZookeeper(String zookeeper) {
        this.zookeeper = zookeeper;
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setBuffSize(int buffSize) {
        this.buffSize = buffSize;
    }

    public void setMaxPollRecords(int maxPollRecords) {
        this.maxPollRecords = maxPollRecords;
    }

}
