package com.kevindai.kafka.utils;

import com.kevindai.kafka.object.ClusterConfig;
import com.kevindai.kafka.object.ConsumerConfig;
import com.kevindai.kafka.object.TopicConfig;

import java.io.Closeable;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:10
 */
public interface IConfigCenter extends Closeable {
    /**
     * 当前运行环境是否是naive模式（由当前所连接的注册中心决定）
     */
    boolean isNaive();

    ClusterConfig queryCluster(String cluster);

    /**
     * 查询这个topic的注册配置，若当前为naive模式，则使用默认配置
     */
    TopicConfig queryTopic(String topic);

    /**
     * 查询这个consumer的注册配置，若当前为naive模式，则使用默认配置
     */
    ConsumerConfig queryConsumer(final String name);
}
