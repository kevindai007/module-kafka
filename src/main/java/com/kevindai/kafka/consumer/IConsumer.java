package com.kevindai.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

/**
 * 具备at-least-once语义特征的消费者
 *
 * 可能会有重复消息,业务需要自行保证幂等
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:12
 */
public interface IConsumer {

    /**
     * 业务消费
     *
     * 注意!!该方法可能会被多个线程调用,业务需要自行保证线程安全
     *
     * 该方法不得抛异常!!!请在方法内部自行处理所有异常!!!
     *
     * 一旦方法返回,系统将认定这一批消息已经处理完毕
     *
     * @param messages 系统保证本次调用data数据均属于同一个partition下
     *
     */
    void doConsume(final List<ConsumerRecord<String, byte[]>> messages);
}