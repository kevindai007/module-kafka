package com.kevindai.kafka.producer;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 08:55
 */

import com.kevindai.kafka.exception.ProducerException;
import org.apache.kafka.clients.producer.Callback;

import java.io.Closeable;

/**
 * 高性能 易使用的Kafka消息发送
 * <p>
 * 请仔细处理ProducerException，并根据原因（各is开头的方法）来做出相应处理
 */
public interface IProducer extends Closeable {

    /**
     * 异步发送消息,该方法会将消息放入本地缓冲区并立刻返回, 若此时宕机则该消息可能会永久丢失
     * <p>
     * 可靠性：高
     */
    void produce(final String topic, final String messageKey, final byte[] message) throws ProducerException;

    /**
     * 异步发送消息,该方法支持callback以获取实际发送结果,该方法自身抛异常唯一的原因是参数非法，如某个参数为null
     * <p>
     * 业务从可以Callback中拿到exception信息，并调用ErrorHelper.isRecoverable方法判断
     * 该异常是否是可恢复性，若为可恢复性异常则应稍后再次重试发送
     * 如果为不可恢复异常则不要重试，因为重试再多次数也毫无意义，这种情况请记录日志并触发告警
     * <p>
     * 可靠性：高
     */
    void produce(
            final String topic,
            final String messageKey,
            final byte[] message,
            final Callback callback
    ) throws ProducerException;

    /**
     * 异步发送消息,但阻塞当前线程直到有结果返回（或抛异常）
     * <p>
     * 请严格注意该方法的特性!!!!
     * <p>
     * 可靠性：最高
     *
     * @return 返回是否成功，业务流程需要关注这个返回值
     */
    boolean syncProduce(final String topic, final String messageKey, final byte[] message) throws ProducerException;
}
