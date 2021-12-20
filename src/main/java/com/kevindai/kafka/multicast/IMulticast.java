package com.kevindai.kafka.multicast;

import com.kevindai.kafka.exception.ProducerException;

/**
 *
 * 广播接口
 *
 * 其中channel和topic由管理员统一分配
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:18
 */
public interface IMulticast {

    /**
     * 同步发送广播,阻塞直到服务器返回成功
     */
    void multicast(final String message) throws ProducerException;

    /**
     * 设置业务独享的频道号
     */
    void setChannel(int channel);

    /**
     * 设置topic
     */
    void setTopic(String topic);
}
