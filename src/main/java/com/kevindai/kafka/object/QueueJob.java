package com.kevindai.kafka.object;

import org.msgpack.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 10:19
 */

public class QueueJob {

    protected static final Logger log = LoggerFactory.getLogger("module-kafka");
    protected static final MessagePack messagePack = new MessagePack();

    static {
        messagePack.register(QueueJob.class);
    }

    String topic;
    String messageKey;
    byte[] message;
    long timestamp;
    int age = 1;

    public QueueJob() {

    }

    public QueueJob(String topic, String messageKey, byte[] message, long ts) {
        this.topic = topic;
        this.messageKey = messageKey;
        this.message = message;
        this.timestamp = ts;
    }

    public static QueueJob deserialize(final byte[] bb) throws IOException {
        if (bb == null) {
            return null;
        }
        final QueueJob job = messagePack.read(bb, QueueJob.class);
        return job;
    }

    public byte[] serialize() throws IOException {
        return messagePack.write(this);
    }

    public boolean isOldEnough() {
        return System.currentTimeMillis() - timestamp > 10 * 1000;
    }

    public void renew() {
        this.age++;
        this.timestamp = System.currentTimeMillis();
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setMessageKey(String messageKey) {
        this.messageKey = messageKey;
    }

    public void setMessage(byte[] message) {
        this.message = message;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getTopic() {
        return topic;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public byte[] getMessage() {
        return message;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }
}