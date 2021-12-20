package com.kevindai.kafka.producer;

import com.google.common.base.Preconditions;
import com.kevindai.kafka.exception.ProducerException;
import com.kevindai.kafka.object.ClusterConfig;
import com.kevindai.kafka.object.QueueJob;
import com.kevindai.kafka.object.TopicConfig;
import com.kevindai.kafka.utils.ErrorHelper;
import com.kevindai.kafka.utils.IConfigCenter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * 若配置了多个topic,请咨询管理员并确保这些topic位于同一个集群下,否则会报错
 *
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:07
 */
public class SimpleProducer implements Closeable, IProducer {
    protected static final Logger log = LoggerFactory.getLogger("module-kafka");

    private boolean running = true;

    private List<String> topics;
    private TopicConfig defaultTopicConfig;
    private ClusterConfig clusterConfig;
    private IConfigCenter configCenter;
    private Producer<String, byte[]> producer;
    private Map<String, String> overrides;

    public void init() throws IOException {
        Preconditions.checkArgument(getTopics() != null && getTopics().size() > 0, "topic list is empty");
        this.defaultTopicConfig = configCenter.queryTopic(getTopics().get(0));
        Preconditions.checkArgument(defaultTopicConfig != null, "topic is not registered");

        for (final String topic : getTopics()) {
            final TopicConfig tc = configCenter.queryTopic(topic);
            Preconditions.checkArgument(tc != null, "topic not registered:" + topic);
            Preconditions.checkArgument(
                    tc.getClusterName().equalsIgnoreCase(this.defaultTopicConfig.getClusterName()),
                    "topic doesn't belong to the same cluster:" + topic
            );
        }

        this.clusterConfig = configCenter.queryCluster(defaultTopicConfig.getClusterName());
        build();
        log.warn("SimpleProducer initiated successfully");
    }


    private void build() {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterConfig.getBrokers());
        props.put(ProducerConfig.ACKS_CONFIG, String.valueOf(defaultTopicConfig.getAcks()));
        props.put(ProducerConfig.RETRIES_CONFIG, defaultTopicConfig.getRetries());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, defaultTopicConfig.getMaxSize());
        props.put(ProducerConfig.LINGER_MS_CONFIG, defaultTopicConfig.getLinger());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, defaultTopicConfig.getCompressor());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, defaultTopicConfig.getBufferSize());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put("_cluster.name", clusterConfig.getName());

        if (overrides != null) {
            props.putAll(overrides);
        }
        this.producer = new KafkaProducer<>(props);
    }

    @Override
    public void produce(String topic, String messageKey, byte[] message) throws ProducerException {
        final QueueJob job = new QueueJob(topic, messageKey, message, System.currentTimeMillis());
        internalProduce(job, null, null);
    }

    @Override
    public void produce(String topic, String messageKey, byte[] message, Callback callback) throws ProducerException {
        checkArg(callback != null, "invalid callback");
        final QueueJob job = new QueueJob(topic, messageKey, message, System.currentTimeMillis());
        internalProduce(job, null, callback);
    }

    @Override
    public boolean syncProduce(String topic, String messageKey, byte[] message) throws ProducerException {
        return false;
    }

    @Override
    public void close() throws IOException {
        if (!running) {
            //already closed
            return;
        }
        this.running = false;
        if (this.producer != null) {
            this.producer.flush();
            log.warn("producer closed for {}", defaultTopicConfig.getTopic());
            this.producer.close();
        }
    }

    protected Future<RecordMetadata> internalProduce(final QueueJob job, final Integer partition, final Callback cb) throws ProducerException {
        checkArg(job.getMessage() != null && job.getMessage().length < defaultTopicConfig.getMaxSize(),
                "message too long while max length in byte is " + defaultTopicConfig.getMaxSize()
        );
        checkArg(running, "failed to send because producer is closed");
        checkArg(topics.contains(job.getTopic()), "topic is not configured in the list");
        checkArg(StringUtils.isNotEmpty(job.getMessageKey()), "message key is empty");

        final ProducerRecord<String, byte[]> rec = new ProducerRecord<>(
                job.getTopic(), partition, job.getMessageKey(), job.getMessage()
        );
        try {
            return this.producer.send(rec, cb);
        } catch (Exception e) {
            onError(job, cb, e);
            return null;
        }
    }

    protected void checkArg(boolean expr, String msg) throws ProducerException {
        if (!expr) {
            throw ProducerException.invalidParam(msg);
        }
    }

    private void onError(final QueueJob job, final Callback callback, final Exception e) throws ProducerException {
        log.error("produce failed:" + job.getMessageKey(), e);
        if (callback != null) {
            //if callback exists then return without persistence
            callback.onCompletion(null, e);
            return;
        }
        if (ErrorHelper.isRecoverable(e)) {
            throw new ProducerException(e);
        } else {
            log.error("exception not recoverable:{}", job.getMessageKey());
            throw ProducerException.fatal(e);
        }
    }


    public boolean isRunning() {
        return running;
    }

    public void setRunning(boolean running) {
        this.running = running;
    }

    public List<String> getTopics() {
        return topics;
    }

    public void setTopics(List<String> topics) {
        this.topics = topics;
    }

    public TopicConfig getDefaultTopicConfig() {
        return defaultTopicConfig;
    }

    public void setDefaultTopicConfig(TopicConfig defaultTopicConfig) {
        this.defaultTopicConfig = defaultTopicConfig;
    }

    public ClusterConfig getClusterConfig() {
        return clusterConfig;
    }

    public void setClusterConfig(ClusterConfig clusterConfig) {
        this.clusterConfig = clusterConfig;
    }

    public IConfigCenter getConfigCenter() {
        return configCenter;
    }

    public void setConfigCenter(IConfigCenter configCenter) {
        this.configCenter = configCenter;
    }

    public Producer<String, byte[]> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, byte[]> producer) {
        this.producer = producer;
    }

    public Map<String, String> getOverrides() {
        return overrides;
    }

    public void setOverrides(Map<String, String> overrides) {
        this.overrides = overrides;
    }
}
