package com.kevindai.kafka.consumer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.kevindai.kafka.exception.RetryLaterException;
import com.kevindai.kafka.object.ClusterConfig;
import com.kevindai.kafka.object.ComplexTopic;
import com.kevindai.kafka.object.ConsumerConfig;
import com.kevindai.kafka.object.TopicConfig;
import com.kevindai.kafka.utils.IConfigCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:13
 */
public class StandardPoller implements Closeable {

    protected static final Logger log = LoggerFactory.getLogger("module-kafka");
    protected static final String THREAD_NAME_PFX = "KPOLLER-";
    protected static final String ERR_UNEXPECTED_EXCEPTION = "are you insane ? exception here is strictly prohibited!!!";

    protected final AtomicBoolean running = new AtomicBoolean(true);
    protected final Random random = new Random();

    private Map<String, List<Integer>> partitions;
    private Map<String, String> overrides;
    private String consumerName;
    private IConfigCenter configCenter;
    private Map<String, IConsumer> bizConsumers;//key is complexTopic
    protected final Map<String, IConsumer> bizConsumerMap = Maps.newHashMap();//key is simple topic
    protected final List<KafkaConsumer<String, byte[]>> alivedConsumers = Lists.newArrayList();

    private Mode mode = Mode.of("standard");
    protected ConsumerConfig consumerConfig;

    public void init() throws Exception {
        consumerConfig = configCenter.queryConsumer(consumerName);
        Preconditions.checkArgument(consumerConfig != null, "consumer is not registered:" + consumerName);
        log.warn("KPOLLER initiating ... ");
        for (final String complexTopic : bizConsumers.keySet()) {
            final ComplexTopic ct = ComplexTopic.create(complexTopic);
            for (final String simple : ct.getParts()) {
                bizConsumerMap.put(simple, bizConsumers.get(complexTopic));
            }
            //use the 1st topic in complex topic. ex 'forseti_topic' when set with 'forset_topic,forseti_topic2'
            final TopicConfig tc = configCenter.queryTopic(ct.getMain());
            Preconditions.checkArgument(tc != null, "topic is not registered:" + ct.getMain());
            final ClusterConfig clusterConfig = configCenter.queryCluster(tc.getClusterName());
            createMessagePoller(ct, clusterConfig);
        }
    }

    private Properties buildProperties(final ComplexTopic ct, final ClusterConfig cluster) {
        final Properties props = new Properties();
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBrokers());
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG, consumerName);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        //following merged properties: [max.poll.records, max.partition.fetch.bytes]
        consumerConfig.query(ct.getMain()).mergeTo(props);
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("_cluster.name", cluster.getName());
        if (overrides != null) {
            props.putAll(overrides);
        }
        return props;
    }

    protected final void randomSleep() {
        try {
            Thread.sleep(1 + random.nextInt(10));
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * override this if you want more (or less) KafkaConsumer
     */
    protected int concurrentConsumer(final ComplexTopic complexTopic) {
        return (int) consumerConfig.query(complexTopic.getMain()).get("cc");
    }

    private void createMessagePoller(final ComplexTopic complexTopic, final ClusterConfig clusterConfig) {
        final Properties props = buildProperties(complexTopic, clusterConfig);
        //one KafkaConsumer per thread model
        for (int i = 0; i < concurrentConsumer(complexTopic); i++) {
            final KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
            switch (mode) {
                case PARANOID:
                    Preconditions.checkArgument(partitions != null && partitions.size() > 0, "partition assignment not found");
                    log.warn("building paranoid consumer for {} !!! ", complexTopic);
                    final Set<TopicPartition> set = Sets.newHashSet();
                    for (final String topic : complexTopic.getParts()) {
                        for (final Integer pt : partitions.get(topic)) {
                            TopicPartition tp = new TopicPartition(topic, pt);
                            set.add(tp);
                        }
                    }
                    consumer.assign(set);
                    break;
                case STANDARD:
                    consumer.subscribe(complexTopic.getParts());
            }
            alivedConsumers.add(consumer);
            kickOff(complexTopic.get(), consumer);
        }
    }

    /**
     * override if you want to start consumer in your own way
     */
    protected void kickOff(final String name, final KafkaConsumer<String, byte[]> consumer) {
        new Thread(THREAD_NAME_PFX + name) {
            @Override
            public void run() {
                try {
                    onPollingWorkerStarted(consumer);
                    while (running.get()) {
                        final ConsumerRecords<String, byte[]> records = consumer.poll(consumerConfig.getPollInterval());
                        final AtomicBoolean retryLater = new AtomicBoolean(false);
                        if (records == null || records.isEmpty()) {
                            continue;
                        }
                        final Map<TopicPartition, OffsetAndMetadata> offsets = Maps.newHashMap();
                        for (final TopicPartition partition : records.partitions()) {
                            final IConsumer biz = bizConsumerMap.get(partition.topic());
                            final List<ConsumerRecord<String, byte[]>> ptRecords = records.records(partition);
                            try {
                                biz.doConsume(ptRecords);
                            } catch (RetryLaterException e) {
                                retryLater.set(true);
                                randomSleep();
                            } catch (Throwable e) {
                                log.error(ERR_UNEXPECTED_EXCEPTION, e);
                            }

                            if (retryLater.get()) {
                                //we have to retry
                                consumer.seek(partition, ptRecords.get(0).offset());
                            } else {
                                //everything is ok and commit offset immediately
                                final long lastOffset = ptRecords.get(ptRecords.size() - 1).offset();
                                offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
                            }
                        }
                        commit(consumer, offsets);
                    }
                } catch (WakeupException e) {
                    log.warn("kafka poller is being closing gently");
                } catch (Exception e) {
                    log.error("kafka poller encountered fatal error and halting:", e);
                } finally {
                    log.warn("consumer closed:" + name);
                    consumer.close();
                }
            }
        }.start();
    }

    /**
     * override this if you need to do something before real polling.
     * this method should never be called concurrently
     */
    protected void onPollingWorkerStarted(final KafkaConsumer<String, byte[]> consumer) {
        //default: nothing to do
    }

    /**
     * override this if commit to somewhere else
     */
    protected void commit(
            final KafkaConsumer<String, byte[]> consumer,
            final Map<TopicPartition, OffsetAndMetadata> offsets
    ) {
        try {
            if (offsets.size() == 0) return;
            consumer.commitSync(offsets);
        } catch (Exception e) {
            log.error("failed to commit due to exception", e);
        }
    }

    @Override
    public void close() throws IOException {
        running.set(false);
        log.warn("closing POLLER...");
        for (final KafkaConsumer<String, byte[]> consumer : alivedConsumers) {
            try {
                consumer.wakeup();//wake them up from `poll`
            } catch (Exception e) {
                log.error("failed to close KafkaConsumer", e);
            }
        }
    }

    public IConfigCenter getConfigCenter() {
        return configCenter;
    }

    public void setMode(String mode) {
        this.mode = Mode.of(mode);
    }

    public void setPartitions(Map<String, List<Integer>> partitions) {
        this.partitions = partitions;
    }

    public void setOverrides(Map<String, String> overrides) {
        this.overrides = overrides;
    }

    public void setConsumerName(String consumerName) {
        this.consumerName = consumerName;
    }

    public void setConfigCenter(IConfigCenter configCenter) {
        this.configCenter = configCenter;
    }

    public void setBizConsumers(Map<String, IConsumer> bizConsumers) {
        this.bizConsumers = bizConsumers;
    }
}
