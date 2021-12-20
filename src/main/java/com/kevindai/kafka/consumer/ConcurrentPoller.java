package com.kevindai.kafka.consumer;

import com.google.common.collect.Maps;
import com.kevindai.kafka.exception.RetryLaterException;
import com.kevindai.kafka.object.ComplexTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * 多线程消费者
 * <p>
 * 多个线程共享一个KafkaConsumer实例
 * <p>
 * 总线程数=TOPIC数量*被分配的Partition数量+1
 *
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:11
 */
public class ConcurrentPoller  extends StandardPoller {

    private final Map<TopicPartition, ExecutorService> executor = Maps.newConcurrentMap();

    @Override
    protected int concurrentConsumer(final ComplexTopic ct) {
        //only one KafkaConsumer in order to reduce connections
        return 1;
    }

    private ExecutorService newSingleCachedThreadPool() {
        final ThreadPoolExecutor pool = new ThreadPoolExecutor(1, 1,
                60 * 1000L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>());
        //when rebalanced, some TopicPartition might have been moved to another node
        //so remaining ExecutorService thread should be terminated in order to reduce CPU utilization.
        pool.allowCoreThreadTimeOut(true);
        return pool;
    }

    @Override
    protected void kickOff(final String name, final KafkaConsumer<String, byte[]> consumer) {
        final Runnable poller = () -> {
            try {
                //only one thread is allowed to enter !!!
                while (running.get()) {
                    final ConsumerRecords<String, byte[]> records = consumer.poll(consumerConfig.getPollInterval());
                    if (records == null || records.isEmpty()) {
                        continue;
                    }

                    final Map<TopicPartition, OffsetAndMetadata> offsets = Maps.newHashMap();
                    final CountDownLatch latch = new CountDownLatch(records.partitions().size());
                    for (final TopicPartition partition : records.partitions()) {
                        final List<ConsumerRecord<String, byte[]>> ptRecords = records.records(partition);
                        final long newOffset = ptRecords.get(ptRecords.size() - 1).offset() + 1;
                        final IConsumer bizConsumer = bizConsumerMap.get(partition.topic());
                        if (!executor.containsKey(partition)) {
                            executor.put(partition, newSingleCachedThreadPool());
                        }
                        final ExecutorService es = executor.get(partition);
                        es.submit(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    bizConsumer.doConsume(ptRecords);
                                    offsets.put(partition, new OffsetAndMetadata(newOffset));
                                } catch (RetryLaterException re) {
                                    randomSleep();
                                    consumer.seek(partition, ptRecords.get(0).offset());
                                } catch (Throwable e) {
                                    log.error(ERR_UNEXPECTED_EXCEPTION, e);
                                } finally {
                                    latch.countDown();
                                }
                            }
                        });
                    }
                    latch.await();
                    commit(consumer, offsets);
                }
            } catch (WakeupException e) {
                log.warn("ConcurrentPoller closing ... ");
            } catch (Exception e) {
                log.error("ConcurrentPoller encountered fatal error:", e);
            } finally {
                log.warn("ConcurrentPoller closed: " + name);
                consumer.close();
            }
        };

        new Thread(poller, THREAD_NAME_PFX + "CC-" + name).start();
        log.warn("ConcurrentPoller started:{}", name);
    }

    @Override
    public void close() throws IOException {
        super.close();
        for (final ExecutorService es : executor.values()) {
            try {
                es.shutdown();
                es.awaitTermination(1, TimeUnit.MINUTES);
            } catch (InterruptedException e) {
                log.error("failed to close executors", e);
            }
        }
    }
}
