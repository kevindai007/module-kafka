package com.kevindai.kafka.test;

import com.google.common.collect.Sets;
import com.kevindai.kafka.consumer.ConcurrentPoller;
import com.kevindai.kafka.consumer.IConsumer;
import com.kevindai.kafka.consumer.StandardPoller;
import com.kevindai.kafka.exception.RetryLaterException;
import com.kevindai.kafka.object.ComplexTopic;
import com.kevindai.kafka.utils.IConfigCenter;
import com.kevindai.kafka.utils.NaiveConfigCenter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:25
 */
public class ConsumerTest {
    protected static final Logger log = LoggerFactory.getLogger("module-kafka");
    private long lastRcvTime = System.currentTimeMillis();
    private final Set<String> rcvMsg = Sets.newConcurrentHashSet();
    private static final Random random = new Random();
    private final AtomicInteger retryExceptionMsg = new AtomicInteger();
    private StandardPoller poller;
    private IConfigCenter configCenter;
    public static final String TOPIC = "hylia_shield_monitor_namelist_temp";
    public static final String COMPLEX_TOPIC = String.format("%s", TOPIC);
    private final ComplexTopic complexTopic = ComplexTopic.create(COMPLEX_TOPIC);


    public class MyConsumer implements IConsumer {

        @Override
        public void doConsume(List<ConsumerRecord<String, byte[]>> messages) {
            lastRcvTime = System.currentTimeMillis();
            final ConsumerRecord<String, byte[]> meta = messages.get(0);
            log.warn("consume() called once >> {}, topic:{}, pt:{}", messages.size(), meta.topic(), meta.partition());
            final int cnt = messages.size();
            for (final ConsumerRecord<String, byte[]> cr : messages) {
                rcvMsg.add(cr.key());
            }
            try {
                if (random.nextInt(10) % 5 == 0) {
                    throw new Exception();
                }
            } catch (Exception e) {
                log.warn("RetryLaterException:" + cnt);
                retryExceptionMsg.addAndGet(cnt);
                throw new RetryLaterException();
            }

        }
    }


    private void buildPoller() throws Exception {
        final String type = System.getProperty("poller", "cc");
        if (type.equalsIgnoreCase("cc")) {
            poller = new ConcurrentPoller();
        } else {
            poller = new StandardPoller();
        }
        poller.setConfigCenter(configCenter);
        poller.setConsumerName("perf-test-consumer2");
        poller.setBizConsumers(Collections.singletonMap(complexTopic.get(), new MyConsumer()));
        poller.init();
    }


    public void setup() throws Exception {
        this.configCenter = setupNaiveConfigCenter();
        buildPoller();
        Thread.sleep(2000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                poller.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }));

        System.out.println("setup completed!");
    }

    public IConfigCenter setupNaiveConfigCenter() throws Exception {
        final NaiveConfigCenter ncc = new NaiveConfigCenter();
        ncc.setBootstrap("10.57.19.182:9101");
//        ncc.setDc("te");
        ncc.init();
        return ncc;
    }


    public static void main(String[] argv) throws Exception {
        System.out.println("app-test started!!!");
        long send = Long.parseLong(System.getProperty("send", "10"));
        System.out.printf("sending %s msg\n", send);

        final ConsumerTest app = new ConsumerTest();
        app.setup();
        Thread.sleep(5 * 1000);
        while (true) {
            if (System.currentTimeMillis() - app.lastRcvTime > 10 * 1000) {
//                app.configCenter.close();
//                app.poller.close();
//                System.out.println("retryLaterException messages:" + app.retryExceptionMsg.get());
                break;
            }
        }
    }

}
