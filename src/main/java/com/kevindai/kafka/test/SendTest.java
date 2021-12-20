package com.kevindai.kafka.test;

import com.google.common.collect.Sets;
import com.kevindai.kafka.object.ComplexTopic;
import com.kevindai.kafka.producer.IProducer;
import com.kevindai.kafka.producer.SimpleProducer;
import com.kevindai.kafka.utils.IConfigCenter;
import com.kevindai.kafka.utils.NaiveConfigCenter;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 11:05
 */
public class SendTest {


    protected static final Logger log = LoggerFactory.getLogger("module-kafka");

    public static final String TOPIC = "hylia_shield_monitor_namelist_temp";
    //    public static final String COMPLEX_TOPIC = String.format("%s,%s2", TOPIC, TOPIC);
    public static final String COMPLEX_TOPIC = String.format("%s", TOPIC);

    private IConfigCenter configCenter;
    private IProducer producer;
    private static final Random random = new Random();

    private long lastRcvTime = System.currentTimeMillis();

    private final ComplexTopic complexTopic = ComplexTopic.create(COMPLEX_TOPIC);
    private final Set<String> sntMsg = Sets.newConcurrentHashSet();
    private final Set<String> rcvMsg = Sets.newConcurrentHashSet();
    private final AtomicInteger retryExceptionMsg = new AtomicInteger();

    public static void main(String[] argv) throws Exception {
        System.out.println("app-test started!!!");
        long send = Long.parseLong(System.getProperty("send", "10"));
        System.out.printf("sending %s msg\n", send);

        final SendTest sendTest = new SendTest();
        sendTest.setup();
        if (send > 0) {
            sendTest.sender(send);
        }
        Thread.sleep(5 * 1000);
        while (true) {
            if (System.currentTimeMillis() - sendTest.lastRcvTime > 10 * 1000) {
                sendTest.configCenter.close();
//                sendTest.poller.close();
//                timer.cancel();
//                System.out.println("rcv contains all: " + app.rcvMsg.containsAll(app.sntMsg));
//                System.out.printf("no further message received! rcvMsg:%d, sentMsg:%d\n", app.rcvMsg.size(), app.sntMsg.size());
//                System.out.println("retryLaterException messages:" + app.retryExceptionMsg.get());
                break;
            }
        }
    }

    public IConfigCenter setupNaiveConfigCenter() throws Exception {
        final NaiveConfigCenter ncc = new NaiveConfigCenter();
        ncc.setBootstrap("10.57.19.182:9101");
//        ncc.setDc("te");
        ncc.init();
        return ncc;
    }


    private void buildProducer() throws IOException {
        final SimpleProducer sp = new SimpleProducer();
        sp.setConfigCenter(configCenter);
        sp.setTopics(complexTopic.getParts());
        sp.init();
        producer = sp;
    }

    public void setup() throws Exception {
        this.configCenter = setupNaiveConfigCenter();
        buildProducer();
//        buildPoller();
        Thread.sleep(2000);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    producer.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });

        System.out.println("setup completed!");
    }

    public void sender(final long send) throws Exception {
        final long start = System.currentTimeMillis();
        final String msg1 = UUID.randomUUID().toString();
        producer.syncProduce(TOPIC, msg1, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        sntMsg.add(msg1);

        for (long i = 1; i < send; i++) {
            final byte[] data = new byte[random.nextInt(10 * 900) + 1000];
            for (int n = 0; n < data.length; n++) {
                data[n] = (byte) (random.nextInt(60) + 48);
            }
            final String topic = complexTopic.getParts().get((random.nextInt(1)));
            final String msgKey = UUID.randomUUID().toString();
            producer.produce(topic, msgKey, data, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        sntMsg.add(msgKey);
                    } else {
                        log.error("failed to sent msg:" + msgKey);
                    }
                }
            });
            Thread.sleep(random.nextInt(300));
        }
        producer.close();
        System.out.printf("sender ended within %d seconds\n", (System.currentTimeMillis() - start) / 1000);
        System.out.printf("%d messages have been sent\n", sntMsg.size());
    }
}
