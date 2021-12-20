package com.kevindai.kafka.object;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 09:18
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriberConfig extends HashMap<String, Object> {

    public SubscriberConfig() {
        put("max.poll.records", 200);
        put("max.partition.fetch.bytes", 5 * 1024 * 1024);
        put("cc", 1);
    }

    public void mergeTo(final Properties props) {
        for (Map.Entry<String, Object> e : this.entrySet()) {
            props.put(e.getKey(), e.getValue());
        }
    }
}
