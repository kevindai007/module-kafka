package com.kevindai.kafka.object;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 11:06
 */
public class ComplexTopic {

    private String complexTopic;
    private List<String> parts;

    public static ComplexTopic create(String s) {
        final ComplexTopic ct = new ComplexTopic();
        ct.complexTopic = s;
        ct.parts = Lists.newArrayList(StringUtils.split(s,","));
        return ct;
    }

    public boolean isComplex() {
        return parts.size() > 1;
    }

    public String getMain() {
        return parts.get(0);
    }

    public List<String> getParts() {
        return parts;
    }

    public String get() {
        return complexTopic;
    }
}

