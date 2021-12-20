package com.kevindai.kafka.consumer;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:15
 */
public enum Mode {

    STANDARD,
    //do not use paranoid mode unless you are pretty sure about what is going on here !!!
    PARANOID
    ;

    public static Mode of (final String mode) {
        for (final Mode m : Mode.values()) {
            if (m.name().equalsIgnoreCase(mode)) {
                return m;
            }
        }
        return STANDARD;
    }
}
