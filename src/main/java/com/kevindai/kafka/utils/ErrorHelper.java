package com.kevindai.kafka.utils;

import org.apache.kafka.common.errors.*;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 10:51
 */
public class ErrorHelper {

    public static boolean isRecoverable(final Exception e) {
        if (e == null){
            return true;
        }
        if (e instanceof CorruptRecordException) {
            return true;
        }
        if (e instanceof InvalidMetadataException) {
            return true;
        }
        if (e instanceof NotEnoughReplicasAfterAppendException) {
            return true;
        }
        if (e instanceof NotEnoughReplicasException) {
            return true;
        }
        if (e instanceof OffsetOutOfRangeException) {
            return true;
        }
        if (e instanceof TimeoutException) {
            return true;
        }
        return false;
    }
}
