package com.kevindai.kafka.exception;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/16 14:16
 */
public class RetryLaterException extends RuntimeException {
    //如果IConsumer抛出该异常，则说明当前业务无法正常消费，需要等待并重试
}
