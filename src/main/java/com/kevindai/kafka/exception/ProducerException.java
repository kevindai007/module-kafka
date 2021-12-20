package com.kevindai.kafka.exception;

/**
 * @author kevindai
 * @Desc
 * @date 2021/12/14 08:56
 */
public class ProducerException extends Exception {

    private boolean invalidParam = false ;
    private boolean fatal = false;

    public ProducerException(String msg) {
        super(msg);
    }

    public ProducerException(Exception e) {
        super(e);
    }

    public static ProducerException invalidParam(final String msg) {
        final ProducerException e = new ProducerException(msg);
        e.invalidParam = true;
        return e;
    }

    public static ProducerException fatal(Exception parent) {
        final ProducerException e = new ProducerException(parent);
        e.fatal = true;
        return e;
    }

    public boolean isInvalidParam() {
        return invalidParam;
    }

    /**
     * 是否是致命错误（即重试发送也无济于事的）
     */
    public boolean isFatal() {
        return fatal;
    }
}
