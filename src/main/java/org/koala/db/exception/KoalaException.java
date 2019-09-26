package org.koala.db.exception;

/**
 * Author: srliu
 * Date: 9/19/19
 */
public class KoalaException extends RuntimeException {

    public KoalaException() {
    }

    public KoalaException(String msg) {
        super(msg);
    }

    public KoalaException(String msg, Throwable t) {
        super(msg,t);
    }
}
