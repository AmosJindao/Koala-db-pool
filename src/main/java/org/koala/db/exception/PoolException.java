package org.koala.db.exception;

/**
 * Author: srliu
 * Date: 9/26/19
 */
public class PoolException extends KoalaException {
    public PoolException(String msg) {
        super(msg);
    }
    public PoolException(String msg, Throwable t) {
        super(msg,t);
    }
}
