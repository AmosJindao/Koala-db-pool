package org.koala.db.exception;

/**
 * Author: srliu
 * Date: 9/19/19
 */
public class ConnectionException extends KoalaException {
    public ConnectionException(String msg) {
        super(msg);
    }
    public ConnectionException(String msg, Throwable t) {
        super(msg,t);
    }
}
