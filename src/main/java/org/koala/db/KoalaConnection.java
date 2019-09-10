package org.koala.db;

import java.sql.Connection;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class KoalaConnection {

    public static final int CONN_STATE_IDLE = 0;
    public static final int CONN_STATE_BUSY = 1;
    public static final int CONN_STATE_EXPIRED = 2;
    public static final int CONN_STATE_RELEASE = 3;

    private volatile int state;

    private Connection connection;


}
