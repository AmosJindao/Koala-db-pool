package org.koala.db.pool;

import org.koala.db.KoalaConfiguration;

import java.sql.Connection;

/**
 * @author shengri
 * @date 10/23/19
 */
public interface ConnectionPool {
    String getName();

    KoalaConfiguration getKoalaConfig();

    int getIdleConnectionNum();

    int getBusyConnectionNum();

    int getAllActiveCount();

    boolean isReady();

    boolean isClosed();

    Connection getConnection();

    void release(Connection connection);

    void shutDown();
}
