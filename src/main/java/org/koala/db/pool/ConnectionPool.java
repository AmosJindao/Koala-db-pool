package org.koala.db.pool;

import org.koala.db.KoalaConfiguration;
import org.koala.db.connection.KoalaConnection;
import org.koala.db.exception.ConfigurationException;
import org.koala.utils.ErrorCode;
import org.koala.utils.StringUtils;

import java.sql.Connection;
import java.util.Deque;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class ConnectionPool {

    private KoalaConfiguration koalaConfig;

    private BlockingDeque<KoalaConnection> idleConns;
    private BlockingDeque<KoalaConnection> busyConns;

    private AtomicInteger idleCount = new AtomicInteger(0);
    private AtomicInteger busyCount = new AtomicInteger(0);
    private AtomicInteger allActiveCount = new AtomicInteger(0);

    public ConnectionPool(KoalaConfiguration koalaConfig) {
        if (koalaConfig.getMinIdle() > koalaConfig.getMaxIdle()) {
            throw new ConfigurationException(ErrorCode.ERR_IDLE_NUM.getCode(), ErrorCode.ERR_IDLE_NUM.getDescription() +
                    " koala.pool.min.idle: " + koalaConfig.getMinIdle() + "; koala.pool.max.idle: " + koalaConfig.getMaxIdle());
        }

        if (koalaConfig.getMaxIdle() > koalaConfig.getMaxActive()) {
            throw new ConfigurationException(ErrorCode.ERR_MAX_ACTIVE_NUM.getCode(), ErrorCode.ERR_MAX_ACTIVE_NUM.getDescription() +
                    " koala.pool.max.idle: " + koalaConfig.getMaxIdle() + "; koala.pool.max.active: " + koalaConfig.getMaxActive());
        }

        if (StringUtils.isBlank(koalaConfig.getJdbcUrl()) || StringUtils.isBlank(koalaConfig.getUserName())) {
            throw new ConfigurationException(ErrorCode.ERR_URL_USERNAME_EMPTY.getCode(), ErrorCode.ERR_URL_USERNAME_EMPTY.getDescription() +
                    " koala.connection.jdbc.url: " + koalaConfig.getJdbcUrl() + "; koala.connection.user.name: " + koalaConfig.getUserName());
        }

        if (StringUtils.isBlank(koalaConfig.getPassword())) {
            koalaConfig.setPassword("");
        }

        if (StringUtils.isBlank(koalaConfig.getTestSql())) {
            throw new ConfigurationException(ErrorCode.ERR_TEST_SQL_EMPTY.getCode(), ErrorCode.ERR_TEST_SQL_EMPTY.getDescription() +
                    " koala.connection.test.sql: " + koalaConfig.getTestSql());
        }

        this.koalaConfig = koalaConfig;

        idleConns = new LinkedBlockingDeque<>();
        busyConns = new LinkedBlockingDeque<>();
    }

    public synchronized Connection getConnection() {
        KoalaConnection koalaConnection = idleConns.poll();
        if (koalaConnection != null) {
            busyConns.offer(koalaConnection);
            idleCount.decrementAndGet();
            busyCount.incrementAndGet();


        }

        return null;
    }

    public synchronized void release(Connection connection) {

    }

}
