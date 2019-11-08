package org.koala.db.pool;

import org.koala.db.KoalaConfiguration;
import org.koala.db.connection.KoalaConnection;
import org.koala.db.exception.ConfigurationException;
import org.koala.db.exception.PoolException;
import org.koala.utils.ErrorCode;
import org.koala.utils.StringUtils;
import org.koala.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class ConnectionPoolImpl implements ConnectionPool {
    private static Logger LOG = LoggerFactory.getLogger(ConnectionPoolImpl.class);

    private static final long FREE_CHECK_PERIOD = 5 * 60 * 1000l;//5 mins

    public static final int POOL_STATUS_INITIALIZING = 0;
    public static final int POOL_STATUS_READY = 1;
    public static final int POOL_STATUS_CLOSED = 2;

    private String name;

    private KoalaConfiguration koalaConfig;

    private List<KoalaConnection> connList;
    private AtomicInteger rusedCount = new AtomicInteger(0);
    private AtomicInteger allActiveCount = new AtomicInteger(0);

    private ExecutorService executorService = Executors.newFixedThreadPool(2);

    private volatile int status = POOL_STATUS_INITIALIZING;

    public ConnectionPoolImpl(KoalaConfiguration koalaConfig) {
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

        if (koalaConfig.isTestNeeded()
                && StringUtils.isBlank(koalaConfig.getTestSql())) {
            throw new ConfigurationException(ErrorCode.ERR_TEST_SQL_EMPTY.getCode(), ErrorCode.ERR_TEST_SQL_EMPTY.getDescription() +
                    " koala.connection.test.sql: " + koalaConfig.getTestSql());
        }

        if (StringUtils.isNotBlank(koalaConfig.getPoolName())) {
            this.name = koalaConfig.getPoolName();
        } else {
            this.name = this.getClass().getSimpleName() + "_" + UUID.randomUUID().toString();
        }

        this.koalaConfig = koalaConfig;

        if (StringUtils.isNotBlank(koalaConfig.getDriverClass())) {
            try {
                Class.forName(koalaConfig.getDriverClass());
            } catch (ClassNotFoundException e) {
                throw new PoolException("Database driver class was not found, class: " + this.koalaConfig.getDriverClass(), e);
            }
        }

//        idleConns = new LinkedBlockingDeque<>();
//        busyConns = new LinkedBlockingDeque<>();

        connList = new ArrayList<>(this.koalaConfig.getMinIdle());

        executorService.submit(new PoolKeeper());
    }

    @Override
    public synchronized Connection getConnection() {
        Iterator<KoalaConnection> connIter = connList.iterator();
        while (connIter.hasNext()) {
            KoalaConnection tmpConn = connIter.next();
            if (tmpConn.isIdle()) {
                if (tmpConn.compareAndSetStatus(KoalaConnection.CONN_STATUS_IDLE, KoalaConnection.CONN_STATUS_BUSY)) {
                    boolean testPass = true;
                    try {
                        if (koalaConfig.isTestBeforeReturn() && tmpConn.isNormal() &&
                                tmpConn.getLastCheckedMillis() - System.currentTimeMillis() > FREE_CHECK_PERIOD) {
                            tmpConn.checkConnection();
                        }
                    } catch (Exception e) {
                        testPass = false;
                        LOG.error("Connection test failed!", e);
                    }
                    if (testPass) {
                        rusedCount.incrementAndGet();

                        return tmpConn;
                    } else {
                        connIter.remove();
                        closeKoalaConnection(tmpConn);
                    }
                }
            }
        }

        while (true) {
            int allCount = allActiveCount.get();
            if (allCount < this.koalaConfig.getMaxActive()) {
                if (allActiveCount.compareAndSet(allCount, allCount + 1)) {
                    return doCreateConnection(this.koalaConfig.getCreationTimeOutSeconds() * 1000);
                }
            } else {
                break;
            }
        }

        return null;
    }

    private KoalaConnection doCreateConnection(long timeOutMillis) {
        while (true) {
            try {
                KoalaConnection newConn = new KoalaConnection(this);

                return newConn;
            } catch (Exception e) {
                LOG.error("Create a new connnection error! ", e);
                Utils.sleepSilence(100);
            }
        }
    }

    @Override
    public synchronized void release(Connection connection) {
        if (connection instanceof KoalaConnection) {
            KoalaConnection tmpConn = (KoalaConnection) connection;

            if (getIdleConnectionNum() < koalaConfig.getMaxIdle()) {
                if (tmpConn.compareAndSetStatus(KoalaConnection.CONN_STATUS_BUSY, KoalaConnection.CONN_STATUS_IDLE)) {
                    tmpConn.setLastStatusChangeMillis(System.currentTimeMillis());
                }
            } else {
                if (tmpConn.compareAndSetStatus(KoalaConnection.CONN_STATUS_BUSY, KoalaConnection.CONN_STATUS_CLOSED)) {
                    if (connList.remove(tmpConn)) {
                        allActiveCount.decrementAndGet();
                    }
                    closeKoalaConnection(tmpConn);
                }
            }
        } else {
            try {
                connection.close();
            } catch (SQLException e) {
                //ignore
            }
        }
    }

    private void closeKoalaConnection(KoalaConnection koalaConnection) {
        koalaConnection.setPool(null);
        try {
            koalaConnection.close();
        } catch (SQLException e) {
            //ignore
        }
    }

    @Override
    public void shutDown() {
        this.status = POOL_STATUS_CLOSED;
        this.executorService.shutdown();

        this.executorService = null;
    }

    public KoalaConnection createKoalaConnection() {
        KoalaConnection newConn = doCreateConnection(-1l);
        if (newConn != null) return newConn;

        return null;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isReady() {
        return status == POOL_STATUS_READY;
    }

    @Override
    public boolean isClosed() {
        return this.status == POOL_STATUS_CLOSED;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public KoalaConfiguration getKoalaConfig() {
        return koalaConfig;
    }

    public void setKoalaConfig(KoalaConfiguration koalaConfig) {
        this.koalaConfig = koalaConfig;
    }

    @Override
    public int getAllActiveCount() {
        return allActiveCount.get();
    }

    public void setAllActiveCount(AtomicInteger allActiveCount) {
        this.allActiveCount = allActiveCount;
    }

    @Override
    public int getIdleConnectionNum() {
        return countConnection(KoalaConnection.CONN_STATUS_IDLE);
    }

    @Override
    public int getBusyConnectionNum() {
        return countConnection(KoalaConnection.CONN_STATUS_BUSY);
    }

    private int countConnection(int status) {
        if (connList == null) {
            return 0;
        }

        int count = 0;
        for (KoalaConnection koalaConnection : connList) {
            if (koalaConnection.getStatus() == status) {
                count++;
            }
        }

        return count;
    }

    private class PoolKeeper implements Runnable {

        @Override
        public void run() {
            while (!isClosed()) {
                if (connList.size() < getKoalaConfig().getMinIdle()) {
                    KoalaConnection koalaConnection = createKoalaConnection();

                    if (koalaConnection != null && koalaConnection.isNormal()) {
                        connList.add(koalaConnection);
                    }
                } else if (getIdleConnectionNum() > getKoalaConfig().getMinIdle()) {
                    Iterator<KoalaConnection> connIter = connList.iterator();
                    while (connIter.hasNext()) {
                        KoalaConnection tmpConn = connIter.next();

                        if (tmpConn.isIdle() &&
                                (System.currentTimeMillis() - tmpConn.getLastStatusChangeMillis() >= getKoalaConfig().getMaxIdleSeconds() * 1000) &&
                                connList.size() > getKoalaConfig().getMinIdle()) {
                            if (tmpConn.compareAndSetStatus(KoalaConnection.CONN_STATUS_IDLE, KoalaConnection.CONN_STATUS_CLOSED)) {
                                connIter.remove();
                                allActiveCount.decrementAndGet();

                                closeKoalaConnection(tmpConn);
                            }
                        }
                    }
                }

                Utils.sleepSilence(500);
            }
        }
    }
}
