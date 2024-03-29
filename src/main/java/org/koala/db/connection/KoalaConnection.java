package org.koala.db.connection;

import org.koala.db.KoalaConfiguration;
import org.koala.db.exception.ConnectionException;
import org.koala.db.pool.ConnectionPool;
import org.koala.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class KoalaConnection implements Connection {

    private static Logger LOG = LoggerFactory.getLogger(KoalaConnection.class);

//    public static final int CONN_STATUS_INITIALIZING = 0;
    public static final int CONN_STATUS_IDLE = 0;
    public static final int CONN_STATUS_BUSY = 1;
    public static final int CONN_STATUS_CLOSED = 2;

    private volatile int status;
    private volatile long lastCheckedMillis;
    private volatile long lastStatusChangeMillis;

    private ConnectionPool pool;
    private Connection connection;

    private AtomicIntegerFieldUpdater<KoalaConnection> statusUpdater =
            AtomicIntegerFieldUpdater.newUpdater(KoalaConnection.class, "status");

    public KoalaConnection(ConnectionPool pool) {
        this.pool = pool;

        KoalaConfiguration koalaConfig = pool.getKoalaConfig();

        try {
            this.connection = DriverManager.getConnection(koalaConfig.getJdbcUrl(), koalaConfig.getUserName(), koalaConfig.getPassword());

            if (koalaConfig.isTestAfterCreation()) {
                checkConnection();
            }

            setAutoCommit(koalaConfig.isAutoCommit());
            setReadOnly(koalaConfig.isReadOnly());
        } catch (SQLException e) {
            if (this.connection != null) {
                try {
                    this.connection.close();
                    this.connection = null;
                } catch (SQLException e1) {
                    //ignore
                }
            }
            throw new ConnectionException("Error in creation!", e);
        }
        this.status = CONN_STATUS_IDLE;
        lastStatusChangeMillis = System.currentTimeMillis();
    }

    public int getStatus() {
        return status;
    }

    public boolean compareAndSetStatus(int expect, int update) {
        return this.statusUpdater.compareAndSet(this, expect, update);
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public ConnectionPool getPool() {
        return pool;
    }

    public void setPool(ConnectionPool pool) {
        this.pool = pool;
    }

    public long getLastCheckedMillis() {
        return lastCheckedMillis;
    }

    public void setLastCheckedMillis(long lastCheckedMillis) {
        this.lastCheckedMillis = lastCheckedMillis;
    }

    public long getLastStatusChangeMillis() {
        return lastStatusChangeMillis;
    }

    public void setLastStatusChangeMillis(long lastStatusChangeMillis) {
        this.lastStatusChangeMillis = lastStatusChangeMillis;
    }

    public boolean isNormal() {
        return this.status != CONN_STATUS_CLOSED && this.connection != null;
    }

    public boolean isIdle() {
        return this.status == CONN_STATUS_IDLE && this.connection != null;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return this.status == CONN_STATUS_CLOSED || this.connection == null || connection.isClosed();
    }

    @Override
    public void close() throws SQLException {
        if (pool != null) {
            pool.release(this);
        } else {
            this.status = CONN_STATUS_CLOSED;

            connection.close();

            connection = null;
        }
    }

    public void checkConnection() {
        KoalaConfiguration koalaConfig = pool.getKoalaConfig();

        if (StringUtils.isNotBlank(koalaConfig.getTestSql())) {
            try {
                PreparedStatement ps = this.connection.prepareStatement(koalaConfig.getTestSql());
                ps.execute();
                ps.close();

                setLastCheckedMillis(System.currentTimeMillis());
            } catch (SQLException e) {
                setLastCheckedMillis(0);
                this.status = CONN_STATUS_CLOSED;
                LOG.warn("The connection fails to pass the test, and will be disposed! pool name: {}, test sql: {}",
                        this.pool.getName(), koalaConfig.getTestSql(), e);

                throw new ConnectionException("Connection test failed!", e);
            }
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        return connection.createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return connection.prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        return connection.prepareCall(sql);
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return connection.nativeSQL(sql);
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        connection.setAutoCommit(autoCommit);
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return connection.getAutoCommit();
    }

    @Override
    public void commit() throws SQLException {
        connection.commit();
    }

    @Override
    public void rollback() throws SQLException {
        connection.rollback();
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return connection.getMetaData();
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {
        connection.setReadOnly(readOnly);
    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return connection.isReadOnly();
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {
        connection.setCatalog(catalog);
    }

    @Override
    public String getCatalog() throws SQLException {
        return connection.getCatalog();
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        connection.setTransactionIsolation(level);
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return connection.getTransactionIsolation();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return connection.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        connection.clearWarnings();
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return connection.getTypeMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
        connection.setTypeMap(map);
    }

    @Override
    public void setHoldability(int holdability) throws SQLException {
        connection.setHoldability(holdability);
    }

    @Override
    public int getHoldability() throws SQLException {
        return connection.getHoldability();
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
        return connection.setSavepoint();
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        return connection.setSavepoint(name);
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        connection.rollback(savepoint);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {
        connection.releaseSavepoint(savepoint);
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return connection.prepareStatement(sql, autoGeneratedKeys);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return connection.prepareStatement(sql, columnIndexes);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return connection.prepareStatement(sql, columnNames);
    }

    @Override
    public Clob createClob() throws SQLException {
        return connection.createClob();
    }

    @Override
    public Blob createBlob() throws SQLException {
        return connection.createBlob();
    }

    @Override
    public NClob createNClob() throws SQLException {
        return connection.createNClob();
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return connection.createSQLXML();
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return connection.isValid(timeout);
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        connection.setClientInfo(name, value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        connection.setClientInfo(properties);
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return connection.getClientInfo(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return connection.getClientInfo();
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return connection.createArrayOf(typeName, elements);
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return connection.createStruct(typeName, attributes);
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        connection.setSchema(schema);
    }

    @Override
    public String getSchema() throws SQLException {
        return connection.getSchema();
    }

    @Override
    public void abort(Executor executor) throws SQLException {
        connection.abort(executor);
    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
        connection.setNetworkTimeout(executor, milliseconds);
    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return connection.getNetworkTimeout();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return connection.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return connection.isWrapperFor(iface);
    }
}
