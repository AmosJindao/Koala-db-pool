package org.koala.db;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class KoalaConfiguration {
    private String poolName;

    private int minIdle;
    private int maxIdle;
    private int maxIdleSeconds;
    private int maxActive;

    private long creationTimeOutSeconds;

    private boolean testBeforeReturn = false;
    private boolean testAfterCreation = false;
    private boolean testWhenChecking = false;

//    private DataSource dataSource;

    private String driverClass;

    private String jdbcUrl;
    private String userName;
    private String password;

    private String testSql;
    private boolean readOnly = false;
    private boolean autoCommit = true;

    public boolean isTestNeeded() {
        return isTestBeforeReturn() || isTestAfterCreation() || isTestWhenChecking();
    }

    public boolean isTestBeforeReturn() {
        return testBeforeReturn;
    }

    public void setTestBeforeReturn(boolean testBeforeReturn) {
        this.testBeforeReturn = testBeforeReturn;
    }

    public boolean isTestAfterCreation() {
        return testAfterCreation;
    }

    public void setTestAfterCreation(boolean testAfterCreation) {
        this.testAfterCreation = testAfterCreation;
    }

    public boolean isTestWhenChecking() {
        return testWhenChecking;
    }

    public void setTestWhenChecking(boolean testWhenChecking) {
        this.testWhenChecking = testWhenChecking;
    }

    public String getTestSql() {
        return testSql;
    }

    public void setTestSql(String testSql) {
        this.testSql = testSql;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public void setMinIdle(int minIdle) {
        if (minIdle < 0) {
            this.minIdle = 0;
        } else {
            this.minIdle = minIdle;
        }
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public void setMaxIdle(int maxIdle) {
        if (maxIdle < 0) {
            this.maxIdle = 0;
        } else {
            this.maxIdle = maxIdle;
        }
    }

    public int getMaxIdleSeconds() {
        return maxIdleSeconds;
    }

    public void setMaxIdleSeconds(int maxIdleSeconds) {
        this.maxIdleSeconds = maxIdleSeconds;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public void setMaxActive(int maxActive) {
        if (maxActive <= 0) {
            this.maxActive = 1;
        } else {
            this.maxActive = maxActive;
        }
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public void setDriverClass(String driverClass) {
        this.driverClass = driverClass;
    }

    public String getPoolName() {
        return poolName;
    }

    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }

    public long getCreationTimeOutSeconds() {
        return creationTimeOutSeconds;
    }

    public void setCreationTimeOutSeconds(long creationTimeOutSeconds) {
        this.creationTimeOutSeconds = creationTimeOutSeconds;
    }
}
