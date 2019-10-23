package org.koala.db.pool;

import org.koala.db.KoalaConfiguration;
import org.koala.db.exception.ConfigurationException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.sql.Connection;
import java.util.concurrent.TimeUnit;

/**
 * @author shengri
 * @date 9/27/19
 */

public class ConnectionPoolTests {

    private static final String DRIVER_CLASS = "org.postgresql.Driver";
    private static final String JDBS_URL = "jdbc:postgresql://127.0.0.1:5432/postgres";
    private static final String USER_NAME = "postgres";
    private static final String PASSWORD = "postgres";
    private static final String TEST_SQL = "select 1";

    @Test
    public void createConnectionPool() {
        KoalaConfiguration koalaConfiguration = new KoalaConfiguration();

        ConnectionPoolImpl pool = null;

        Assert.assertThrows(ConfigurationException.class,
                () -> new ConnectionPoolImpl(koalaConfiguration)
        );

        koalaConfiguration.setMinIdle(10);
        koalaConfiguration.setMaxIdle(8);

        Assert.assertThrows(ConfigurationException.class,
                () -> new ConnectionPoolImpl(koalaConfiguration)
        );

        koalaConfiguration.setMaxIdle(20);
        koalaConfiguration.setMaxActive(15);

        Assert.assertThrows(ConfigurationException.class,
                () -> new ConnectionPoolImpl(koalaConfiguration)
        );

        koalaConfiguration.setMaxActive(30);

        Assert.assertThrows(ConfigurationException.class,
                () -> new ConnectionPoolImpl(koalaConfiguration)
        );

        koalaConfiguration.setJdbcUrl(JDBS_URL);
        koalaConfiguration.setUserName(USER_NAME);
        koalaConfiguration.setPassword(PASSWORD);

        koalaConfiguration.setTestBeforeReturn(true);
        Assert.assertThrows(ConfigurationException.class,
                () -> new ConnectionPoolImpl(koalaConfiguration)
        );

        koalaConfiguration.setTestSql(TEST_SQL);
    }

    @Test
    public void getConnection() {
        KoalaConfiguration koalaConfiguration = genConfig();

        ConnectionPoolImpl pool = new ConnectionPoolImpl(koalaConfiguration);

        Connection connection = pool.getConnection();

        try {
            TimeUnit.MILLISECONDS.sleep(50);
            connection.close();
        } catch (Exception e) {
            //ignore
        }
    }

    private KoalaConfiguration genConfig() {
        KoalaConfiguration koalaConfiguration = new KoalaConfiguration();

        koalaConfiguration.setMinIdle(3);
        koalaConfiguration.setMaxIdle(5);
        koalaConfiguration.setMaxActive(10);

        koalaConfiguration.setDriverClass(DRIVER_CLASS);
        koalaConfiguration.setJdbcUrl(JDBS_URL);
        koalaConfiguration.setUserName(USER_NAME);
        koalaConfiguration.setPassword(PASSWORD);

        koalaConfiguration.setTestBeforeReturn(true);
        koalaConfiguration.setTestAfterCreation(true);
        koalaConfiguration.setTestWhenChecking(true);

        koalaConfiguration.setTestSql(TEST_SQL);

        return koalaConfiguration;
    }
}
