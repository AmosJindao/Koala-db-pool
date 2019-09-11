package org.koala.db.connection;

import org.koala.db.KoalaConfig;
import org.koala.utils.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

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

    private KoalaConfig koalaConfig;

    private KoalaConnection(KoalaConfig koalaConfig) {
        this.koalaConfig = koalaConfig;
    }

    private Connection createConnection() throws SQLException, ClassNotFoundException {
        if (StringUtils.isNotBlank(koalaConfig.getDriverClass())) {
            Class.forName(koalaConfig.getDriverClass());
        }

        Connection conn = DriverManager.getConnection(koalaConfig.getJdbcUrl(), koalaConfig.getUserName(), koalaConfig.getPassword());

        return conn;
    }

}
