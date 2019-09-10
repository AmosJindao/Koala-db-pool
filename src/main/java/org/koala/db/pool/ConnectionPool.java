package org.koala.db.pool;

import org.koala.db.KoalaConfig;
import org.koala.utils.StringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public class ConnectionPool {

    private KoalaConfig koalaConfig;



    private Connection createConnection() throws SQLException, ClassNotFoundException {
        if (StringUtils.isNotBlank(koalaConfig.getDriverClass())) {
            Class.forName(koalaConfig.getDriverClass());
        }

        Connection conn = DriverManager.getConnection(koalaConfig.getJdbcUrl(), koalaConfig.getUserName(), koalaConfig.getPassword());

        return conn;
    }
}
