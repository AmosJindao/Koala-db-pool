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

    public ConnectionPool(KoalaConfig koalaConfig) {
        this.koalaConfig = koalaConfig;
    }

    public synchronized Connection getConnection() {
        return null;
    }



}
