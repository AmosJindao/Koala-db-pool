package org.koala.utils;

/**
 * Author: srliu
 * Date: 9/12/19
 */
public enum ErrorCode {
    ERR_IDLE_NUM(0, "The minIdle number of connections can not be greater than the maxIdle number of connections!"),
    ERR_MAX_ACTIVE_NUM(1, "The maxIdle number of connections can not be greater than the max active number of connections!"),
    ERR_URL_USERNAME_EMPTY(2, "The jdbcUrl and username can not be empty."),
    ERR_TEST_SQL_EMPTY(3, "The testSql can not be empty."),

    ;

    private int code;
    private String description;

    ErrorCode(int code, String description){
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    public String getDescription() {
        return description;
    }
}
