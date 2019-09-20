package org.koala.db.exception;

/**
 * Author: srliu
 * Date: 9/12/19
 */
public class ConfigurationException extends KoalaException {
    private int errorCode;

    public ConfigurationException(int errorCode, String errorMessage){
        super(errorMessage);
        this.errorCode = errorCode;
    }

    public int getErrorCode() {
        return errorCode;
    }
}
