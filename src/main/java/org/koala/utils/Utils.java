package org.koala.utils;

import java.util.concurrent.TimeUnit;

/**
 * @author shengri
 * @date 11/5/19
 */
public class Utils {

    public static void sleepSilence(long millis){
        try {
            TimeUnit.MILLISECONDS.sleep(millis);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
