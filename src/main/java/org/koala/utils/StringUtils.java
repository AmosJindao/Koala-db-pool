package org.koala.utils;

/**
 * Author: srliu
 * Date: 9/9/19
 */
public final class StringUtils {
    private StringUtils() {
    }

    public static boolean isBlank( CharSequence cs ) {
        if (cs == null || cs.length() == 0) {
            return true;
        }

        int len = cs.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean isNotBlank(final CharSequence cs) {
        return !isBlank(cs);
    }
}
