package com.rehoshi.utils;

public class TextUtil {
    public static boolean isEmptyOrNull(String text) {
        return text == null || text.trim().equalsIgnoreCase("");
    }
}
