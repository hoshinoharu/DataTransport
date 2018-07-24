package com.rehoshi.utils;

public class DBUtil {
    public static void close(AutoCloseable... closeables){
        if(closeables != null){
            for (AutoCloseable autoCloseable : closeables){
                if(autoCloseable != null){
                    try {
                        autoCloseable.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
