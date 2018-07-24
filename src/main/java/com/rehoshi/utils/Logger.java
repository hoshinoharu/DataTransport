package com.rehoshi.utils;

import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Logger {
    private static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private String logPath;

    public Logger(String logPath) {
        this.logPath = logPath;
    }

    public void log(String msg) {
        String log = dateFormat.format(new Date()) + " : " + msg;
        log = log.replaceAll("\n", "\r\n") + "\r\n";
        try {
            FileWriter fileWriter = null;
            fileWriter = new FileWriter(logPath, true);
            fileWriter.write(log);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
