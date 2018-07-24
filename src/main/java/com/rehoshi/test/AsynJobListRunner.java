package com.rehoshi.test;

import com.rehoshi.config.TransportConfig;
import com.rehoshi.transport.job.AsynJobList;

public class AsynJobListRunner {
    public static void main(String[] args) {
        AsynJobList jobList = new AsynJobList(new TransportConfig(true), "listTrans");
        jobList.transportAll();
    }
}
