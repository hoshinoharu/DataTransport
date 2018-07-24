package com.rehoshi.transport.job;

import java.util.HashMap;
import java.util.Map;

//jobList的工作状态
public class AsyncJobListStatus {

    public Map<String, Boolean> finishMap = new HashMap<>();

    public Map<String, JobStatus> statusMap = new HashMap<>();

    public boolean tableFinishTransport(String tableName) {
        return finishMap.get(tableName);
    }

    public void setFinishTransport(String tableName, boolean finish) {
        finishMap.put(tableName, finish);
    }

    public JobStatus getJobStatus(String tableName) {
        return statusMap.get(tableName);
    }

    public void saveJobStatus(String tableName, JobStatus jobStatus) {
        statusMap.put(tableName, jobStatus);
    }
}
