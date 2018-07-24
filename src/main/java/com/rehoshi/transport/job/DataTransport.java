package com.rehoshi.transport.job;

import com.google.gson.Gson;
import com.rehoshi.transport.datasource.i.DataTableSource;
import com.rehoshi.utils.Logger;
import com.rehoshi.utils.TextUtil;

import java.io.*;

//传输任务 的基础类
public abstract class DataTransport<In extends DataTableSource, Out extends DataTableSource> {

    public interface OnTransportErrorListener {
        void onTransportError(Exception e);
    }

    public interface OnTransportCompletedListener {
        void onTransportCompleted();
    }

    //输入源
    protected In source;

    //输出源
    protected Out target;

    //
    protected String savePath;

    protected JobStatus jobStatus = new JobStatus();

    protected OnTransportErrorListener onTransportErrorListener;

    protected OnTransportCompletedListener onTransportCompletedListener;

    private Logger logger;

    public DataTransport(In source, Out target, String cacheDir, String jobName, String logDir) {
        this.source = source;
        this.target = target;
        this.savePath = cacheDir + File.separator + "job_" + jobName + ".json";
        this.logger = new Logger(logDir + File.separator + jobName + ".log");
    }

    //加载工作状态
    public boolean loadJobStatus() {
        try {
            JobStatus jobStatus = new Gson().fromJson(new FileReader(this.savePath), JobStatus.class);
            if (jobStatus != null) {
                this.jobStatus = jobStatus;
                return true;
            }
        } catch (FileNotFoundException e) {
//            e.printStackTrace();
        }
        return false;
    }

    //自动开始迁移数据
    public boolean autoTransport() {
        boolean start = false;
        switch (this.jobStatus.transportMethod) {
            case JobStatus.TRANSPORT_METHOD_SINGLE:
                if (!TextUtil.isEmptyOrNull(this.jobStatus.tableName)) {
                    start = true;
                    transportSingleTable(this.jobStatus.tableName);
                }
                break;
            case JobStatus.TRANSPORT_METHOD_LIST:
                transportAll();
                break;
        }
        return start;
    }

    //迁移一张表
    public abstract boolean transportSingleTable(String tableName);

    //迁移所有
    public abstract boolean transportAll();

    //保存工作状态
    protected void saveJobStatue() {
        try {
            String json = new Gson().toJson(this.jobStatus);
            FileWriter fileWriter = new FileWriter(this.savePath);
            fileWriter.write(json);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void onTransportError(Exception e) {
        log(e.toString());
        if (onTransportErrorListener != null) {
            onTransportErrorListener.onTransportError(e);
        }
    }

    private String preTag;

    //tag 用作标识
    protected void onTransportError(String tag, Exception e) {
        log(e.toString());
        if (preTag == null || !preTag.equals(tag)) {
            preTag = tag;
            onTransportError(e);
        }
    }

    public In getSource() {
        return source;
    }

    public Out getTarget() {
        return target;
    }

    public int getBatchSize() {
        return jobStatus.batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.jobStatus.batchSize = batchSize;
    }

    public void setOnTransportErrorListener(OnTransportErrorListener onTransportErrorListener) {
        this.onTransportErrorListener = onTransportErrorListener;
    }

    public void setOnTransportCompletedListener(OnTransportCompletedListener onTransportCompletedListener) {
        this.onTransportCompletedListener = onTransportCompletedListener;
    }

    protected void onTransportCompleted() {
        if (this.onTransportCompletedListener != null) {
            this.onTransportCompletedListener.onTransportCompleted();
        }
    }

    protected void log(String msg) {
        logger.log(msg);
    }

    public void setStartIndex(long startIndex) {
        this.jobStatus.startIndex = startIndex;
    }

    public long getStartIndex() {
        return this.jobStatus.startIndex;
    }

    public long getTotalDataCount() {
        return jobStatus.totalDataCount;
    }


    public String getTableName() {
        return jobStatus.tableName;
    }
}
