package com.rehoshi.transport.job;

import com.google.gson.Gson;
import com.rehoshi.config.TransportConfig;
import com.rehoshi.transport.datasource.i.DataTableSource;
import com.rehoshi.transport.datasource.impl.MySqlDataTableSource;
import com.rehoshi.transport.datasource.impl.SqlServerDataDataTableSource;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

@Deprecated
public class AsynJobList {

    private String sourceInPath;

    private String sourceOutPath;

    private String cachePath;

    private AsyncJobListStatus jobListStatus = new AsyncJobListStatus();

    private String name;

    private String savePath;

    private TransportConfig config;

    public AsynJobList(TransportConfig config, String name) {
        this.config = config;
        this.sourceInPath = config.getSourceInPath();
        this.sourceOutPath = config.getSourceOutPath();
        this.cachePath = config.getJobsHomePath();
        this.savePath = cachePath + "/" + name + ".json";
        try {
            AsyncJobListStatus asyncJobListStatus = new Gson().fromJson(new FileReader(this.savePath), AsyncJobListStatus.class);
            if (asyncJobListStatus != null) {
                this.jobListStatus = asyncJobListStatus;
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

    }

    public void transportAll() {
        try {
            DataTableSource in = new SqlServerDataDataTableSource(sourceInPath);
            in.close();
            List<String> allTableNames = in.getAllTableNames();
            for (String tableName : allTableNames) {
                if (!jobListStatus.tableFinishTransport(tableName)) {
                    DataTransport<SqlServerDataDataTableSource, MySqlDataTableSource> dataTransport
                            = new AsyncJob<>(new SqlServerDataDataTableSource(config.getSourceInPath()), new MySqlDataTableSource(config.getSourceOutPath()), config.getJobsHomePath(), this.name + "_" + tableName, config.getLogPath());

                    dataTransport.setOnTransportErrorListener(e -> {
                        try {
                            Thread.sleep(6 * 1000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                        //重新开始
                        dataTransport.autoTransport();
                    });

                    dataTransport.setOnTransportCompletedListener(() -> {
                        jobListStatus.setFinishTransport(tableName, true);
                        saveJobListStatue();
                    });

                    if (dataTransport.loadJobStatus()) {
                        if (!dataTransport.autoTransport()) {
                            dataTransport.transportSingleTable(tableName);
                        }
                    } else {
                        dataTransport.setBatchSize(3000);
                        dataTransport.transportSingleTable(tableName);
                    }
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //保存工作状态
    protected void saveJobListStatue() {
        try {
            String json = new Gson().toJson(this.jobListStatus);
            FileWriter fileWriter = new FileWriter(this.savePath);
            fileWriter.write(json);
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
