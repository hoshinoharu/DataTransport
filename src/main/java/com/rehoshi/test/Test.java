package com.rehoshi.test;

import com.google.gson.Gson;
import com.rehoshi.config.TransportConfig;
import com.rehoshi.transport.datasource.impl.MySqlDataTableSource;
import com.rehoshi.transport.datasource.impl.SqlServerDataDataTableSource;
import com.rehoshi.transport.job.AsyncJob;
import com.rehoshi.transport.job.DataTransport;
import com.rehoshi.transport.job.JobInfo;

import java.io.FileNotFoundException;
import java.io.FileReader;

public class Test {
    public static void main(String[] args) {
        //获取配置文件
        TransportConfig config = new TransportConfig(true);

        //配置数据源
        SqlServerDataDataTableSource sqlServerDS = new SqlServerDataDataTableSource(
                config.getSourceInPath()
        );
        MySqlDataTableSource mySqlDataSource = new MySqlDataTableSource(
                config.getSourceOutPath()
        );


        //加载工作信息
        JobInfo jobInfo = null;
        try {
            jobInfo = new Gson().fromJson(new FileReader(config.getJobInfoPath()), JobInfo.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //创建传输任务
        DataTransport<SqlServerDataDataTableSource, MySqlDataTableSource> dataTransport
                = new AsyncJob<>(sqlServerDS, mySqlDataSource, config.getJobsHomePath(), jobInfo.name, config.getLogPath());

        //报错之后尝试重启任务 针对 网络错误 设置可以继续开始迁移
        dataTransport.setOnTransportErrorListener(e -> {
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            //重新开始
            dataTransport.autoTransport();
        });

        //如果成功加载工作状态
        if (dataTransport.loadJobStatus()) {
            //自动开始
            dataTransport.autoTransport();
        } else {
            //设置 一次插入的大小 DataReader的缓存大小
            dataTransport.setBatchSize(3000);
            //传输全部数据表
            dataTransport.transportAll();
        }
    }
}
