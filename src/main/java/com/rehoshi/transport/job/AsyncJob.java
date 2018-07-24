package com.rehoshi.transport.job;

import com.rehoshi.transport.datasource.i.DataTableSource;

import java.util.List;

//读写分离的工作
public class AsyncJob<In extends DataTableSource, Out extends DataTableSource> extends DataTransport<In, Out> {


    public AsyncJob(In source, Out target, String cacheDir, String jobName, String logDir) {
        super(source, target, cacheDir, jobName, logDir);
    }

    @Override
    public boolean transportSingleTable(String tableName) {
        String tag = System.currentTimeMillis() + "";
        Exception exception = null;
        Object locker = new Object();
        try {
            //标签
            //获取源数据数量
            log("开始获取 " + tableName + " 中数据行数");
            jobStatus.totalDataCount = source.getDataRowCount(tableName);
            log("总共 " + jobStatus.totalDataCount + " 条数据");
            jobStatus.tableName = tableName;
            jobStatus.transportMethod = JobStatus.TRANSPORT_METHOD_SINGLE;
            DataReader dataReader = new DataReader(source, tableName, jobStatus.startIndex, jobStatus.totalDataCount, jobStatus.batchSize);
            DataWriter dataWriter = new DataWriter(target, this);
            dataWriter.setOnErrorListener((writer, e) -> {
                target.cancel();
                onTransportError(tag, e);
            });
            dataWriter.setOnCompleteListener((writer) -> {
                //释放资源
                dataReader.dispose();
                dataWriter.dispose();
                source.close();
                target.close();
                onTransportCompleted();
            });
            dataReader.setOnErrorListener((rd, e) -> {
                source.cancel();
                onTransportError(tag, e);
            });
            dataReader.setOnReadDataListener(rd -> {
                dataWriter.continueWrite();
            });
            dataReader.read();
            dataWriter.write(dataReader);
            return true;
        } catch (Exception e) {
            log("出异常了" + e.toString());
            exception = e;
            target.cancel();
            source.close();
            target.close();
//            if (exception != null) {
            onTransportError(tag, exception);
//            }
        }
        return false;
    }

    //迁移锁
    private Object locker = new Object();

    @Override
    public boolean transportAll() {
        String tag = System.currentTimeMillis() + "";
        Exception exception = null;
        try {
            //标签
            //获取源数据数量
            log("开始获取表名列表");
            List<String> allTableNames = source.getAllTableNames();
            jobStatus.tableNameList = allTableNames;
            jobStatus.transportMethod = JobStatus.TRANSPORT_METHOD_LIST;
            log("开始表索引" + jobStatus.tableIndex);
            for (int i = jobStatus.tableIndex; i < jobStatus.tableNameList.size(); i++) {
                String tableName = allTableNames.get(i);
                jobStatus.tableName = tableName;
                jobStatus.tableIndex = i;
                saveJobStatue();
                log("开始获取 " + tableName + " 中数据行数");
                jobStatus.totalDataCount = source.getDataRowCount(tableName);
                log("总共 " + jobStatus.totalDataCount + " 条数据");

                DataReader dataReader = new DataReader(source, tableName, jobStatus.startIndex, jobStatus.totalDataCount, jobStatus.batchSize);
                DataWriter dataWriter = new DataWriter(target, this);

                //错误使用统一的tag 逻辑上一个job只会发生一次错误
                dataWriter.setOnErrorListener((writer, e) -> {
                    target.cancel();
                    onTransportError(tag, e);
                });
                dataReader.setOnErrorListener((rd, e) -> {
                    source.cancel();
                    onTransportError(tag, e);
                });

                //读取到数据 让writer 继续写
                dataReader.setOnReadDataListener(rd -> {
                    dataWriter.continueWrite();
                });
                //写入数据结束
                dataWriter.setOnCompleteListener((writer) -> {
                    //释放资源
                    dataReader.dispose();
                    dataWriter.dispose();
                    //重新设置开始索引
                    jobStatus.startIndex = 1;
                    synchronized (locker) {
                        //开始写入 下一张表
                        locker.notifyAll();
                    }
                });
                dataReader.read();
                dataWriter.write(dataReader);
                //一次只迁移一张表 等待writer 写入完成
                synchronized (locker) {
                    locker.wait();
                }
            }
            exception = null;
            onTransportCompleted();
            log("所有数据迁移完成");
            return true;
        } catch (Exception e) {
            exception = e;
            target.cancel();
        } finally {
            source.close();
            target.close();
            if (exception != null) {
                onTransportError(tag, exception);
            }
        }
        return false;
    }
}
