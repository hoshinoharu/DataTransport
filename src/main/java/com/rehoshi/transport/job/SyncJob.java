package com.rehoshi.transport.job;

import com.rehoshi.transport.datasource.model.StaticDataTable;
import com.rehoshi.transport.datasource.i.DataTableSource;

import java.io.File;
import java.util.List;

//单线程的Job
@Deprecated
public class SyncJob<In extends DataTableSource, Out extends DataTableSource> extends DataTransport<In, Out> {


    public SyncJob(In source, Out target, String cacheDir, String jobName, String logDir) {
        super(source, target, cacheDir, jobName, logDir);
    }

    private boolean transportTable(String tableName, long startIndex, long dataCount, int batchSize) throws Exception {
        long start = startIndex;
        while (start <= dataCount) {
            long end = start + batchSize - 1;
            log("开始获取 " + start + " - " + end + " 数据");
            StaticDataTable inRange = source.getInRange(tableName, start, end);
            log("数据获取成功");
            log("开始批量插入");
            boolean add = target.add(inRange);
            if (add) {
                //保存修改
                target.save();
                int size = inRange.getRowList().size();
                start += size;
                jobStatus.startIndex = start;
                log(tableName + "表中的" + (start - 1) + "条数据迁移完成\n");
                saveJobStatue();
            } else {
                //取消修改
                target.cancel();
                //插入失败
                return false;
            }
        }
        log(tableName + "表中的全部数据迁移完成, 共" + (start - 1) + "条\n");
        return true;
    }

    @Override
    public boolean transportSingleTable(String tableName) {
        Exception exception = null;
        try {
            //获取源数据数量
            jobStatus.totalDataCount = source.getDataRowCount(tableName);
            jobStatus.tableName = tableName;
            jobStatus.transportMethod = JobStatus.TRANSPORT_METHOD_SINGLE;
            boolean b = transportTable(tableName, jobStatus.startIndex, jobStatus.totalDataCount, jobStatus.batchSize);
            if (b) {
                //删除保存的工作文件
                File file = new File(this.savePath);
                file.deleteOnExit();
            }
            return b;
        } catch (Exception e) {
            exception = e;
            target.cancel();
        } finally {
            source.close();
            target.close();
            if (exception != null) {
                onTransportError(exception);
            }
        }
        return false;
    }

    @Override
    public boolean transportAll() {
        Exception exception = null;
        try {
            log("开始获取迁移列表...");
            List<String> allTableNames = source.getAllTableNames();
            log("获取迁移列表成功");
            jobStatus.tableNameList = allTableNames;
            jobStatus.transportMethod = JobStatus.TRANSPORT_METHOD_LIST;
            for (int i = jobStatus.tableIndex; i < jobStatus.tableNameList.size(); i++) {
                String tableName = jobStatus.tableNameList.get(i);
                jobStatus.tableIndex = i;
                log("开始获取" + tableName + "表数据行数");
                jobStatus.totalDataCount = source.getDataRowCount(tableName);
                log(tableName + "表数据行数为:" + jobStatus.totalDataCount);
                jobStatus.tableName = tableName;
                log("开始迁移" + tableName);
                if (!transportTable(tableName, jobStatus.startIndex, jobStatus.totalDataCount, jobStatus.batchSize)) {
                    return false;
                }
                //完成工作后 重新将开始索引设置为0
                jobStatus.startIndex = 1;
            }
            //删除保存的工作文件
            File file = new File(this.savePath);
            file.deleteOnExit();
            return true;
        } catch (Exception e) {
            exception = e;
            target.cancel();
        } finally {
            source.close();
            target.close();
            if (exception != null) {
                onTransportError(exception);
            }
        }
        return false;
    }
}
