package com.rehoshi.transport.job;

import com.rehoshi.transport.datasource.i.DataTableSource;
import com.rehoshi.transport.datasource.model.DataRow;
import com.rehoshi.transport.datasource.model.DataTable;
import com.rehoshi.transport.datasource.model.DynamicDataTable;
import com.rehoshi.transport.datasource.model.StaticDataTable;

import java.util.ArrayList;
import java.util.List;

//数据读取器
public class DataReader {

    private boolean disposed = false;

    //出错的回调
    public interface OnErrorListener {
        void onError(DataReader reader, Exception e);
    }

    //获取到数据的回调
    public interface OnReadDataListener {
        void onReadData(DataReader reader);
    }

    private DataTableSource source;

    //最大数据缓存数量
    private long manxDataCacheSize = 10000;

    private List<DataRow> dataCache = new ArrayList<>();

    private String tableName;
    private long startIndex;
    private long dataCount;
    private int batchSize;
    //抓取的开始索引
    private long fetchStartIndex;

    private OnErrorListener onErrorListener;

    private OnReadDataListener onReadDataListener;

    private DataTable table;

    private volatile boolean reading;

    private final Object readLocker = new Object();


    public DataReader(DataTableSource source, String tableName, long startIndex, long dataCount, int batchSize) {
        this.source = source;
        this.tableName = tableName;
        this.startIndex = startIndex;
        this.dataCount = dataCount;
        this.batchSize = batchSize;
        this.fetchStartIndex = startIndex;
    }

    //开始读取
    public void read() {
        //异步读取
        new Thread(() -> {
            try {
                //获取动态数据表
                DynamicDataTable dataTable = source.getDynamicTableFromIndex(tableName, startIndex);

                this.table = dataTable;


                while (dataTable.hashNextRow()) {

                    DataRow dataRow = dataTable.nextRow();

                    //循环读取加入缓存中
                    addDataRow(dataRow);

                    //缓存满了 开始调用写入
                    if (getDataCacheCount() >= batchSize) {
                        //调用监听器
                        if (onReadDataListener != null) {
                            onReadDataListener.onReadData(this);
                        }

                        //这边再判断一次 防止死锁
                        if (getDataCacheCount() >= batchSize) {
                            //等待写入完成
                            synchronized (readLocker) {
                                readLocker.wait();
                            }
                        }
                    }
                }
                //最后flush一下
                if (getDataCacheCount() > 0) {
                    if (onReadDataListener != null) {
                        onReadDataListener.onReadData(this);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                if (onErrorListener != null && !disposed) {
                    onErrorListener.onError(this, e);
                }
            }
        }).start();
    }

    //继续读
    public void continueRead() {
        synchronized (readLocker) {
            readLocker.notifyAll();
        }
    }

    //同步方法 获取数据 获取完成之后 会从缓存中删除
    public synchronized StaticDataTable fetch(int batchSize) {
        batchSize = dataCache.size() < batchSize ? dataCache.size() : batchSize;
        StaticDataTable table = null;
        if (batchSize > 0) {
            table = new StaticDataTable(this.table);
            List<DataRow> dataRows = dataCache.subList(0, batchSize);
            table.addRows(dataRows);
            dataCache.removeAll(dataRows);
        }
        return table;
    }

    //同步添加数据
    private synchronized void addDataRow(DataRow dataRow) {
        dataCache.add(dataRow);
    }

    //同步 获取数据缓存数量
    private synchronized int getDataCacheCount() {
        return dataCache.size();
    }

    public void setOnErrorListener(OnErrorListener onErrorListener) {
        this.onErrorListener = onErrorListener;
    }

    public void setOnReadDataListener(OnReadDataListener onReadDataListener) {
        this.onReadDataListener = onReadDataListener;
    }

    //释放资源
    public void dispose() {
        dataCache.clear();
        disposed = true;
    }
}
