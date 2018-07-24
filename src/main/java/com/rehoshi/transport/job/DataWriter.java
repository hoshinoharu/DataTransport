package com.rehoshi.transport.job;

import com.rehoshi.transport.datasource.model.StaticDataTable;
import com.rehoshi.transport.datasource.i.DataTableSource;

//异步数据写入
public class DataWriter {

    private AsyncJob job;

    public interface OnErrorListener {
        void onWriteError(DataWriter dataWriter, Exception e);
    }

    //写入完成回调
    public interface OnCompleteListener {
        void onComplete(DataWriter dataWriter);
    }

    private OnErrorListener onErrorListener;

    private OnCompleteListener onCompleteListener;

    private DataTableSource source;

    private Object locker = new Object();

    public DataWriter(DataTableSource source, AsyncJob job) {
        this.source = source;
        this.job = job;
    }

    public boolean writeTable(StaticDataTable table) throws Exception {
        return source.add(table);
    }

    public void setOnErrorListener(OnErrorListener onErrorListener) {
        this.onErrorListener = onErrorListener;
    }

    //开始写入
    public void write(DataReader dataReader) {
        new Thread(() -> {
            try {
                //开启同步锁
                synchronized (locker) {

                    //获取之前保存的状态
                    job.log("开始迁移 " + job.getTableName());
                    long start = job.getStartIndex();
                    int batchSize = job.getBatchSize();
                    long dataCount = job.getTotalDataCount();

                    //如果未完成 继续工作
                    while (start <= dataCount) {
                        //抓取数据
                        StaticDataTable fetch = dataReader.fetch(batchSize);

                        if (fetch != null) {
                            int rowSize = fetch.getRowList().size();
                            long startIndex = start;
                            long endIndex = (startIndex + rowSize - 1);
                            job.log("获取到" + fetch.getTableName() + "中的 " + startIndex + " - " + endIndex + "条数据");
                            //插入数据
                            if (source.add(fetch)) {
                                //保存
                                source.save();
                                start += rowSize;
                                //保存job的工作状态
                                job.setStartIndex(start);
                                job.saveJobStatue();
                                job.log(startIndex + " - " + endIndex + " 数据插入完成\n");
                            } else {
                                source.cancel();
                                throw new Exception("数据插入失败");
                            }
                        } else {
                            job.log("暂未获取到数据 等待数据读入");
                            //没有数据就等待
                            locker.wait();
                        }

                        //这里再次唤醒一波阅读器
                        dataReader.continueRead();
                    }
                    job.log(job.getTableName() + "表迁移完成\n");
                    if (this.onCompleteListener != null) {
                        this.onCompleteListener.onComplete(this);
                    }
                }
            } catch (Exception e) {
                job.onTransportError(e);
            }
        }).start();
    }

    public void setOnCompleteListener(OnCompleteListener onCompleteListener) {
        this.onCompleteListener = onCompleteListener;
    }

    //继续写入
    public void continueWrite() {
        synchronized (locker) {
            job.log("接收到新数据");
            this.locker.notifyAll();
        }
    }


    //释放资源 暂未实现
    public void dispose() {
    }
}
