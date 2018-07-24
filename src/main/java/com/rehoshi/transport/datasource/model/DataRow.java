package com.rehoshi.transport.datasource.model;

//数据行  行的数据量和 列的数量相同 并一一对应
public class DataRow {
    //列数量
    private Object[] datas;

    public DataRow(int columnCount) {
        this.datas = new Object[columnCount];
    }

    public void setData(int index, Object val) {
        this.datas[index] = val;
    }

    public int count() {
        return datas.length;
    }

    public Object get(int index) {
        return datas[index];
    }
}
