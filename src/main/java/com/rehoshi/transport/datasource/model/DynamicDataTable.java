package com.rehoshi.transport.datasource.model;

//动态数据表
public abstract class DynamicDataTable extends DataTable {

    public DynamicDataTable() {

    }

    public DynamicDataTable(DataTable table) {
        super(table);
    }

    //是否有下一个
    public abstract boolean hashNextRow() throws Exception;

    //获取下一个数据行
    public abstract DataRow nextRow() throws Exception;

    //需要跳过数据行的长度
    public abstract void skipRows(int count) throws Exception;
}
