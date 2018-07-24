package com.rehoshi.transport.datasource.model;

import java.util.Arrays;

public class DataTable {
    //表名
    private String tableName;
    //表的列
    private String[] tableColumns;


    public DataTable(DataTable inRange) {
        this.tableName = inRange.tableName;
        this.tableColumns = inRange.tableColumns;
    }

    public DataTable() {

    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String[] getTableColumns() {
        return tableColumns;
    }

    public void setTableColumns(String[] tableColumns) {
        this.tableColumns = tableColumns;
    }

    @Override
    public String toString() {
        return "DataTable{" +
                "tableName='" + tableName + '\'' +
                ", tableColumns=" + Arrays.toString(tableColumns) +
                '}';
    }
}
