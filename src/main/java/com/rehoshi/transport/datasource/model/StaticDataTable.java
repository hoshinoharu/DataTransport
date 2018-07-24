package com.rehoshi.transport.datasource.model;

import java.util.ArrayList;
import java.util.List;

//静态数据表
public class StaticDataTable extends DataTable {

    //所有的数据行
    private List<DataRow> rowList = new ArrayList<>();

    public StaticDataTable() {
    }

    public StaticDataTable(DataTable table) {
        super(table);
    }

    public List<DataRow> getRowList() {
        return rowList;
    }

    public void addRow(DataRow dataRow) {
        rowList.add(dataRow);
    }

    //清除数据 循环利用
    public void cleanRows() {
        this.rowList.clear();
    }

    public void addRows(List<DataRow> rows) {
        this.rowList.addAll(rows);
    }
}
