package com.rehoshi.transport.datasource.model;

import java.sql.ResultSet;
import java.sql.SQLException;

//数据库动态数据表 可以动态的读取数据行
public class DBDynamicDataTable extends DynamicDataTable {

    private ResultSet resultSet;
    private long curRowIndex = 0;

    public DBDynamicDataTable(DataTable ori, ResultSet resultSet) {
        super(ori);
        this.resultSet = resultSet;
    }

    //是否有下一行数据 这里直接返回 数据库查询结果集的next
    @Override
    public boolean hashNextRow() throws Exception {
        return resultSet.next();
    }

    //获取下一行 从结果集中读取
    @Override
    public DataRow nextRow() throws SQLException {
        String[] columns = getTableColumns();
        int columnCount = columns.length;
        DataRow dataRow = new DataRow(columnCount);
        for (int i = 0; i < columnCount; i++) {
            Object val = resultSet.getObject(columns[i]);
            dataRow.setData(i, val);
        }
        curRowIndex++;
        return dataRow;
    }

    //跳过多少行
    @Override
    public void skipRows(long count) throws Exception {
        long skips = 0;
        while (skips < count) {
            resultSet.next();
            skips++;
            //当前行++
            curRowIndex++;
        }
    }

}
