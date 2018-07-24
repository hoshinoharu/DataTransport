package com.rehoshi.transport.datasource.i;

import com.rehoshi.transport.datasource.model.DynamicDataTable;
import com.rehoshi.transport.datasource.model.StaticDataTable;

import java.util.List;

//数据源
public interface DataTableSource {

    //获取所有的表名
    List<String> getAllTableNames() throws Exception;

    //获取所有表
    List<StaticDataTable> getAllTables();

    //获取 数据行数
    long getDataRowCount(String tableName) throws Exception;

    //获取一张表的全部数据
    StaticDataTable getAllFromTable(String tableName) throws Exception;

    //在范围内获取
    StaticDataTable getInRange(String tableName, long startIndex, long endIndex) throws Exception;

    //从索引处 获取全部数据
    DynamicDataTable getDynamicTableFromIndex(String tableName, long startIndex) throws Exception;

    //追加数据
    boolean add(StaticDataTable staticDataTable) throws Exception;

    //在指定位置追加数据
    boolean addAt(int index, StaticDataTable staticDataTable) throws Exception;

    //保存添加的修改
    void save();

    //取消修改
    void cancel();

    //删除所有数据
    void cleanAllData() throws Exception;

    void close();
}
