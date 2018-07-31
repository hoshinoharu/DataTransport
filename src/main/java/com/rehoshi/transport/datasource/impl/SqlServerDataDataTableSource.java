package com.rehoshi.transport.datasource.impl;

import com.rehoshi.transport.datasource.model.DBConnectionInfo;
import com.rehoshi.transport.datasource.model.DynamicDataTable;
import com.rehoshi.transport.datasource.model.StaticDataTable;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

//sqlserver 的数据源
public class SqlServerDataDataTableSource extends DataBaseDataTableSource {

    public SqlServerDataDataTableSource(String dbIp, int port, String database, String userName, String password) {
        super(dbIp, port, database, userName, password);
    }

    public SqlServerDataDataTableSource(DBConnectionInfo info) {
        super(info);
    }

    @Override
    protected String createGetAllTableNameSql() {
        return "SELECT name FROM sys.tables ORDER BY name";
    }

    @Override
    protected String createDbInsertSql(StaticDataTable staticDataTable) {
        StringBuilder sb = new StringBuilder("INSERT INTO [").append(staticDataTable.getTableName()).append("] (");
        String[] columns = staticDataTable.getTableColumns();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("[").append(columns[i]).append("]");
        }
        sb.append(") VALUES (");
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("?");
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    protected Connection createDbConnection(String dbIp, int port, String database, String userName, String password) {
        StringBuilder sb = new StringBuilder("jdbc:sqlserver://");
        sb.append(dbIp).append(":").append(port).append(";DatabaseName=").append(database).append(";");
        String url = sb.toString();
        try {
            return DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    protected String createGetInRangeSql(String tableName, long startIndex, long endIndex) {
        //sqlserver rownum 从1开始
        return "SELECT * FROM(SELECT ROW_NUMBER () OVER (ORDER BY(SELECT 0)) AS SqlServerRowNum, * FROM " + tableName + ") tb WHERE tb.SqlServerRowNum BETWEEN " + startIndex + " AND " + endIndex;
    }

    @Override
    public DynamicDataTable getDynamicTableFromIndex(String tableName, long startIndex) throws Exception {
        DynamicDataTable dataTable = super.getDynamicTableFromIndex(tableName, startIndex);
        //索引从1开始 跳过之前的数据
        dataTable.skipRows((int) (startIndex - 1));
        return dataTable;
    }

    //    @Override
//    protected String createGetRangeFromSql(String tableName, long index) {
//        //sqlserver rownum 从1开始
//        return "SELECT * FROM(SELECT ROW_NUMBER () OVER (ORDER BY(SELECT 0)) AS SqlServerRowNum, * FROM " + tableName + ") tb WHERE tb.SqlServerRowNum >= " + index;
//    }

    @Override
    public StaticDataTable getInRange(String tableName, long startIndex, long endIndex) {
        StaticDataTable staticDataTable = null;
        try {
            //不去序号
            staticDataTable = readFromDb(tableName, createGetInRangeSql(tableName, startIndex, endIndex), s -> !s.equals("SqlServerRowNum"));
            staticDataTable.setTableName(tableName);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return staticDataTable;
    }

    @Override
    protected String createGetTableDataCountSql(String tableName) {
        return "SELECT rows FROM sysindexes ii INNER JOIN sysobjects oo ON ( oo.id = ii.id AND oo.xtype = 'U ') WHERE   ii.indid < 2 AND OBJECT_NAME(ii.id) = '" + tableName + "'";
    }
}
