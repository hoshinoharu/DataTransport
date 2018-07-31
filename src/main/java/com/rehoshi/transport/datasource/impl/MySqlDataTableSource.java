package com.rehoshi.transport.datasource.impl;

import com.rehoshi.transport.datasource.model.DBConnectionInfo;
import com.rehoshi.transport.datasource.model.DataRow;
import com.rehoshi.transport.datasource.model.StaticDataTable;
import com.rehoshi.utils.DBUtil;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

//mysql的数据源 针对mysql的连接参数 和 sql语句
public class MySqlDataTableSource extends DataBaseDataTableSource {

    private static String connParams = "useUnicode=true&serverTimezone=Asia/Shanghai&characterEncoding=utf-8&allowMultiQueries=true&useSSL=false&rewriteBatchedStatements=true";

    public MySqlDataTableSource(String dbIp, int port, String database, String userName, String password) {
        super(dbIp, port, database, userName, password);
    }

    public MySqlDataTableSource(DBConnectionInfo info) {
        super(info);
    }

    @Override
    protected String createGetAllTableNameSql() {
        return "SHOW TABLES";
    }

    @Override
    protected String createDbInsertSql(StaticDataTable staticDataTable) {
        StringBuilder sb = new StringBuilder("INSERT INTO `").append(staticDataTable.getTableName()).append("` (");
        String[] columns = staticDataTable.getTableColumns();
        for (int i = 0; i < columns.length; i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("`").append(columns[i]).append("`");
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
        StringBuilder sb = new StringBuilder("jdbc:mysql://");
        sb.append(dbIp).append(":").append(port)
                .append("/").append(database).append("?").append(connParams);
        String url = sb.toString();
        try {
            return DriverManager.getConnection(url, userName, password);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }


    @Override
    protected String createGetRangeFromSql(String tableName, long index) {
        //mysql limit 从0开始
        return "SELECT * FROM " + tableName + " LIMIT " + (index - 1);
    }


    @Override
    protected String createGetInRangeSql(String tableName, long startIndex, long endIndex) {
        //mysql limit 从0开始
        return "SELECT * FROM " + tableName + " LIMIT " + (startIndex - 1) + ", " + (endIndex - startIndex + 1);
    }

    @Override
    protected boolean insertIntoDb(StaticDataTable staticDataTable) throws SQLException {
        PreparedStatement batchInsertStatement = null;
        try {
            //拼接插入sql 使用mysql的批量插入语句
            StringBuilder sb = new StringBuilder("INSERT INTO `").append(staticDataTable.getTableName()).append("` (");
            String[] columns = staticDataTable.getTableColumns();
            for (int i = 0; i < columns.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("`").append(columns[i]).append("`");
            }
            sb.append(") VALUES ");
            for (int j = 0; j < staticDataTable.getRowList().size(); j++) {
                if (j > 0) {
                    sb.append(", ");
                }
                sb.append("(");
                for (int i = 0; i < columns.length; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append("?");
                }
                sb.append(")");
            }
            connection = getConnection();
            batchInsertStatement = connection.prepareStatement(sb.toString());
            List<DataRow> rowList = staticDataTable.getRowList();
            int index = 1;
            for (DataRow dataRow : rowList) {
                //填充数据
                for (int i = 0; i < dataRow.count(); i++) {
                    batchInsertStatement.setObject(index, dataRow.get(i));
                    index++;
                }
            }
            int num = batchInsertStatement.executeUpdate();
            return num == rowList.size();
        } catch (SQLException e) {
            throw e;
        } finally {
            DBUtil.close(batchInsertStatement);
        }
    }
}
