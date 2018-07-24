package com.rehoshi.transport.datasource.impl;

import com.google.gson.Gson;
import com.rehoshi.transport.datasource.i.DataTableSource;
import com.rehoshi.transport.datasource.model.*;
import com.rehoshi.utils.DBUtil;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.*;
import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;


//数据库类型的数据源
public abstract class DataBaseDataTableSource implements DataTableSource {

    //数据库连接
    protected Connection connection;
    //是否自动提交
    private boolean autoCommit = false;

    //数据库地址
    private String dbIp;

    //数据库端口
    private int port;

    private String database;

    //数据库用户名
    private String userName;

    //数据库密码
    private String password;

    private boolean recycleConnection = true;

    //数据table的缓存
    private Map<String, StaticDataTable> tableCache = new HashMap<>();

    public DataBaseDataTableSource(String configPath) {
        try {
            DBConnectionInfo info = new Gson().fromJson(new FileReader(configPath), DBConnectionInfo.class);
            this.dbIp = info.serverIP;
            this.port = info.port;
            this.database = info.database;
            this.userName = info.userName;
            this.password = info.password;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public DataBaseDataTableSource(String dbIp, int port, String database, String userName, String password) {
        this.dbIp = dbIp;
        this.port = port;
        this.database = database;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public StaticDataTable getAllFromTable(String tableName) {
        try {
            StaticDataTable table = readFromDb(tableName, "SELECT * FROM" + tableName);
            table.setTableName(tableName);
            return table;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public StaticDataTable getInRange(String tableName, long startIndex, long endIndex) throws Exception {
        StaticDataTable staticDataTable = null;
        staticDataTable = readFromDb(tableName, createGetInRangeSql(tableName, startIndex, endIndex));
        staticDataTable.setTableName(tableName);
        return staticDataTable;
    }

    @Override
    public List<StaticDataTable> getAllTables() {
        return null;
    }

    @Override
    public List<String> getAllTableNames() throws Exception {
        connection = getConnection();
        List<String> tableNames = new ArrayList<>();
        String sql = createGetAllTableNameSql();
//        Logger.log(sql);
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        ResultSet resultSet = preparedStatement.executeQuery();
        while (resultSet.next()) {
            tableNames.add(resultSet.getString(1));
        }
        return tableNames;
    }

    @Override
    public long getDataRowCount(String tableName) throws Exception {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            String sql = createGetTableDataCountSql(tableName);
            preparedStatement = connection.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                return resultSet.getLong(1);
            }
            return 0;
        } catch (Exception e) {
            throw e;
        } finally {
            DBUtil.close(resultSet, preparedStatement);
        }
    }

    protected String createGetTableDataCountSql(String tableName) {
        return "SELECT COUNT (*) FROM " + tableName;
    }

    //创建获取所有表名的sql语句
    protected abstract String createGetAllTableNameSql();

    /**
     * 创建分页的sql语句
     *
     * @param tableName  查询的表
     * @param startIndex 开始的索引(从1开始)
     * @param endIndex   结束的索引
     * @return sql
     */
    protected abstract String createGetInRangeSql(String tableName, long startIndex, long endIndex);

    protected StaticDataTable readFromDb(String tableName, String sql) throws SQLException {
        return readFromDb(tableName, sql, s -> true);
    }

    protected StaticDataTable readFromDb(String tableName, String sql, Predicate<String> filter) throws SQLException {
        PreparedStatement preparedStatement = null;
        ResultSet resultSet = null;
        try {
            connection = getConnection();
            preparedStatement = connection.prepareStatement(sql);
//            Logger.log("执行查询sql:" + sql);
            resultSet = preparedStatement.executeQuery();
            //缓存表结构
            StaticDataTable table = tableCache.get(tableName);
            if (table == null) {
                table = createTable(resultSet.getMetaData(), filter);
                tableCache.put(tableName, table);
            }
            table.cleanRows();
            String[] columns = table.getTableColumns();
            int columnCount = columns.length;

            while (resultSet.next()) {
                DataRow dataRow = new DataRow(columnCount);
                for (int i = 0; i < columnCount; i++) {
                    Object val = resultSet.getObject(columns[i]);
                    dataRow.setData(i, val);
                }
                table.addRow(dataRow);
            }
            return table;
        } catch (SQLException e) {
            throw e;
        } finally {
            DBUtil.close(preparedStatement, resultSet);
        }
    }

    protected StaticDataTable createTable(ResultSetMetaData metaData, Predicate<String> filter) throws SQLException {
        StaticDataTable table = new StaticDataTable();
        //获取所有列的数量
        int columnCount = metaData.getColumnCount();
        //创建列的数组
        String[] columns = new String[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columns[i] = metaData.getColumnLabel(i + 1);
        }
        //过滤 不需要的列
        List<String> collect = Arrays.stream(columns).filter(filter).collect(Collectors.toList());

        columnCount = collect.size();

        columns = new String[columnCount];

        for (int i = 0; i < columnCount; i++) {
            columns[i] = collect.get(i);
        }

        //设置表的列
        table.setTableColumns(columns);
        return table;
    }

    @Override
    public boolean add(StaticDataTable staticDataTable) throws Exception {
        return insertIntoDb(staticDataTable);
    }

    //插入数据到数据库中
    protected boolean insertIntoDb(StaticDataTable staticDataTable) throws SQLException {
        //获取数据库连接
        PreparedStatement batchInsertStatement = null;
        try {
            connection = getConnection();
            batchInsertStatement = connection.prepareStatement(createDbInsertSql(staticDataTable));
            List<DataRow> rowList = staticDataTable.getRowList();
            for (DataRow dataRow : rowList) {
                //填充数据
                for (int i = 0; i < dataRow.count(); i++) {
                    batchInsertStatement.setObject(i + 1, dataRow.get(i));
                }
                batchInsertStatement.addBatch();
            }
            int[] ints = batchInsertStatement.executeBatch();
            return Arrays.stream(ints).filter(n -> n == -2 || n == 1).count() == rowList.size();
        } catch (SQLException e) {
            throw e;
        } finally {
            DBUtil.close(batchInsertStatement);
        }
    }

    protected abstract String createDbInsertSql(StaticDataTable staticDataTable);

    //数据库插入
    @Override
    public boolean addAt(int index, StaticDataTable staticDataTable) throws Exception {
        return add(staticDataTable);
    }

    private Connection cacheConnection;

    protected Connection getConnection() {
        if (recycleConnection) {
            if (cacheConnection == null) {
                cacheConnection = createDbConnection(dbIp, port, database, userName, password);
            } else {
                boolean closed = true;
                try {
                    closed = cacheConnection.isClosed();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                if (closed) {
                    cacheConnection = createDbConnection(dbIp, port, database, userName, password);
                }
            }
        } else {
            cacheConnection = createDbConnection(dbIp, port, database, userName, password);
        }
        try {
            cacheConnection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return cacheConnection;
    }

    //创建数据库连接
    protected abstract Connection createDbConnection(String dbIp, int port, String database, String userName, String password);

    @Override
    public void save() {
        commit();
    }

    @Override
    public void cancel() {
        rollback();
    }

    @Override
    public void cleanAllData() throws Exception {
        //加一层保险
        if (this instanceof MySqlDataTableSource) {
            List<String> allTableNames = getAllTableNames();
            connection = getConnection();
            for (String name : allTableNames) {
                //清除表中所有数据
                int i = connection.prepareStatement("DELETE FROM " + name).executeUpdate();
//                log("已清除" + name + "表中" + i + "条数据");
            }
            commit();
        }
    }

    //提交事务
    private void commit() {
        if (!autoCommit) {
            try {
                if (connection != null) {
                    connection.commit();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void rollback() {
        if (!autoCommit) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //循环利用数据库连接
    public void recycleConnection(boolean recycleConnection) {
        this.recycleConnection = true;
    }

    @Override
    public void close() {
        System.out.println("关闭数据库连接");
        DBUtil.close(connection, cacheConnection);
        connection = null;
        cacheConnection = null;
    }

    //获取从索引开始位置的数据


    @Override
    public DynamicDataTable getDynamicTableFromIndex(String tableName, long startIndex) throws Exception {
        return getFromIndexWithIgnoreColumns(tableName, startIndex);
    }

    protected DynamicDataTable getFromIndexWithIgnoreColumns(String tableName, long startIndex, String... ignoreColumns) throws Exception {
        ResultSet resultSet = executeQuery(createGetRangeFromSql(tableName, startIndex));
        //获取表结构
        DataTable table = createTable(resultSet.getMetaData(), s -> {
            if (ignoreColumns != null) {
                for (String column : ignoreColumns) {
                    if (column.equalsIgnoreCase(s)) {
                        return false;
                    }
                }
            }
            return true;
        });
        table.setTableName(tableName);
        DynamicDataTable dynamicDataTable = new DBDynamicDataTable(table, resultSet);
        return dynamicDataTable;
    }

    public ResultSet executeQuery(String sql) throws SQLException {
        connection = getConnection();
        return connection.prepareStatement(sql).executeQuery();
    }

    protected String createGetRangeFromSql(String tableName, long index) {
        return "SELECT * FROM " + tableName;
    }

}
