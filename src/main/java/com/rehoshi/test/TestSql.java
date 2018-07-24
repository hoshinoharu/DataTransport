package com.rehoshi.test;

import com.rehoshi.config.TransportConfig;
import com.rehoshi.transport.datasource.impl.SqlServerDataDataTableSource;

import java.sql.ResultSet;
import java.sql.SQLException;

public class TestSql {
    public static void main(String[] args) {
        TransportConfig config = new TransportConfig(true);
        SqlServerDataDataTableSource dataDataTableSource = new SqlServerDataDataTableSource(config.getSourceInPath());
        try {
            ResultSet resultSet = dataDataTableSource.executeQuery("select row_number() OVER (ORDER BY (SELECT 0)) as rowNum,* from MyCheckResultNew");
            while (resultSet.next()) {
                int i = Integer.parseInt(resultSet.getObject(1).toString());
                System.out.println(i);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
