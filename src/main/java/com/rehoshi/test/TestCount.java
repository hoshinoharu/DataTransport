package com.rehoshi.test;

import com.google.gson.Gson;
import com.rehoshi.config.TransportConfig;
import com.rehoshi.transport.datasource.impl.DataBaseDataTableSource;
import com.rehoshi.transport.datasource.model.DBConnectionInfo;
import com.rehoshi.utils.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

public class TestCount {

    private static Logger logger;

    public static void main(String[] args) throws Exception {
        //获取配置文件
        TransportConfig config = new TransportConfig(true);
        logger = new Logger(config.getLogPath() + File.separator + "TestTrans.log");
        DBConnectionInfo inSource = null;
        DBConnectionInfo outSource = null;
        try {
            inSource = new Gson().fromJson(new FileReader(config.getSourceInPath()), DBConnectionInfo.class);
            outSource = new Gson().fromJson(new FileReader(config.getSourceOutPath()), DBConnectionInfo.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //配置数据源
        DataBaseDataTableSource sourceIn = DataBaseDataTableSource.createDataSourceWithConnectionInfo(inSource);
        DataBaseDataTableSource sourceOut = DataBaseDataTableSource.createDataSourceWithConnectionInfo(outSource);


        if (testTables(sourceIn, sourceOut)) {
            if (testDataCount(sourceIn, sourceOut)) {
                logger.log("数据正确性验证成功");
            } else {
                logger.log("数据数量验证失败");
            }
        } else {
            logger.log("表结构验证失败");
        }
    }

    private static boolean testDataCount(DataBaseDataTableSource sourceIn, DataBaseDataTableSource sourceOut) throws Exception {
        List<String> inAllTableNames = sourceIn.getAllTableNames();
        boolean flag = true;
        for (String table : inAllTableNames) {
            long outDataCount = sourceIn.getDataRowCount(table);
            long inDataCount = sourceOut.getDataRowCount(table);

            if (outDataCount != inDataCount) {
                flag = false;
                logger.log(table + "表数据数量验证失败");
                break;
            }
        }
        return flag;
    }

    //比较所有表
    private static boolean testTables(DataBaseDataTableSource sourceIn, DataBaseDataTableSource sourceOut) throws Exception {
        List<String> inAllTableNames = sourceIn.getAllTableNames();
        List<String> outAllTableNames = sourceOut.getAllTableNames();

        boolean flag = false;

        if (inAllTableNames.size() == outAllTableNames.size()) {
            for (int i = 0; i < inAllTableNames.size(); i++) {
                if (!inAllTableNames.get(i).equalsIgnoreCase(outAllTableNames.get(i))) {
                    logger.log(inAllTableNames.get(i) + " 不于 " + outAllTableNames.get(i) + " 匹配");
                    return false;
                }
            }
            flag = true;
        }

        return flag;
    }
}
