package com.rehoshi.transport.job;

import com.google.gson.annotations.SerializedName;

import java.util.List;

public class JobStatus {
    //迁移单一表
    final static int TRANSPORT_METHOD_SINGLE = 0;
    //迁移多张表
    final static int TRANSPORT_METHOD_LIST = 1;

    //需要迁移的所有表的名称
    @SerializedName("TableNameList")
    List<String> tableNameList;

    //表名
    @SerializedName("TableName")
    String tableName;

    //当前迁移的表的位置
    @SerializedName("TableIndex")
    int tableIndex = 0;

    //批处理大小 默认3000
    @SerializedName("BatchSize")
    int batchSize = 3000;

    //迁移开始索引
    @SerializedName("StartIndex")
    long startIndex = 1;

    //迁移方式
    @SerializedName("TransportMethod")
    int transportMethod = TRANSPORT_METHOD_SINGLE;

    //总数据量
    @SerializedName("TotalDataCount")
    long totalDataCount = -1;
}
