package com.rehoshi.transport.datasource.model;

import com.google.gson.annotations.SerializedName;

public class DBConnectionInfo {

    @SerializedName("ServerIP")
    public String serverIP ;

    @SerializedName("Port")
    public int port ;

    @SerializedName("DataBase")
    public String database;

    @SerializedName("UserName")
    public String userName ;

    @SerializedName("Password")
    public String password ;

    @SerializedName("DBType")
    public String dbType ;
}
