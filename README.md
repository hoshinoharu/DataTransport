# DataTransport
## 数据库迁移 适用场景 从一个数据库迁移数据到另一个数据库中 两边数据库可以是不同的数据库版本，INSERT 数据的那一边必须创建好表的结构 以及设置好数据类型的对应，本项目只能迁移表中数据。

## 开发环境 IDEA2018.1 使用gradle 构建 JDK8 u171

# 项目结构
## 数据源
最上层是DataTableSource 接口 一种存储表结构数据的数据源
### DataBaseDataTableSource 
数据库类型的数据源 是项目中使用的数据源的基类 根据传入的连接信息 连接数据库
### SqlServerDataDataTableSource
sqlserver 数据库的数据源类 有针对sqlserver的查询和插入语句 本项目中用来查询数据 查询每张表的数据时调用的sql为SELECT * FROM
### MySqlDataTableSource
mysql 数据库的数据源类 有针对mysql的插入和查询sql 本项目中用来插入数据

## 数据迁移
最上层是
```java 
DataTransport<In extends DataTableSource, Out extends DataTableSource> 
```
从 In中 读取 数据 到Out 中

项目中使用的是AsyncJob 一种读写分离的传输任务
### AsyncJob
其中包含两个主要类，DataReader 和 DataWriter 

DataReader 开始读取数据时 会开启一个线程 在缓存池被读满之前会一直读取 ，当缓存池满了之后，调用onReadData()回调，之后，线程会wait 等待唤醒，需要调用reader的fetch方法 获取数据，获取数据时会唤醒读取线程继续读取数据。

DataWriter 开始写入数据时 开启一个线程，不停地从DataReader中fetch数据，如果没有获取到数据线程会wait()，在job中的onReadData()回调中，会唤醒writer.

## 数据
从数据源中读取出来数据的包装，基类是DataTable 包含了 表名和列名 是表的基本结构
### 静态数据表 StaticDataTable
继承自DataTable 加了一个数据行（DataRow）的list
### 动态数据表 DynamicDataTable
继承自DataTable 数据行不确定 需要不停的迭代获取 
### 数据库的动态数据表 DBDynamicDataTable
构造时传入一个ResultSet 迭代获取数据行时 根基数据列名从ResultSet中查询获取到每一行

# 配置
## TransportConfig
首先会获取 系统环境变量中的 TRANSPORT_HOME 路径作为配置文件的根目录 所有配置文件使用json配置
## Souce文件
数据库连接配置文件
```java
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
}
```
## In.Source
在/config/In.source下 从该数据源中读取数据 
## Out.Souce
在/config/Out.source下 向该数据源中插入数据 

## JobInfo
在/config/Job.info下 配置任务的基本信息
```java
public class JobInfo {
    @SerializedName("Name")
    public String name;
}
```

## 工作状态目录
在/jobs下 保存每个工作的状态 命名方式使用 job_{JobInfo.Name}.json

## 日志输出
在/logs下 命名方式为 {JobInfo.Name}.log

# 使用
```java
//获取配置文件
        TransportConfig config = new TransportConfig(true);

        //配置数据源
        SqlServerDataDataTableSource sqlServerDS = new SqlServerDataDataTableSource(
                config.getSourceInPath()
        );
        MySqlDataTableSource mySqlDataSource = new MySqlDataTableSource(
                config.getSourceOutPath()
        );

        //加载工作信息
        JobInfo jobInfo = null;
        try {
            jobInfo = new Gson().fromJson(new FileReader(config.getJobInfoPath()), JobInfo.class);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //创建传输任务
        DataTransport<SqlServerDataDataTableSource, MySqlDataTableSource> dataTransport
                = new AsyncJob<>(sqlServerDS, mySqlDataSource, config.getJobsHomePath(), jobInfo.name, config.getLogPath());

        //报错之后尝试重启任务 针对 网络错误 设置可以继续开始迁移
        dataTransport.setOnTransportErrorListener(e -> {
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
            //重新开始
            dataTransport.autoTransport();
        });

        //如果成功加载工作状态
        if (dataTransport.loadJobStatus()) {
            //自动开始
            dataTransport.autoTransport();
        } else {
            //设置 一次插入的大小 DataReader的缓存大小
            dataTransport.setBatchSize(3000);
            //传输全部数据表
            dataTransport.transportAll();
        }
```

## 任务状态保存
插入数据时使用事务，事务提交时保存当前插入的表的名称和表在In.getAllTableNames() 中的索引，保存插入的数据的行数，下次读取时调DynamicDataTable.skipRows(long count)方法来直接从索引位置继续任务。

## 效率 
测试环境： 公司的线上备份sqlserver数据库中的数据量为4000w+ 转到局域网中的一台mysql中 耗时为一天一夜 机器配置：1核 4HG
生产环境：5000w+数据 耗时为3小时  机器配置： 8核 16g
## 注意事项
在任务错误回调时 如果是sql或者数据类型错误的异常，是无法重新继续插入数据的，因为再次开始依然还是会错误，这里的报错自动继续 是在网络错误的情况下调用

请务必确定两边数据库的表结构的一致性
