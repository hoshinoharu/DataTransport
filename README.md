# DataTransport
## 数据库迁移 适用场景 从一个数据库迁移数据到另一个数据库中 两边数据库可以是不同的数据库版本，INSERT 数据的那一边必须创建好表的结构 以及设置好数据类型的对应，本项目只能迁移表中数据。

## 开发环境 IDEA2018.1 使用gradle 构建

# 项目结构
## 数据源
最上层是DataTableSource 接口 一种存储表结构数据的数据源
### DataBaseDataTableSource 
数据库类型的数据源 是项目中使用的数据源的基类 根据传入的连接信息 连接数据库
### SqlServerDataDataTableSource
sqlserver 数据库的数据源类 有针对sqlserver的查询和插入语句 本项目中用来查询数据
### MySqlDataTableSource
mysql 数据库的数据源类 有针对mysql的插入和查询sql 本项目中用来插入数据

