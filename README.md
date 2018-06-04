# Corgi 柯基

MySQL 到 Hive 批量数据集成服务

## corgi-web

Web UI 管理界面

#### 编译

```
cd corgi-web
mvn clean package -DskipTests
```

#### 部署

编译成功之后，将 target/corgi-web-release.tar.gz 拷贝到部署服务器，执行

```
tar -zxf corgi-web.release.tar.gz
```

解压缩

#### 配置

首先，将 Hive 配置文件 hive-site.xml 拷贝到 conf 目录下

然后，通过 MySQL 客户端，执行目录 sql/init.sql 文件，创建服务依赖数据库表

最后，编辑 application.yml 文件，下面是一些关键配置：

- server.address  服务地址，生产环境建议修改为 0.0.0.0
- server.port  服务端口号
- spring.datasource.url  MySQL 数据库连接地址
- spring.datasource.username  MySQL 数据库连接用户名
- spring.datasource.password  MySQL 数据库连接密码

其它配置，建议保持默认值

#### 服务启停

启动服务：

```
sh bin/start-corgi.sh
```

停止服务：

```
sh bin/stop-corgi.sh
```

#### 数据源管理

点击菜单【数据源】，进入数据源管理界面，数据源管理界面维护了需要导入的 MySQL 数据库连接信息，如下图所示：

![datasource](http://oahz6ih7r.bkt.clouddn.com/corgi_datasource.png)

#### 批量任务管理

点击菜单【批量任务】，进入批量任务管理界面，批量任务管理界面维护了 corgi-spark 任务的配置信息，如下图所示：

![batch](http://oahz6ih7r.bkt.clouddn.com/corgi_batch.png)

配置说明：

- 名称  全局唯一，用于在提交 corgi-spark 应用时指定
- Source 数据库  需要从数据源导入数据的 MySQL 数据库
- Source 表  需要从数据源导入数据的 MySQL 表
- 模式  目前支持三种模式：全量数据、增量数据、追加数据
- 时间列  模式选择增量数据和追加数据，需要指定根据表中时间列导入数据
- Sink 数据库  导入目标 Hive 数据库，需要手动创建
- Sink 表  导入目标 Hive 表，会根据导入 MySQL 表 Schema 自动创建

## corgi-spark

基于 Spark 的数据集成应用

#### 配置

修改 corgi-spark/src/main/resources/spark.properties 文件

- api.sesrver  corgi-web 服务 HOST:PORT
- hive.metastore.uris  Hive Thrift 服务地址

#### 编译

```
cd corgi-spark
mvn clean package -DskipTests
```

#### 提交任务

在提交任务时，需要指定批量任务名称参数，例如：

```
spark-submit \
--master yarn \
--package com.google.inject:guice:4.1.0,com.alibaba:fastjson:1.2.3,mysql:mysql-connector-java:5.1.40 \
--class com.dyingbleed.corgi.spark.Application 
--jar spark-web.jar <批量任务名称>
```