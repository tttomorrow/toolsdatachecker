# gs_datachecker

#### 介绍
openGauss数据校验工具 ，包含全量数据校验以及增量数据校验。

#### 软件架构
全量数据校验，采用JDBC方式抽取源端和目标端数据，并将抽取结果暂存到kafka中，校验服务通过抽取服务从kafka中获取指定表的抽取结果，进行校验。最后将校验结果输出到指定路径的文件文件中。

增量数据校验，通过debezium监控源端数据库的数据变更记录，抽取服务按照一定的频率定期处理debezium的变更记录，对变更记录进行统计。将统计结果发送给数据校验服务。由数据校验服务发起增量数据校验，并将校验结果输出到指定路径文件。

**安装环境要求：**

	JDK11+
	kafka安装（启动zookeeper，kafka服务）

#### 安装教程

1.  下载并启动kafka
2.  获取数据校验服务jar包，及配置文件模版（datachecker-check.jar/datachecker-extract.jar,application.yml,application-sink.yml,application-source.yml）
3.  将jar包以及配置文件copy到指定服务器目录，并配置相关配置文件，启动相应的jar服务即可。

#### 详细使用说明

**启动Zookeeper**

```
cd {path}/confluent-7.2.0
```

```
bin/zookeeper-server-start  etc/kafka/zookeeper.properties
或者
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties
```

**启动Kafka**

```
bin/kafka-server-start etc/kafka/server.properties
或者
bin/kafka-server-start -daemon etc/kafka/server.properties
```

**启动kafka connect（增量校验）**

```
# 新建connect配置
vi etc/kafka/mysql-conect.properties

name=mysql-connect-all
connector.class=io.debezium.connector.mysql.MySqlConnector
database.hostname=
database.port=3306
database.user=root
database.password=test@123
database.server.id=1
database.server.name=mysql_debezium_connect-all
database.whitelist=test
database.history.kafka.bootstrap.servers=
database.history.kafka.topic=mysql_test_topic-all
include.schema.changes=true
transforms=Reroute
transforms.Reroute.type=io.debezium.transforms.ByLogicalTableRouter
transforms.Reroute.topic.regex=(.*)test(.*)
transforms.Reroute.topic.replacement=data_check_test_all

# 启动connect服务
bin/connect-standalone -daemon etc/kafka/connect-standalone.properties etc/kafka/mysql-conect.properties
```

**校验服务启动配置** 

```
校验服务配置 修改application.yml文件
	server.port 为校验服务web端口，默认可不修改
	logging.config  设置校验服务日志路径为config/log4j2.xml文件绝对路径
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	data.check.data-path 校验结果输出地址，默认配置可不修改
	data.check.source-uri 源端服务请求地址，默认配置可不修改
	data.check.sink-uri 目标端服务请求地址，默认配置可不修改
	data.check.core-pool-size 并发线程数设置，根据当前环境配置，可不修改
```

**源端服务启动配置**

```
源端服务配置 修改application-source.yml文件
	server.port 为源端抽取服务web端口，默认可不修改
	logging.config 设置校验服务日志路径为config/log4j2source.xml文件绝对路径
	spring.check.server-uri 校验服务请求地址，默认配置可不修改
	spring.extract.schema 当前校验数据schema，mysql 数据库名称
	spring.extract.core-pool-size 并发线程数设置，根据当前环境配置，可不修改
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	
	数据源配置
	
```

**目标端服务启动配置**

```
目标端服务配置 修改application-sink.yml文件
	server.port 为目标端抽取服务web端口，默认可不修改
	logging.config 设置校验服务日志路径为config/log4j2sink.xml文件绝对路径
	spring.check.server-uri 校验服务请求地址，默认配置可不修改
	spring.extract.schema 当前校验数据schema，opengauss schema名称
	spring.extract.core-pool-size 并发线程数设置，根据当前环境配置，可不修改
	bootstrap-servers 为kafka工作地址，默认安装可不修改
	
	数据源配置
```



**启动数据校验服务**

```shell
sh extract-endpoints.sh start|restart|stop
sh check-endpoint.sh start|restart|stop
先启动抽取服务，后启动校验服务。
```

**后台启动命令**

```shell
nohup java -jar datachecker-extract-0.0.1.jar --source  >/dev/null 2>&1 &

nohup java -jar datachecker-extract-0.0.1.jar --sink >/dev/null 2>&1 &

nohup java -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 &
```

**校验服务完全启动成功后，会自动发起校验请求。**

**备注：**

```
1、单实例校验使用sh 脚本启动校验服务，如果需要并行开启校验，复制当前工作目录文件，重新配置后，使用java 后台启动命令。
2、抽取服务在启动后，会自动加载数据库的表相关信息，如果数据量较大，则数据加载会比较耗时。
3、校验服务启动后，会检测抽取端的表数据信息是否加载完成，如果在一定时间内，未完成加载，则校验服务会自行退出。这时需要查询源端和宿端的表信息加载进度，通过日志信息查看加载进度。或者直接重新启动校验服务。
4、增量校验服务启动，需要修改源端配置文件\config\application-source.yml 中	debezium-enable:true并配置其他 debezium相关配置，服务启动即可开启增量校验服务
```

**开发人员本地 启动服务**

在启动配置中添加虚拟机参数 VM Option : 

```
源端抽取服务
-Dspring.config.additional-location=.\config\application-source.yml

宿端抽取服务
-Dspring.config.additional-location=.\config\application-sink.yml

校验服务
-Dspring.config.additional-location=.\config\application.yml
```

**限制与约束**

```
JDK版本要求JDK11+
当前版本仅支持对源端MySQL，目标端openGauss数据校验
当前版本仅支持数据校验，不支持表对象校验
MYSQL需要5.7+版本
当前版本不支持地理位置几何图形数据校验
```



#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request


#### 特技

1.  使用 Readme\_XXX.md 来支持不同的语言，例如 Readme\_en.md, Readme\_zh.md
2.  Gitee 官方博客 [blog.gitee.com](https://blog.gitee.com)
3.  你可以 [https://gitee.com/explore](https://gitee.com/explore) 这个地址来了解 Gitee 上的优秀开源项目
4.  [GVP](https://gitee.com/gvp) 全称是 Gitee 最有价值开源项目，是综合评定出的优秀开源项目
5.  Gitee 官方提供的使用手册 [https://gitee.com/help](https://gitee.com/help)
6.  Gitee 封面人物是一档用来展示 Gitee 会员风采的栏目 [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
