# openGauss-tools-datachecker-performance

#### 介绍
openGauss数据迁移校验工具 ，包含全量数据校验以及增量数据校验。

#### 软件架构
全量数据校验，采用JDBC方式抽取源端和目标端数据，并将抽取结果暂存到kafka中，校验服务通过抽取服务从kafka中获取指定表的抽取结果，进行校验。最后将校验结果输出到指定路径的文件文件中。

增量数据校验，通过debezium监控源端数据库的数据变更记录，抽取服务按照一定的频率定期处理debezium的变更记录，对变更记录进行统计。将统计结果发送给数据校验服务。由数据校验服务发起增量数据校验，并将校验结果输出到指定路径文件。


#### 安装教程

1.  获取数据校验服务jar包，及配置文件模版（datachecker-check.jar/datachecker-extract.jar,application.yml,application-sink.yml,application-source.yml）
2.  将jar包以及配置文件copy到指定服务器目录，并配置相关配置文件，启动相应的jar服务即可。
3.  下载并启动kafka

#### 使用说明

**启动Zookeeper**

```
cd {path}/kafka_2.12-3.1.1/bin
```

Start the ZooKeeper service

Note: Soon, ZooKeeper will no longer be required by Apache Kafka.

```
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Open another terminal session and run:

```
sh bin/zookeeper-server-start.sh -daemon config/zookeeper.properties
```

**启动Kafka**

Start the Kafka broker service

```
bin/kafka-server-start.sh config/server.properties

sh bin/kafka-server-start.sh -daemon /config/server.properties
```

**启动kafka界面服务 kafka-eagle（可选项）**

```
cd {path}/kafka-eagle/bin
sh ke.sh start | restart
```

* Welcome, Now you can visit http://ip:port

* Account:admin ,Password:123456 (这里是kafka-eagle默认账户和密码)

**启动数据校验服务**

```
源端抽取服务
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-source.yml

宿端抽取服务
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-sink.yml

校验服务
java -jar datachecker-check.jar -Dspring.config.additional-location=.\config\application.yml
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
