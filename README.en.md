# openGauss-tools-datachecker-performance

#### Description
Opengauss data migration verification tool, including full data verification and incremental data verification.

#### Software Architecture
Full data verification: JDBC is used to extract the source and target data, and the extraction results are temporarily stored in Kafka. The verification service obtains the extraction results of the specified table from Kafka through the extraction service for verification. Finally, output the verification results to the file in the specified path.



Incremental data verification, through debezium monitoring the data change records of the source database, the extraction service regularly processes the change records of debezium according to a certain frequency, and makes statistics on the change records. Send the statistical results to the data verification service. The data verification service initiates incremental data verification and outputs the verification results to the specified path file.

#### Installation

1.  Obtain the data verification service jar package and the configuration file template (datachecker-check.jar/datachecker-extract.jar, application.yml, application sink.yml, application source.yml)
2. Copy the jar package and configuration file to the specified server directory, configure the relevant configuration file, and start the corresponding jar service.

3. Download and start Kafka

#### Instructions

**Start zookeeper**

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

**Start Kafka**

Start the Kafka broker service

```
bin/kafka-server-start.sh config/server.properties

sh bin/kafka-server-start.sh -daemon /config/server.properties
```

**Start Kafka interface service Kafka Eagle (optional)**

```
cd {path}/kafka-eagle/bin
sh ke.sh start | restart
```

* Welcome, Now you can visit http://ip:port

* Account:admin ,Password:123456(here is the default account and password of Kafka Eagle)

**Start datachecker performance service**

```
Source side extraction service
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-source.yml

Destination extraction service
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-sink.yml

check service
java -jar datachecker-check.jar -Dspring.config.additional-location=.\config\application.yml
```



**Developer local  startup service**

Add virtual machine parameter VM option in startup configuration:

```
Source side extraction service
-Dspring.config.additional-location=.\config\application-source.yml

Destination extraction service
-Dspring.config.additional-location=.\config\application-sink.yml

check service
-Dspring.config.additional-location=.\config\application.yml
```



#### Contribution

1.  Fork the repository
2.  Create Feat_xxx branch
3.  Commit your code
4.  Create Pull Request


#### Gitee Feature

1.  You can use Readme\_XXX.md to support different languages, such as Readme\_en.md, Readme\_zh.md
2.  Gitee blog [blog.gitee.com](https://blog.gitee.com)
3.  Explore open source project [https://gitee.com/explore](https://gitee.com/explore)
4.  The most valuable open source project [GVP](https://gitee.com/gvp)
5.  The manual of Gitee [https://gitee.com/help](https://gitee.com/help)
6.  The most popular members  [https://gitee.com/gitee-stars/](https://gitee.com/gitee-stars/)
