# openGauss-tools-datachecker-performance

#### Description
Opengauss data migration verification tool, including full data verification and incremental data verification.

#### Software Architecture
Full data verification: JDBC is used to extract the source and target data, and the extraction results are temporarily stored in Kafka. The verification service obtains the extraction results of the specified table from Kafka through the extraction service for verification. Finally, output the verification results to the file in the specified path.



Incremental data verification, through debezium monitoring the data change records of the source database, the extraction service regularly processes the change records of debezium according to a certain frequency, and makes statistics on the change records. Send the statistical results to the data verification service. The data verification service initiates incremental data verification and outputs the verification results to the specified path file.

#### Installation

1.  Download and start Kafka
2.  Obtain the data verification service jar package and the configuration file template (datachecker-check.jar/datachecker-extract.jar, application.yml, application sink.yml, application source.yml)
3.  Copy the jar package and configuration file to the specified server directory, configure the relevant configuration file, and start the corresponding jar service.

#### Instructions

**Start zookeeper**

```
cd {path}/confluent-7.2.0
```

Start the ZooKeeper service

```
bin/zookeeper-server-start etc/kafka/zookeeper.properties
or
bin/zookeeper-server-start -daemon etc/kafka/zookeeper.properties
```

**Start Kafka**

Start the Kafka broker service

```
bin/kafka-server-start  etc/kafka/server.properties

bin/kafka-server-start -daemon etc/kafka/server.properties
```

**Start  kafka connect (incremental check)**

```
# New connect configuration
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

# Start the Kafka connect service 
bin/connect-standalone -daemon etc/kafka/connect-standalone.properties etc/kafka/mysql-conect.properties
```



**Start datachecker performance service**

```
Source side extraction service
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-source.yml

Destination extraction service
java -jar datachecker-extract.jar -Dspring.config.additional-location=.\config\application-sink.yml

or use extract-endpoints shell command to start the source and sink service
sh extract-endpoints.sh start|stop|restart 

check service
java -jar datachecker-check.jar -Dspring.config.additional-location=.\config\application.yml
or use check-endpoint shell command to start the check service
sh check-endpoint.sh start|stop|restart 
```

**remarks: **

```
The incremental verification service is started, and the source side configuration file  config\application-source.yml needs to be modified.
debezium-enable:true
And configure other debezium related configurations. The incremental verification service can be started when the service is started
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

**Limits and Constraints**

```
JDK version requirements JDK11+
The current version only supports the verification of source MySQL and target openGauss data
The current version only supports data verification, not table object verification
MYSQL requires version 5.7+
The current version does not support the verification of geographic location geometry data
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
