#!/bin/bash

#nohup java  -Dspring.config.additional-location=config/application.yml -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 &
APP_NAME=datachecker-check-0.0.1.jar
CONFIG_PATH=config
#使用说明，用来提示输入参数
usage() {
echo "Usage: sh 脚本名.sh [start|stop|restart|status]"
exit 1
}

#检查程序是否在运行
is_exist(){
pid=`ps -ef|grep $APP_NAME | grep $CONFIG_PATH |grep -v grep|awk '{print $2}' `
#如果不存在返回1，存在返回0
if [ -z "${pid}" ]; then
return 1
else
return 0
fi
}

#启动方法
start(){
is_exist
if [ $? -eq "0" ]; then
echo "${APP_NAME} is already running. pid=${pid} ."
else
sleep 3s
nohup java -Xmx5G -Xms5G -XX:MaxMetaspaceSize=1G -XX:MetaspaceSize=1G -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled  -Dspring.config.additional-location=$CONFIG_PATH/application.yml -jar $APP_NAME  >/dev/null 2>&1 &
echo "${APP_NAME} start success"
fi
}

#停止方法
stop(){
is_exist
if [ $? -eq "0" ]; then
kill -15 $pid
sleep 3s
else
echo "${APP_NAME} is not running"
fi
}

#输出运行状态
status(){
is_exist
if [ $? -eq "0" ]; then
echo "${APP_NAME} is running. Pid is ${pid}"
else
echo "${APP_NAME} is NOT running."
fi
}

#重启
restart(){
stop
start
}

#根据输入参数，选择执行对应方法，不输入则执行使用说明
case "$1" in
"start")
start
;;
"stop")
stop
;;
"status")
status
;;
"restart")
restart
;;
*)
usage
;;
esac






