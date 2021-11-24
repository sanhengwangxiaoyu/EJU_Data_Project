#!/bin/bash
echo "" >> /root/logs/executiondata.log
echo -e "\e[1;31m $(date)####################################################### \e[0m" >> /root/logs/executiondata.log

##echo -e "\e[1;33m【STOP BDP ----------------------- Hive3】\e[0m" >> /root/logs/executiondata.log
##kill -9 $(lsof -i:10000 |awk '{print $2}' | sed -n '2p') >> /root/logs/executiondata.log
##echo "hiveserver2 server shutdown finish ..........." >> /root/logs/executiondata.log
##kill -9 $(lsof -i:9083 |awk '{print $2}' | sed -n '2p') >> /root/logs/executiondata.log
##echo "metastore server shutdown finish ..........." >> /root/logs/executiondata.log



echo -e "\e[1;33m【STOP BDP ----------------------- HBASE】\e[0m" >> /root/logs/executiondata.log
stop-hbase.sh >> /root/logs/executiondata.log

sleep 10
jps >> /root/logs/executiondata.log
echo "==========NODE01============" >> /root/logs/executiondata.log
ssh root@node002 "jps" >> /root/logs/executiondata.log
echo "==========NODE02============" >> /root/logs/executiondata.log
ssh root@node003 "jps" >> /root/logs/executiondata.log
echo "==========NODE03============" >> /root/logs/executiondata.log



echo -e "\e[1;33m【STOP BDP ----------------------- HADOOP】\e[0m" >> /root/logs/executiondata.log
stop-all.sh >> /root/logs/executiondata.log
mapred --daemon stop historyserver >> /root/logs/executiondata.log
echo "node001 historyserver stop finish ..........." >> /root/logs/executiondata.log
##ssh root@node002 "/program/bdp/soft/hadoop/bin/mapred --daemon stop historyserver" >> /root/logs/executiondata.log
##echo "node002 historyserver stop finish ..........." >> /root/logs/executiondata.log
##ssh root@node003 "/program/bdp/soft/hadoop/bin/mapred --daemon stop historyserver" >> /root/logs/executiondata.log
##echo "node003 historyserver stop finish ..........." >> /root/logs/executiondata.log

sleep 10
jps >> /root/logs/executiondata.log
echo "==========NODE01============" >> /root/logs/executiondata.log
ssh root@node002 "jps" >> /root/logs/executiondata.log
echo "==========NODE02============" >> /root/logs/executiondata.log
ssh root@node003 "jps" >> /root/logs/executiondata.log
echo "==========NODE03============" >> /root/logs/executiondata.log




echo -e "\e[1;33m【STOP BDP ----------------------- ZOOKEEPER】\e[0m" >> /root/logs/executiondata.log
zkServer.sh stop >> /root/logs/executiondata.log
echo "node001 zokkeeper server shutdown finish ..........." >> /root/logs/executiondata.log
ssh root@node002 "/opt/zookeeper-3.4.5/bin/zkServer.sh stop" >> /root/logs/executiondata.log
echo "node002 zokkeeper server shutdown finish ..........." >> /root/logs/executiondata.log
ssh root@node003 "/opt/zookeeper-3.4.5/bin/zkServer.sh stop" >> /root/logs/executiondata.log
echo "node003 zokkeeper server shutdown finish ..........." >> /root/logs/executiondata.log

echo -e "\e[1;31m $(date)####################################################### \e[0m" >> /root/logs/executiondata.log
echo "" >> /root/logs/executiondata.log
