#!/bin/bash
echo "" >> /root/logs/executiondata.log 
echo -e "\e[1;31m $(date)######################################################################### \e[0m" >> /root/logs/executiondata.log 

echo -e "\e[1;33m【START BDP ----------------------- ZOOKEEPER】\e[0m" >> /root/logs/executiondata.log 
zkServer.sh start >> /root/logs/executiondata.log 
echo "node001 zokkeeper server startup finish ..........." >> /root/logs/executiondata.log 
ssh root@node002 "/opt/zookeeper-3.4.5/bin/zkServer.sh start" >> /root/logs/executiondata.log 
echo "node002 zokkeeper server startup finish ..........." >> /root/logs/executiondata.log 
ssh root@node003 "/opt/zookeeper-3.4.5/bin/zkServer.sh start" >> /root/logs/executiondata.log 
echo "node003 zokkeeper server startup finish ..........." >> /root/logs/executiondata.log 

sleep 10 
jps >> /root/logs/executiondata.log 
echo "==========NODE01============" >> /root/logs/executiondata.log 
ssh root@node002 "jps" >> /root/logs/executiondata.log 
echo "==========NODE02============" >> /root/logs/executiondata.log 
ssh root@node003 "jps" >> /root/logs/executiondata.log 
echo "==========NODE03============" >> /root/logs/executiondata.log 



echo -e "\e[1;33m【START BDP ----------------------- HDFS】\e[0m" >> /root/logs/executiondata.log 
start-dfs.sh >> /root/logs/executiondata.log 

sleep 10 
jps >> /root/logs/executiondata.log 
echo "==========NODE01============" >> /root/logs/executiondata.log 
ssh root@node002 "jps" >> /root/logs/executiondata.log 
echo "==========NODE02============" >> /root/logs/executiondata.log 
ssh root@node003 "jps" >> /root/logs/executiondata.log 
echo "==========NODE03============" >> /root/logs/executiondata.log 



echo -e "\e[1;33m【START BDP ----------------------- YARN】\e[0m" >> /root/logs/executiondata.log 
start-yarn.sh >> /root/logs/executiondata.log 

sleep 10 
jps >> /root/logs/executiondata.log 
echo "==========NODE01============" >> /root/logs/executiondata.log 
ssh root@node002 "jps" >> /root/logs/executiondata.log 
echo "==========NODE02============" >> /root/logs/executiondata.log 
ssh root@node003 "jps" >> /root/logs/executiondata.log 
echo "==========NODE03============" >> /root/logs/executiondata.log 



echo -e "\e[1;33m【START BDP ----------------------- HISTORYSERVER】\e[0m" >> /root/logs/executiondata.log 
mapred --daemon start historyserver >> /root/logs/executiondata.log 
echo "node001 historyserver startup finish ..........." >> /root/logs/executiondata.log 
##ssh root@node002 "/program/bdp/soft/hadoop/bin/mapred --daemon start historyserver" >> /root/logs/executiondata.log 
##echo "node002 historyserver startup finish ..........." >> /root/logs/executiondata.log 
##ssh root@node003 "/program/bdp/soft/hadoop/bin/mapred --daemon start historyserver" >> /root/logs/executiondata.log 
##echo "node003 historyserver startup finish ..........." >> /root/logs/executiondata.log 

sleep 10 
jps >> /root/logs/executiondata.log 
echo "==========NODE01============" >> /root/logs/executiondata.log 
ssh root@node002 "jps" >> /root/logs/executiondata.log 
echo "==========NODE02============" >> /root/logs/executiondata.log 
ssh root@node003 "jps" >> /root/logs/executiondata.log 
echo "==========NODE03============" >> /root/logs/executiondata.log 



##echo -e "\e[1;33m【START BDP ----------------------- Hive3】\e[0m" >> /root/logs/executiondata.log
##nohup hive --service metastore >> /root/logs/executiondata.log 2>&1 &
##sleep 20
##nohup hiveserver2 >> /root/logs/executiondata.log 2>&1 &

##sleep 10
##jps >> /root/logs/executiondata.log
##echo "==========NODE01============" >> /root/logs/executiondata.log
##netstat -ntulp|grep 9083 >> /root/logs/executiondata.log
##echo "==========Hive 9083============" >> /root/logs/executiondata.log
##sleep 90
##netstat -ntulp|grep 10000  >> /root/logs/executiondata.log
##echo "==========Hive 10000============" >> /root/logs/executiondata.log


echo -e "\e[1;33m【START BDP ----------------------- HBASE】\e[0m" >> /root/logs/executiondata.log 
start-hbase.sh >> /root/logs/executiondata.log 

sleep 10 
jps >> /root/logs/executiondata.log 
echo "==========NODE01============" >> /root/logs/executiondata.log 
ssh root@node002 "jps" >> /root/logs/executiondata.log 
echo "==========NODE02============" >> /root/logs/executiondata.log 
ssh root@node003 "jps" >> /root/logs/executiondata.log 
echo "==========NODE03============" >> /root/logs/executiondata.log 


echo -e "\e[1;31m $(date)######################################################################### \e[0m" >> /root/logs/executiondata.log 
echo "" >> /root/logs/executiondata.log
