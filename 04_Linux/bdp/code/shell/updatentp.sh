#!bin/bash
echo -e "\e[1;36m【$(date +%Y%m%d%H%M)----------ntp更新时间】\e[0m" >> /root/logs/executiondata.log
/usr/sbin/ntpdate cn.ntp.org.cn >> /root/logs/executiondata.log
