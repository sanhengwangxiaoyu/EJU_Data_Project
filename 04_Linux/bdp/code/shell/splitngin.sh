#!/bin/bash
#设置日志文件存放目录
logs_path="/bdp/data/"
#设置备份目录
logs_bak_path="/bdp/data/access_logs_bak/"

#重命名日志文件
mv ${logs_path}access.log ${logs_bak_path}access_$(date "+%Y%m%d%H").log

#重载nginx：重新生成新的log日志文件
/opt/nginx/sbin/nginx -s reload
