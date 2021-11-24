##判断是否有参数传入,有则读取参数.无则读取本地时间
if [ x$1 != x ]
then
  echo "=================================================================== day_batch 分割线 ==========================================================================" 
  echo "=================================================================== day_batch 分割线 =========================================================================="
  echo "=================================================================== day_batch 分割线 ==========================================================================" 
  # echo "start Time : $(date "+%Y%m%d")" >> /bdp/log/temp_check_data_ftp.log
  python2 /opt/datax/bin/datax.py -p "-Dtime=$1" /bdp/conf/datax/city_newest_deal_check.json
  #echo "执行时间： $(date "+%Y-%m-%d") 00:00:00 。" >> /bdp/log/temp_check_data_ftp.log_error.log
  #echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json >> /bdp/log/temp_check_data_ftp.log。" >> /bdp/log/temp_check_data_ftp.log_error.log
  #cat -n /bdp/log/temp_check_data_ftp.log |grep "写失败总数                    :" | awk '{print "第"$1"行>>>>>>>>>>>>>>>>>>>>>>>>>>>"$2$3$4$5$6}' | tail -n 1 >> /bdp/log/temp_check_data_ftp.log_error.log
  echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$1" /bdp/conf/datax/city_newest_deal_check.json"
  echo "=================================================================== ods_table 分割线 =========================================================================="
  echo "=================================================================== ods_table 分割线 ==========================================================================" 
  # echo "start Time : $(date "+%Y%m%d")" >> /bdp/log/temp_check_data_ftp.log
  python2 /opt/datax/bin/datax.py -p "-Dtime=$1" /bdp/conf/datax/city_newest_deal_check_2.json
  #echo "执行时间： $(date "+%Y-%m-%d") 00:00:00 。" >> /bdp/log/temp_check_data_ftp.log_error.log
  #echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json >> /bdp/log/temp_check_data_ftp.log。" >> /bdp/log/temp_check_data_ftp.log_error.log
  #cat -n /bdp/log/temp_check_data_ftp.log |grep "写失败总数                    :" | awk '{print "第"$1"行>>>>>>>>>>>>>>>>>>>>>>>>>>>"$2$3$4$5$6}' | tail -n 1 >> /bdp/log/temp_check_data_ftp.log_error.log
  echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime$1" /bdp/conf/datax/city_newest_deal_check_2.json"
else
    echo "=================================================================== day_batch 分割线 ==========================================================================" 
    echo "=================================================================== day_batch 分割线 =========================================================================="
    echo "=================================================================== day_batch 分割线 ==========================================================================" 
    # echo "start Time : $(date "+%Y%m%d")" >> /bdp/log/temp_check_data_ftp.log
    python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json
    #echo "执行时间： $(date "+%Y-%m-%d") 00:00:00 。" >> /bdp/log/temp_check_data_ftp.log_error.log
    #echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json >> /bdp/log/temp_check_data_ftp.log。" >> /bdp/log/temp_check_data_ftp.log_error.log
    #cat -n /bdp/log/temp_check_data_ftp.log |grep "写失败总数                    :" | awk '{print "第"$1"行>>>>>>>>>>>>>>>>>>>>>>>>>>>"$2$3$4$5$6}' | tail -n 1 >> /bdp/log/temp_check_data_ftp.log_error.log
    echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json"
    echo "=================================================================== ods_table 分割线 =========================================================================="
    echo "=================================================================== ods_table 分割线 ==========================================================================" 
    # echo "start Time : $(date "+%Y%m%d")" >> /bdp/log/temp_check_data_ftp.log
    python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check_2.json
    #echo "执行时间： $(date "+%Y-%m-%d") 00:00:00 。" >> /bdp/log/temp_check_data_ftp.log_error.log
    #echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check.json >> /bdp/log/temp_check_data_ftp.log。" >> /bdp/log/temp_check_data_ftp.log_error.log
    #cat -n /bdp/log/temp_check_data_ftp.log |grep "写失败总数                    :" | awk '{print "第"$1"行>>>>>>>>>>>>>>>>>>>>>>>>>>>"$2$3$4$5$6}' | tail -n 1 >> /bdp/log/temp_check_data_ftp.log_error.log
    echo "执行命令：  python2 /opt/datax/bin/datax.py -p "-Dtime=$(date "+%Y%m%d")" /bdp/conf/datax/city_newest_deal_check_2.json"
fi
