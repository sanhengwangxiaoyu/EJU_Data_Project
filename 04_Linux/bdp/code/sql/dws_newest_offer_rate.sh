#!/bin/sh
# fetch user parameters input by period
period=$1
# set parameters as sql variables && add them to the first line of dws_newest_offer_rate.sql
if [ ! -n "$1" ] ; then
  echo "you have not input a word!"
else
  period=$1
  sed -i '1d' dws_newest_offer_rate.sql 
  sed -i "1 i\ set @p=$period;" /bdp/code/sql/dws_newest_offer_rate.sql
  mysql -h 172.28.36.77  -u wanganming  -P 3306 -NDR_AhfzXT3MSxfh   -Ddws_db < /bdp/code/sql/dws_newest_offer_rate.sql
fi
