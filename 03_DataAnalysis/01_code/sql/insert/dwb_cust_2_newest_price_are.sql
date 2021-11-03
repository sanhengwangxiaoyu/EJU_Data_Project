insert into dwb_db.dwb_cust_2_newest_price_are 
(newest_id,newest_name,direct_avg_price,browse_avg_price,avg_price_rate,browse_avg_price_sum,browse_avg_price_count,browse_count,period,dr,create_time,update_time,pal_name)
select newest_id,newest_name,direct_avg_price,
       cast(sum(browse_avg_price_sum)/sum(browse_avg_price_count) as decimal(18,0))  browse_avg_price,
       cast(case when direct_avg_price is null then 0 else direct_avg_price end /(sum(browse_avg_price_sum)/sum(browse_avg_price_count)) as decimal(18,2)) avg_price_rate,
       sum(browse_avg_price_sum) browse_avg_price_sum,sum(browse_avg_price_count) browse_avg_price_count,null,null,dr,now(),now(),pal_name 
from dwb_db.dwb_cust_2_newest_price_are 
where newest_id not in (select newest_id from dwb_db.dwb_cust_2_newest_price_are where avg_price_rate = 1 and pal_name = '贝壳' group by newest_id)
      and browse_avg_price_count != 0 and browse_avg_price_sum != 0
group by newest_id,newest_name,direct_avg_price,dr,pal_name 
  having avg_price_rate<0.9 or avg_price_rate>1.1;