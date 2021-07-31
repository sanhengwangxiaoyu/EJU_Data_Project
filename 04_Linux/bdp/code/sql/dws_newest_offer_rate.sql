 set @p='2021Q1';
-- insert into dws_db.dws_newest_offer_rate (city_id,offer,rate,period,create_time,update_time,dr)
select ds.city_id,value,case when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds.period,now(),now(),'0' from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null and period=@p) dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1 and period=@p) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period
union all 
select ds2.city_id,value,case when value='-' then '-' when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds2.period,now(),now(),'0' from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null and period=@p) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1 and period=@p) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period;

exit
