insert into dws_db.dws_customer_cre(city_id,newest_id,exist,imei_num,period)
select b.city_id,a.* from 
  (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter 
  union all 
  select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter) a
left join 
  dwb_db.a_dwb_newest_info b on a.newest_id=b.newest_id ;