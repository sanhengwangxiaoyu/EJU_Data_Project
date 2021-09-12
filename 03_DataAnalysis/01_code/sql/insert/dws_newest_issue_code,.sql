insert into dws_db.dws_newest_issue_code (city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time)
select b.city_id,b.city_name,b.region_id county_id,b.region_name county_name,b.uuid newest_id,b.newest_name,b.address,b.developer business,a.issue_number issue_code,a.issue_date,
       a.issue_quarter,a.issue_month,a.issue_area,a.room_sum supply_num,a.housing_id,0 dr ,now() create_time ,now() update_time from 
  (select housing_id,issue_number,issue_date,case when issue_date is not null then date_format(issue_date,'%Y%m') else issue_date end issue_month,case when issue_date is not null then concat(substr(issue_date,1,4) ,'Q',QUARTER(issue_date)) else issue_date end  issue_quarter,room_sum,issue_area from dwb_db.dim_housing_issue where is_del = 0) a
left join 
  (select t1.*,t2.region_name from  (select id,uuid,newest_name,city_id,city_name,region_id,address,developer from dwb_db.dim_housing) t1 left join (select city_id ,region_id ,region_name  from dws_db.dim_geography dg where grade = 4 group by city_id ,region_id ,region_name ) t2 on t1.city_id = t2.city_id and t1.region_id=t2.region_id) b
on a.housing_id = b.id ;
