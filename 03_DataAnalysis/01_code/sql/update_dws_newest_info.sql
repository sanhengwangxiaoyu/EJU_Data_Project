-- 更新 楼盘表 开发时间 
update dws_db_prd.dws_newest_info set recent_opening_time = replace(recent_opening_time,'2099','2019') where substr(recent_opening_time ,1,4)>2021;
update dws_db_prd.dws_newest_info set recent_opening_time = replace(recent_opening_time,'2414','2014') where substr(recent_opening_time ,1,4)>2021;
update dws_db_prd.dws_newest_info set recent_opening_time = replace(recent_opening_time,'2022','2020') where substr(recent_opening_time ,1,4)>2021;
update dws_db_prd.dws_newest_info set recent_opening_time = replace(recent_opening_time,'2023','2020') where substr(recent_opening_time ,1,4)>2021;
update dws_db_prd.dws_newest_info set recent_opening_time = replace(recent_opening_time,'2027','2020') where substr(recent_opening_time ,1,4)>2021;

-- 更新楼盘表经纬度 1
update dws_db_prd.dws_newest_info a ,(select newest_id,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10) = '2021-09-29' and city_name = gd_city and county_name = gd_district and gd_lat is not null and gd_lat != '')b set a.lat = b.gd_lat ,a.lng = b.gd_lng where a.newest_id = b.newest_id;

-- 新楼盘表经纬度 2
update dws_db_prd.dws_newest_info a ,(select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and change_clomn_index is null or change_clomn_index = '')b set a.lat = b.gd_lat ,a.lng = b.gd_lng where a.newest_id = b.newest_id;

-- 新楼盘表区县和经纬度 1
update dws_db_prd.dws_newest_info a ,(select t1.*,t2.city_id,t2.region_id from (select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and  change_clomn_index = '1') t1 left join (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name) t2 on t1.gd_city = t2.city_name and t1.gd_district = t2.region_name)b set a.county_id=b.region_id,a.lat = b.gd_lat ,a.lng = b.gd_lng where a.newest_id = b.newest_id;

-- 新楼盘表城市和区县和经纬度 1
update dws_db_prd.dws_newest_info a ,(select t1.*,t2.city_id,t2.region_id from (select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and change_clomn_index = '3') t1 left join (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name) t2 on t1.gd_city = t2.city_name and t1.gd_district = t2.region_name)b set a.city_id = b.city_id,a.county_id=b.region_id,a.lat = b.gd_lat ,a.lng = b.gd_lng where a.newest_id = b.newest_id;

-- 新楼盘表城市和区县和经纬度 2
update dws_db_prd.dws_newest_info a ,(select t1.*,t2.city_id,t2.region_id from (select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 14:32:43' and change_clomn_index = '3') t1 left join (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name) t2 on t1.gd_city = t2.city_name and t1.gd_district = t2.region_name)b set a.city_id = b.city_id,a.county_id=b.region_id,a.lat = b.gd_lat ,a.lng = b.gd_lng where a.newest_id = b.newest_id;

-- 更新准入表
update dws_db_prd.dws_newest_period_admit set dr = 1 where newest_id in ('e62634b1ee0a2500cbfdd8f058a61984','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7');



