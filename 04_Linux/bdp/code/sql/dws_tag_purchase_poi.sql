#### "========================================================================================================================="
#### "========================================================================================================================="
#####  "===================新数据加载到结果表，旧数据删除==============="
# 登录数据库
use dws_db_prd;
# 加载数据到结果表
#####insert into dws_db_prd.dws_tag_purchase_poi(city_id,newest_id,tag_value,tag_detail,pure_distance,lng,lat,tag_value2) select city_id,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2  from dwb_db.dwb_tag_purchase_poi_info where dr = 0;
####insert into dws_db_prd.dws_tag_purchase_poi(city_id,newest_id,tag_value,tag_detail,pure_distance,lng,lat,tag_value2) select t2.city_id,newest_id,tag_value,tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2 from (select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info where dr = 0) t1 inner join (select city_id,city_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) t2 on t1.city = t2.city_name group by  city_id,newest_id,poi_index,poi_name,pure_distance,poi_lnglat,poi_type;
insert into dws_db_prd.dws_tag_purchase_poi(city_id,newest_id,tag_value,tag_detail,pure_distance,lng,lat,tag_value2) select t2.city_id,newest_id,tag_value,tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2 from (select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info where dr = 0) t1 inner join (select city_id,city_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) t2 on t1.city = t2.city_name group by  city_id,newest_id,tag_value,tag_detail,pure_distance,poi_lnglat,poi_type;
# 删除历史数据
update dwb_db.dwb_tag_purchase_poi_info set dr = 1;
# 查看是否删除干净
select * from dwb_db.dwb_tag_purchase_poi_info where dr = 0;
#### "========================================================================================================================="
#### "========================================================================================================================="

