-- 1、按照城市清洗Check表

-- 北京
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(replace(issue_date,'0:00:00',''),'%Y/%m/%d') issue_date_clean,str_to_date(case when replace(convert(replace(open_date ,'0:00:00','')using ascii),'?','') = '' then '9999/09/09' else replace(convert(replace(open_date ,'0:00:00','')using ascii),'?','') end,'%Y/%m/%d') open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '北京' and floor_name is not null and floor_name != '';
delete from temp_db.city_newest_deal_data_check where city_name = '北京';

-- 深圳
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(replace(replace(replace(replace(substring_index(case when issue_date = '' then '9999年09月09日' else issue_date end, ',',1) ,'年','-'),'月','-'),'日','') ,'\.','-'),'%Y-%m-%d') issue_date_clean,null open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '深圳' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '深圳';
    
-- 广州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(replace(case when issue_date = '' then '9999-09-09' when issue_date is null then '9999-09-09' else issue_date end,'/','-'),'%Y-%m-%d') issue_date_clean,null open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '广州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '广州';


-- 上海（没数据）

-- 三亚

-- 南京

-- 厦门
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,STR_TO_DATE(substring_index(issue_date , 'T',1),'%Y-%m-%d') issue_date_clean,null open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '厦门' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '厦门';

-- 天津 （没数据）


-- 杭州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(replace(case when issue_date='' then '9999/09/09' else issue_date end,'/','-') ,'%Y-%m-%d') issue_date_clean,null open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '杭州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '杭州';


-- 福州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date='' then '9999/09/09' else issue_date end,'%Y/%m/%d') issue_date_clean,null open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '福州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '福州';


-- 苏州


-- 青岛
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when open_date = ' ' then '9999-09-09' else open_date end,'%Y-%m-%d') issue_date_clean,str_to_date(case when open_date = ' ' then '9999-09-09' else open_date end,'%Y-%m-%d') open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '青岛' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '青岛';

-- 东莞
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') issue_date_clean,str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '东莞' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '东莞';
 
-- 丽水(不上线)

-- 九江
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,
  str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') issue_date_clean,
  str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') open_date,
  issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '九江' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '九江';

select issue_date from temp_db.city_newest_deal_data_check where city_name = '九江' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '九江' group by open_date ;


   