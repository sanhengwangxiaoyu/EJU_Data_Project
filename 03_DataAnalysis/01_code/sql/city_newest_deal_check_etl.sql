-- 1、按照城市清洗Check表
SHOW processlist;
kill 5941354;
kill 5676672;

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


-- 上海
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when open_date = '' then '9999-09-09' else open_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '上海' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '上海';

-- 三亚
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when open_date = '' then '9999-09-09' else substring_index(open_date,'T',1) end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '三亚' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '三亚';
 

-- 南京
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'/','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '南京' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '南京';

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

-- 中山
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '中山' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '中山';

-- 南宁
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = ' ' then '9999-09-09' else replace(issue_date,'/','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '南宁' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '南宁';

-- 南昌
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(issue_date,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '南昌' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '南昌';

-- 合肥

-- 宁波
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(substring_index(issue_date,'：',-1),'/','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '宁波' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '宁波';

-- 成都
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when replace(convert(open_date using ascii),'?','') = '' then '9999-09-09' else replace(open_date,' 00:00:00','') end  ,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '成都' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '成都';

-- 昆明

-- 武汉(缺少预售证和时间)
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(issue_date,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
  from temp_db.city_newest_deal_data_check where city_name = '武汉' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '武汉';

-- 沈阳
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end  ,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '沈阳' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '沈阳';

-- 济南
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '济南' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '济南';


-- 海口
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' or issue_date = '-' then '9999-09-09' else replace(issue_date,'/','-') end  ,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '海口' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '海口';

-- 石家庄
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(replace(case when open_date = '' or open_date is null then '99990909' else open_date end,'/',''),'%Y%m%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '石家庄' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '石家庄';

-- 西安
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '西安' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '西安';

-- 贵阳
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when length(issue_date) < 8 or issue_date = '' or issue_date is null then '9999-09-09' when length(issue_date) = 8 then concat(substr(issue_date,1,4),'-',substr(issue_date,5,2),'-',substr(issue_date,7,2)) else replace(replace(replace(replace(replace(replace(replace(replace(replace(substring_index(issue_date, ',',1) ,'年','-'),'月','-'),'日','') ,'/','-') ,'.','-'),'0:00:00',''),'--','-'),'-19-','-9-'),'-30-','-3-') end,'%Y-%m-%d')  issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '贵阳' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '贵阳';

-- 郑州

-- 重庆(没时间)
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,issue_date issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '重庆' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '重庆';


-- 长春
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'/','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '长春' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '长春';


-- 长沙
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999/09/09' else issue_date end,'%Y/%m/%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '长沙' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '长沙';



-- 东莞
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') issue_date_clean,str_to_date(case when open_date = '' then '9999/09/09' else open_date end,'%Y/%m/%d') open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '东莞' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '东莞';
 
-- 丽水(不上线)

-- 九江
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999.09.09' else issue_date end,'%Y.%m.%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '九江' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '九江';

-- 佛山
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'/','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '佛山' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '佛山';


-- 保定

-- 南通

-- 咸阳
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '咸阳' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '咸阳';

-- 唐山
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999/09/09' else substring_index(issue_date,' ',1) end,'%Y/%m/%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '唐山' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '唐山';

-- 嘉兴
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,
    str_to_date(issue_date,'%Y-%m-%d') issue_date_clean,
    open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
  from temp_db.city_newest_deal_data_check where city_name = '嘉兴' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '嘉兴';

-- 宝鸡
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,case when length(issue_date)=16 or length(issue_date)=17 then str_to_date(replace(issue_date,right(issue_date,8),''),'%Y-%m-%d') when length(issue_date)=15 then str_to_date(replace(issue_date,right(issue_date,7),''),'%Y-%m-%d') else str_to_date('9999-09-09','%Y-%m-%d') end issue_date_clean, open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '宝鸡' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '宝鸡';



-- 常州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999/09/09' else substring_index(issue_date,' ',1) end,'%Y/%m/%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '常州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '常州';


-- 徐州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '徐州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '徐州';

-- 惠州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else issue_date end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
  from temp_db.city_newest_deal_data_check where city_name = '惠州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '惠州';

-- 扬州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when replace(convert(open_date using ascii),'?','') = '' then '9999-09-09' else replace(convert(open_date using ascii),'?','') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '扬州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '扬州';

-- 无锡
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(replace(replace(replace(substring_index(issue_date , ',',1) ,'年','-'),'月','-'),'日','') ,'\.','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '无锡' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '无锡';

-- 汕头

-- 济宁

-- 淄博
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'\.','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '淄博' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '淄博';

-- 温州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'\.','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '温州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '温州';

-- 湖州
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'\.','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '湖州' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '湖州';

-- 烟台


-- 珠海
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else replace(issue_date,'\.','-') end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '珠海' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '珠海';



-- 绍兴

-- 肇庆
insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
  select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,str_to_date(case when issue_date = '' then '9999-09-09' else substring_index(issue_date,' ',1) end,'%Y-%m-%d') issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '肇庆' and floor_name is not null and floor_name!='';
delete from temp_db.city_newest_deal_data_check where city_name = '肇庆';


-- 赣州



   