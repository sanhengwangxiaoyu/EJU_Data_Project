-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1102

select length('项目仅有');

select * from dws_db_prd.dws_newest_provide_sche where period = '2018Q2' and length(provide_sche)<15;

delete from dwb_db.dwb_newest_provide_sche where period = '2018Q2';

delete from dws_db_prd.dws_newest_provide_sche where period = '2018Q2';

select * from dwb_db.dwb_newest_provide_sche where period = '2018Q1' and provide_sche like '%欢迎%';

delete from dws_db_prd.dws_newest_provide_sche where provide_sche='com.';

delete from dws_db_prd.dws_newest_provide_sche where provide_sche='com.' or provide_title like '%全部*';

SELECT provide_sche FROM dws_db_prd.dws_newest_provide_sche 
  where (LENGTH(provide_sche) - LENGTH(REPLACE(REPLACE( provide_sche,'(','' ),'（',''))) != (LENGTH(provide_sche) - LENGTH(REPLACE(REPLACE(provide_sche,')','' ),'）',''))) and period = '2018Q1';

SELECT provide_sche FROM dws_db_prd.dws_newest_provide_sche 
  where (LENGTH(provide_sche) - LENGTH(REPLACE(provide_sche,'[','' ))) != (LENGTH(provide_sche) - LENGTH(REPLACE(provide_sche,']','' ))) and period = '2018Q1';

 
 SELECT provide_title FROM dws_db_prd.dws_newest_provide_sche 
  where (LENGTH(provide_title) - LENGTH(REPLACE(provide_title,'[','' ))) != (LENGTH(provide_title) - LENGTH(REPLACE(provide_title,']','' )));
 

 SELECT provide_title FROM dws_db_prd.dws_newest_provide_sche 
  where (LENGTH(provide_title) - LENGTH(REPLACE(REPLACE( provide_title,'(','' ),'（',''))) != (LENGTH(provide_title) - LENGTH(REPLACE(REPLACE(provide_title,')','' ),'）',''))) and period = '2021Q3';

 
 
 

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1101
select distinct newest_id ,max(issue_code)    
  from dws_db_prd.dws_newest_issue_code where substr(create_time,1,10) != '2021-10-29'
group by newest_id ,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(convert(issue_code using ascii),'?','') ,'(' ,''),')' ,''),'-','') ,'*',''),'.',''),'/',''),':',''),'>',''),'<','') ; 

select distinct newest_id ,max(issue_code)    
  from dws_db_prd.dws_newest_issue_code
group by newest_id ,replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(convert(issue_code using ascii),'?','') ,'(' ,''),')' ,''),'-','') ,'*',''),'.',''),'/',''),':',''),'>',''),'<','') ; 

select newest_id,newest_name,address,concat(lng,',',lat) lnglat from dws_db_prd.dws_newest_info where dr = 0 and  newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period='2021Q3' and dr=0 and newest_id not in (select newest_id from dws_db_prd.dws_tag_purchase_poi) group by newest_id);


update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'2年','2021年') where period in ('2021Q1','2021Q2','2021Q3');
update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'2年','2020年') where substr(period,1,4)='2020';
update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'2年','2019年') where substr(period,1,4)='2019';
update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'2年','2018年') where substr(period,1,4)='2018';
update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'。','.');


delete from dwb_db.dwb_newest_provide_sche where period = '2018Q1'


select *,substr(provide_sche,1,30) from dwb_db.dwb_newest_provide_sche where period = '2018Q1';


select t2.city_id,newest_id,tag_value,tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2 from (select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info where dr = 0) t1 inner join (select city_id,city_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) t2 on t1.city = t2.city_name group by  city_id,newest_id,tag_value,tag_detail,pure_distance,poi_lnglat,poi_type;

select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info where dr = 0;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1029

select * from dws_db_prd.josn_20211029 where `key` in ('售卖资格','发证时间','date','tag','title','content') and newest_id = '04d2a97f3d4b476551cd49ee2119d484';

delete from dws_db_prd.dws_newest_provide_sche where period = '2021Q4';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1029

show create table dwb_db.dwb_customer_lookest_list_m ;
CREATE TABLE `dwb_customer_lookest_list_m` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `ods_id` varchar(33) DEFAULT NULL COMMENT '源表id',
  `ods_table_name` varchar(200) DEFAULT NULL COMMENT '源表名称',
  `imei` varchar(200) DEFAULT NULL COMMENT 'imei',
  `newest_id` varchar(32) DEFAULT NULL COMMENT 'newest_id',
  `current_week` varchar(30) NOT NULL COMMENT '浏览月份',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识： 0 有效  1无效',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_newest_imei` (`imei`) USING BTREE,
  KEY `idx_newest_visit_month` (`current_week`) USING BTREE,
  KEY `idx_newest_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='客户浏览楼盘明细表_周度';


update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'于2年','于2021年') where period = '2021Q3'

update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'。0','.0') where period = '2021Q3'


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1029
delete from dwb_db.dwb_issue_supply_county where quarter = '2021Q3';

select 
    city_name, 
    county_name, 
    county_id city_id, 
    period, 
    supply_value value, 
    null local_issue_value , 
    null local_room_sum_value, 
    null cric_value, 
    null value_from_index , 
    null county_name_merge, 
    '2' city_county_index, 
    '1' period_index, 
    now() update_time, 
    0 dr, 
    now() create_time, 
    intention follow_people_num, 
    city_id cityid,
    '"+date_quarter+"' quarter 
  from dwb_db.dwb_issue_supply_county where period = '"+date_quarter+"' and dr = 0 
union all 
select city_name, city_name county_name, city_id city_id, period, sum(supply_value) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter 
  from dwb_db.dwb_issue_supply_county  where period = '"+date_quarter+"' and dr = 0  group by city_name,city_id,period 
union all 
select city_name, city_name county_name, city_id city_id, period, sum(supply_num) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter 
  from dwb_db.dwb_issue_supply_city where dr = 0 and period = '"+date_quarter+"' and city_id in ('442000','441900') group by city_name,city_id,period

  
select city_name, county_name county_name, county_id city_id, period,supply_value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '2' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter 
  from dwb_db.dwb_issue_supply_county where quarter = '2021Q3' and period != quarter and dr = 0; 
 
update dws_db_prd.dws_supply a , dwb_db.dwb_newest_county_customer_num b set a.follow_people_num = b.intention 
  where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.`month` 
    and b.`month` is not null and b.dr = 0 and b.city_id != b.county_id
    and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0;
   
select a.follow_people_num , b.intention from dws_db_prd.dws_supply a , (select intention,period,county_id,city_id from dwb_db.dwb_newest_county_customer_num where period is not null and dr = 0 and city_id != county_id and period != quarter and quarter = '2021Q3') b  
  where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.period 
    and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0 and a.quarter = '2021Q3' ;

select intention,period,county_id,city_id from dwb_db.dwb_newest_county_customer_num where period is not null and dr = 0 and city_id != county_id and period != quarter and quarter = '2021Q3';
   
select follow_people_num from dws_db_prd.dws_supply where city_county_index = 2 and period_index = 2 and dr = 0 and quarter = '2021Q3' ;


update dws_db_prd.dws_supply a , (select intention,period,county_id,city_id from dwb_db.dwb_newest_county_customer_num where period is not null and dr = 0 and city_id != county_id and period != quarter and quarter = '2021Q3') b set a.follow_people_num = b.intention where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.period and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0 and a.quarter = '2021Q3' ;


delete from dws_db_prd.dws_supply where quarter = '2021Q3';


select b.city_id,a.* from (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period='2021Q3' union all select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period='2021Q3' ) a inner join (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period = '2021Q3' group by period,newest_id) and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,county_id) b on a.newest_id=b.newest_id where b.city_id is not null;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1028

select * from dwb_db.dwb_customer_lookest_list_m where visit_month <= '202109' and visit_month >= '202107';

truncate table dwb_db.dwb_customer_lookest_list; 
truncate table dwb_db.dwb_customer_lookest_list_m; 

update dwb_db.dwb_newest_issue_offer a,(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) b 
  set a.region_id = b.county_id where a.newest_id = b.newest_id;
update dwb_db.dwb_newest_issue_offer a,(select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b 
  set a.city_name = b.city_name where a.city_id = b.city_id;

delete from dwb_db.dwb_newest_issue_offer where city = '三亚市' and newest_id = '待定';

select substr(issue_date_clean,1,7) from odsdb.city_newest_deal where city_name = '三亚' and insert_time = '20210702' group by substr(issue_date_clean,1,7) ;

delete from dwb_db.dwb_newest_county_customer_num where create_time = '2021-10-28 11:22:31.0' and dr = 0;
 
delete from dwb_db.dwb_issue_supply_county where city_name = '三亚市';

delete from dwb_db.dwb_issue_supply_county where quarter = '2021Q3';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1027

select t2.city_id,newest_id,tag_value,tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2 from (select city,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,poi_lnglat,poi_type from dwb_db.dwb_tag_purchase_poi_info) t1 inner join (select city_id,city_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) t2 on t1.city = t2.city_name;

CREATE TABLE dwb_db.dwb_customer_buyable (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `ods_id` varchar(33) DEFAULT NULL COMMENT '源表id',
  `ods_table_name` varchar(30) DEFAULT NULL COMMENT '源表名称',
  `imei` varchar(30) DEFAULT NULL COMMENT 'imei',
  `visit_quarter` varchar(30) NOT NULL COMMENT '浏览季度',
  `buyable` int(2) NOT NULL DEFAULT '0' COMMENT '买房类型：0投资|1自住',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识： 0 有效  1无效',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`visit_quarter`),
  KEY `idx_newest_imei_visit_quarter` (`imei`,`visit_quarter`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='新楼盘交易数据表' PARTITION BY LINEAR KEY (visit_quarter) PARTITIONS 8;

insert into dwb_db.dwb_customer_buyable(ods_id,ods_table_name,imei,visit_quarter,buyable,dr,create_time,update_time)
select max(id) ods_id,'dwb_db.b_dwb_customer_imei_tag' ods_table_name, imei,'2021Q3' visit_quarter,0 buyable,0 dr,now() create_time,now() update_time from dwb_db.b_dwb_customer_imei_tag where is_college_stu = '否' and marriage = '已婚' and education = '高' and have_child = '有' group by imei;


CREATE TABLE dwb_db.dwb_customer_lookest_list (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `ods_id` varchar(33) DEFAULT NULL COMMENT '源表id',
  `ods_table_name` varchar(30) DEFAULT NULL COMMENT '源表名称',
  `imei` varchar(30) DEFAULT NULL COMMENT 'imei',
  `newest_id` varchar(32) DEFAULT NULL COMMENT 'newest_id',
  `visit_quarter` varchar(30) NOT NULL COMMENT '浏览季度',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识： 0 有效  1无效',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`visit_quarter`),
  KEY `idx_newest_imei` (`imei`) USING BTREE,
  KEY `idx_newest_visit_quarter` (`visit_quarter`) USING BTREE,
  KEY `idx_newest_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='客户浏览楼盘明细表' PARTITION BY LINEAR KEY (visit_quarter) PARTITIONS 8;


truncate table  dwb_db.dwb_customer_lookest_list;

CREATE TABLE dwb_db.dwb_customer_lookest_list_m (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `ods_id` varchar(33) DEFAULT NULL COMMENT '源表id',
  `ods_table_name` varchar(30) DEFAULT NULL COMMENT '源表名称',
  `imei` varchar(30) DEFAULT NULL COMMENT 'imei',
  `newest_id` varchar(32) DEFAULT NULL COMMENT 'newest_id',
  `visit_month` varchar(30) NOT NULL COMMENT '浏览月份',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识： 0 有效  1无效',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`,`visit_month`),
  KEY `idx_newest_imei` (`imei`) USING BTREE,
  KEY `idx_newest_visit_month` (`visit_month`) USING BTREE,
  KEY `idx_newest_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='客户浏览楼盘明细表_月份' PARTITION BY LINEAR KEY (visit_month) PARTITIONS 12;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1026
select * from dwb_db.dwb_customer_add_new_code where visit_quarter = '2021Q3';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1025

## etl清洗
select * from temp_db.city_newest_deal_data_check where city_name = '嘉兴' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '嘉兴' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '嘉兴' group by open_date ;

select issue_date from odsdb.city_newest_deal where city_name = '嘉兴' group by issue_date ;

delete from temp_db.city_newest_deal_data_check ;

delete from odsdb.ori_newest_poi_info where dr = 0;

select city_id,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2  from dwb_db.dwb_tag_purchase_poi_info where dr = 0;


## 预售证清洗
select * from dwb_db.dwb_newest_issue_offer;

select * from dwb_db.dwb_issue_supply_city where period = '2021Q3'; 
select * from dwb_db.dwb_issue_supply_city where dr = 0;

update dwb_db.dwb_newest_issue_offer set dr = 1 where city = '扬州市';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1022

delete from temp_db.city_newest_deal_data_check where city_name = '嘉兴';

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1022
## 青岛
select t.newest_id,t.newest_name,t.city_id city1,t.county_id county1,t.region_name region1,t.issue_code,tt.county_id,tt.county_name,tt.issue_s from 
	(select a.newest_id,a.newest_name,a.city_id,a.county_id,a.region_name,b.county_name,a.address,max(a.issue_code) issue_code,0,now() create_date ,now() update_date from 
	  (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address,t3.region_name from 
	    (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '370200' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
	  inner join
	    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
	  on t1.newest_id = t2.newest_id
	  inner join 
	    (select city_id,region_id,city_name,region_name from dws_db_prd.dim_geography where grade='4' group by city_id,region_id,city_name,region_name) t3
	  on t1.city_id = t3.city_id and t1.county_id=t3.region_id) a
	left join 
		(select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name,t3.county_id,t3.county_name from 
		  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '370200'  group by newest_id ,city_id ,county_id ) t1
		left join 
		  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
		on t1.newest_id = t2.newest_id
		left join 
		  (select * from dwb_db.dim_issue where city_id = '370200' and dr = 0) t3
		on t1.city_id = t3.city_id and t1.county_id = t3.county_id
		where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id,t3.county_id,t3.county_name) b  
	on a.newest_id = b.newest_id where b.newest_id is null  and a.issue_code not like '%青%'
	group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address,b.county_name,a.region_name) t
left join 
    (select city_id,city_name,county_id,county_name,issue_s from dwb_db.dim_issue where dr = 0 and city_id='370200' and county_id is not null) tt
on t.city_id = tt.city_id where INSTR(t.issue_code, tt.issue_s);

select "--------------------------------";

select a.newest_id,a.newest_name,a.city_id,a.address,max(a.issue_code) issue_code,0,now(),now() from 
  (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address from 
    (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '370200' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
  inner join
    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
  on t1.newest_id = t2.newest_id) a
left join 
  (select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name from 
    (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '370200'  group by newest_id   ,city_id ,county_id ) t1
  left join 
    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
  on t1.newest_id = t2.newest_id
  left join 
    (select * from dwb_db.dim_issue where dr = 0) t3
  on t1.city_id = t3.city_id
  where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id) b 
on a.newest_id = b.newest_id where b.newest_id is null group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address;

## 上海
select t.newest_id,t.newest_name,t.city_id city1,t.county_id county1,t.region_name region1,t.issue_code,tt.county_id,tt.county_name,tt.issue_s from 
	(select a.newest_id,a.newest_name,a.city_id,a.county_id,a.region_name,b.county_name,a.address,max(a.issue_code) issue_code,0,now() create_date ,now() update_date from 
	  (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address,t3.region_name from 
	    (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '310000' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
	  inner join
	    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
	  on t1.newest_id = t2.newest_id
	  inner join 
	    (select city_id,region_id,city_name,region_name from dws_db_prd.dim_geography where grade='4' group by city_id,region_id,city_name,region_name) t3
	  on t1.city_id = t3.city_id and t1.county_id=t3.region_id) a
	left join 
		(select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name,t3.county_id,t3.county_name from 
		  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id ,city_id ,county_id ) t1
		left join 
		  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
		on t1.newest_id = t2.newest_id
		left join 
		  (select * from dwb_db.dim_issue where city_id = '310000' and dr = 0) t3
		on t1.city_id = t3.city_id and t1.county_id = t3.county_id
		where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id,t3.county_id,t3.county_name) b  
	on a.newest_id = b.newest_id where b.newest_id is null  and a.issue_code not like '%市2%' and a.issue_code not like '%房地市字%' and a.issue_code not like '%商市%'
	group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address,b.county_name,a.region_name) t
left join 
    (select city_id,city_name,county_id,county_name,issue_s from dwb_db.dim_issue where dr = 0 and city_id='310000' and county_id is not null) tt
on t.city_id = tt.city_id where INSTR(t.issue_code, tt.issue_s);


select substr(issue_code,1,9) issue_code from odsdb.city_newest_deal where city_name = '上海' group by substr(issue_code,1,9) having issue_code like '%浦%';

select substr(issue_code,1,9) issue_code from odsdb.city_newest_deal where city_name = '青岛' group by substr(issue_code,1,9);

insert into dwb_db.dim_issue(city_id,city_name,county_id,county_name,issue_s,dr,create_time,update_time,order_index) 
  select city_id ,city_name ,region_id county_,region_name ,'NULL',0,now(),now(),1  
  from dws_db_prd.dim_geography 
  where grade = 4 and country_id is not null  and city_id = '370200'
  group by city_id ,city_name ,region_id ,region_name ;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1021
## 填充新增楼盘数据


insert into dws_db_prd.dws_newest_popularity_rownumber_quarter (city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,imei_c_avg,index_rate_change,create_time,update_time,dr)
 values('320600','7cfc36a6472f13e761a5c1442736fae7','693','49508','0.013997738','871','1','4.08','2','2018Q1','0.754077','4.4106',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','693','12093','0.057305879','871','1','4.08','2','2018Q1','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','799','44892','0.017798271','1297','1','3.27','4','2018Q2','0.655953','3.9851',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','799','12548','0.063675486','1297','1','3.27','3','2018Q2','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','546','34781','0.015698226','1192','1','2.56','2','2018Q3','0.633983','3.038',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','546','9517','0.05737102','1192','1','2.56','2','2018Q3','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','306','31960','0.009574468','985','1','1.89','14','2018Q4','0.644051','1.9346',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','306','8466','0.036144578','985','1','1.89','4','2018Q4','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','135','20024','0.00674191','1031','1','1.09','25','2019Q1','0.597758','0.8235',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','135','6135','0.02200489','1031','1','1.09','9','2019Q1','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','487','47646','0.010221215','2380','1','1.42','13','2019Q2','0.58189','1.4403',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','487','14357','0.033920736','2380','1','1.42','5','2019Q2','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','710','42165','0.01683861','3032','1','1.55','6','2019Q3','0.565708','1.7399',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','710','13196','0.053804183','3032','1','1.55','2','2019Q3','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','300','17179','0.017463182','1545','1','1.37','3','2019Q4','0.551474','1.4843',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','300','5346','0.056116723','1545','1','1.37','2','2019Q4','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','510','213235','0.002391727','4145','1','1.05','139','2020Q2','0.848069','0.2381',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','510','56470','0.009031344','4145','1','1.05','40','2020Q2','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','1125','305080','0.003687557','6266','1','1.31','76','2020Q3','0.808884','0.6195',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','1125','77999','0.014423262','6266','2','1.31','22','2020Q3','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','670','367698','0.001822148','48595','1','0.56','168','2020Q4','0.539047','0.0389',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','670','79579','0.008419307','3234','1','0.56','49','2020Q4','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','324','376121','0.000861425','5763','1','0.75','267','2021Q1','0.800533','-0.0631',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','324','99197','0.003266228','5763','1','0.75','68','2021Q1','-999','-999',now(),now(),'0'),('320600','7cfc36a6472f13e761a5c1442736fae7','334','347379','0.000961486','7269','1','0.71','263','2021Q2','0.739787','-0.0403',now(),now(),'0'),('320681','7cfc36a6472f13e761a5c1442736fae7','334','89300','0.003740202','7269','1','0.71','64','2021Q2','-999','-999',now(),now(),'0');

insert into dws_db_prd.dws_newest_investment_pop_rownumber_quarter(city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,create_time,update_time,dr)
 values('320600','7cfc36a6472f13e761a5c1442736fae7','2','250.0','0.008','17.0','1.0','0.78','7.0','2019Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','2','77.0','0.025974025974025976','17.0','1.0','0.78','6.0','2019Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','1','581.0','0.0017211703958691911','22.0','1.0','0.50','14.0','2019Q1','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','1','195.0','0.005128205128205128','22.0','1.0','0.50','11.0','2019Q1','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','5','848.0','0.00589622641509434','16.0','1.0','1.70','12.0','2018Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','5','243.0','0.0205761316872428','16.0','1.0','1.70','9.0','2018Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','16','1118.0','0.014311270125223614','24.0','1.0','3.43','4.0','2018Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','16','314.0','0.050955414012738856','21.0','1.0','3.43','2.0','2018Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','17','2263.0','0.007512152010605391','50.0','1.0','1.97','13.0','2018Q1','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','17','540.0','0.03148148148148148','35.0','1.0','1.97','4.0','2018Q1','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','12','1233.0','0.009732360097323601','54.0','1.0','1.43','14.0','2019Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','12','350.0','0.03428571428571429','35.0','1.0','1.43','6.0','2019Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','6','1574.0','0.0038119440914866584','36.0','1.0','1.14','20.0','2019Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','6','466.0','0.012875536480686695','36.0','1.0','1.14','16.0','2019Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','17','1476.0','0.011517615176151762','38.0','1.0','2.45','13.0','2018Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','17','411.0','0.0413625304136253','38.0','1.0','2.45','7.0','2018Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','16','7459.0','0.002145059659471779','137.0','1.0','1.00','49.0','2020Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','16','2154.0','0.007428040854224698','95.0','1.0','1.00','29.0','2020Q2','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','10','10450.0','0.0009569377990431','170.0','1.0','0.74','74.0','2020Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','10','2725.0','0.003669724770642202','124.0','1.0','0.74','43.0','2020Q3','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','54','25490.0','0.0021184778344448805','3567.0','1.0','0.57','79.0','2020Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','54','5728.0','0.009427374301675978','274.0','1.0','0.57','31.0','2020Q4','2021-10-08 00:00:00.0','2021-10-08 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','20','29438.0','0.0006793939805693','708.0','1.0','0.62','132.0','2021Q2','2021-10-09 00:00:00.0','2021-10-09 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','20','7028.0','0.0028457598178713715','708.0','1.0','0.62','53.0','2021Q2','2021-10-09 00:00:00.0','2021-10-09 00:00:00.0','0'),('320600','7cfc36a6472f13e761a5c1442736fae7','26','31260.0','0.0008317338451695','423.0','1.0','0.77','143.0','2021Q1','2021-10-09 00:00:00.0','2021-10-09 00:00:00.0','0'),('320681','7cfc36a6472f13e761a5c1442736fae7','26','7164.0','0.0036292573981016193','301.0','1.0','0.77','51.0','2021Q1','2021-10-09 00:00:00.0','2021-10-09 00:00:00.0','0');

insert into dws_db_prd.dws_newest_popularity_rownumber_quarter (city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,imei_c_avg,index_rate_change,create_time,update_time,dr)
 values('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','181','30276.0','0.005978332672744088','992.0','1.0','1.32','42.0','2021Q1','0.830634','0.5891','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','181','2547.0','0.07106399685904986','412.0','1.0','1.32','3.0','2021Q1','-999.0','-999.0000','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','49','19210.0','0.0025507548152004164','696.0','1.0','0.81','78.0','2020Q2','0.938393','-0.1368','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','49','2241.0','0.02186523873270861','436.0','1.0','0.81','10.0','2020Q2','-999.0','-999.0000','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','364','22733.0','0.016011964984823823','1378.0','1.0','1.69','8.0','2020Q3','0.782385','1.1601','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','364','3796.0','0.0958904109589041','1378.0','1.0','1.69','3.0','2020Q3','-999.0','-999.0000','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','21523.0','0.05468568508107606','1177.0','1.0','5.00','1.0','2020Q4','0.752555','5.6440','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','3045.0','0.3865353037766831','1177.0','1.0','5.00','1.0','2020Q4','-999.0','-999.0000','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','85','8187.0','0.010382313423720533','227.0','1.0','2.17','14.0','2021Q2','0.944006','1.2987','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','85','1028.0','0.08268482490272373','227.0','1.0','2.17','3.0','2021Q2','-999.0','-999.0000','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','49','19210.0','0.0025507548152004164','696.0','1.0','0.81','78.0','2020Q2','0.938393','-0.1368','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','49','2241.0','0.02186523873270861','436.0','1.0','0.81','10.0','2020Q2','-999.0','-999.0000','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','364','22733.0','0.016011964984823823','1378.0','1.0','1.69','8.0','2020Q3','0.782385','1.1601','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','364','3796.0','0.0958904109589041','1378.0','1.0','1.69','3.0','2020Q3','-999.0','-999.0000','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','21636.0','0.054400073950822705','1177.0','1.0','5.00','1.0','2020Q4','0.753106','5.6392','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','3158.0','0.3727042431918936','1177.0','1.0','5.00','1.0','2020Q4','-999.0','-999.0000','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','181','30291.0','0.005975372222772441','992.0','1.0','1.32','42.0','2021Q1','0.829976','0.5904','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','181','2562.0','0.070647931303669','412.0','1.0','1.32','3.0','2021Q1','-999.0','-999.0000','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','85','8191.0','0.010377243315834452','227.0','1.0','2.17','14.0','2021Q2','0.942918','1.3014','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','85','1032.0','0.08236434108527131','227.0','1.0','2.17','3.0','2021Q2','-999.0','-999.0000','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','49','19210.0','0.0025507548152004164','696.0','1.0','0.81','78.0','2020Q2','0.938393','-0.1368','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','49','2241.0','0.02186523873270861','436.0','1.0','0.81','10.0','2020Q2','-999.0','-999.0000','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','364','22733.0','0.016011964984823823','1378.0','1.0','1.69','8.0','2020Q3','0.782385','1.1601','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','364','3796.0','0.0958904109589041','1378.0','1.0','1.69','3.0','2020Q3','-999.0','-999.0000','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','21636.0','0.054400073950822705','1177.0','1.0','5.00','1.0','2020Q4','0.753106','5.6392','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','1177','3158.0','0.3727042431918936','1177.0','1.0','5.00','1.0','2020Q4','-999.0','-999.0000','2021-09-18 00:00:00.0','2021-09-18 00:00:00.0','0');

insert into dws_db_prd.dws_newest_investment_pop_rownumber_quarter(city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,create_time,update_time,dr)
 values('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','1','776.0','0.001288659793814433','31.0','1.0','0.50','21.0','2020Q2','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','1','90.0','0.011111111111111112','24.0','1.0','0.50','8.0','2020Q2','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','23','2719.0','0.008458992276572269','75.0','1.0','1.84','18.0','2021Q1','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','23','205.0','0.11219512195121951','42.0','1.0','1.84','2.0','2021Q1','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','9','913.0','0.009857612267250822','36.0','1.0','1.53','14.0','2020Q3','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','9','90.0','0.1','17.0','1.0','1.53','3.0','2020Q3','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','8','610.0','0.013114754098360656','15.0','1.0','2.75','6.0','2021Q2','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','8','74.0','0.10810810810810811','15.0','1.0','2.75','3.0','2021Q2','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','127','2135.0','0.059484777517564404','127.0','1.0','5.00','1.0','2020Q4','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','127','294.0','0.43197278911564624','127.0','1.0','5.00','1.0','2020Q4','2021-09-16 00:00:00.0','2021-09-16 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','9','913.0','0.009857612267250822','36.0','1.0','1.53','14.0','2020Q3','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','9','90.0','0.1','17.0','1.0','1.53','3.0','2020Q3','2021-09-17 00:00:00.0','2021-09-17 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','1','776.0','0.001288659793814433','31.0','1.0','0.50','21.0','2020Q2','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','1','90.0','0.011111111111111112','24.0','1.0','0.50','8.0','2020Q2','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','9','913.0','0.009857612267250822','36.0','1.0','1.53','14.0','2020Q3','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','9','90.0','0.1','17.0','1.0','1.53','3.0','2020Q3','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','127','2146.0','0.05917986952469711','127.0','1.0','5.00','1.0','2020Q4','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','127','305.0','0.4163934426229508','127.0','1.0','5.00','1.0','2020Q4','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','23','2720.0','0.008455882352941176','75.0','1.0','1.84','18.0','2021Q1','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320600','ff55aa2d6cf682ce338b91d73bdbb6f6','8','610.0','0.013114754098360656','15.0','1.0','2.75','6.0','2021Q2','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','23','206.0','0.11165048543689321','42.0','1.0','1.84','2.0','2021Q1','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0'),('320681','ff55aa2d6cf682ce338b91d73bdbb6f6','8','74.0','0.10810810810810811','15.0','1.0','2.75','3.0','2021Q2','2021-09-22 00:00:00.0','2021-09-22 00:00:00.0','0');




show create table dws_db_prd.dws_newest_popularity_rownumber_quarter;
CREATE TABLE dws_db_prd.dws_newest_popularity_rownumber_quarter_aaaa (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` int(11) NOT NULL COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` decimal(10,2) DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period` varchar(10) DEFAULT NULL COMMENT '时间周期',
  `imei_c_avg` double DEFAULT NULL COMMENT '楼盘热度指数占比平均值',
  `index_rate_change` decimal(10,4) DEFAULT NULL COMMENT '楼盘热度指数占比与本市均值对比情况',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '数据更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识（1 无效  0 有效）',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单表';

load data local infile 'D:/EJU/after_20210520/03_DataAnalysis/02_data/bak/202110/bak_20211015_dws_newest_popularity_rownumber_quarter/bak_20211015_dws_newest_popularity_rownumber_quarter.txt__58cbe389_2437_4bca_8bc9_29ef60c09609' into table dws_db_prd.dws_newest_popularity_rownumber_quarter_aaaa 
CHARACTER SET utf8 -- 可选，避免中文乱码问题
FIELDS TERMINATED BY '\t' -- 字段分隔符，每个字段(列)以什么字符分隔，默认是 \t
	OPTIONALLY ENCLOSED BY '' -- 文本限定符，每个字段被什么字符包围，默认是空字符
	ESCAPED BY '\\' -- 转义符，默认是 \
LINES TERMINATED BY '\n' -- 记录分隔符，如字段本身也含\n，那么应先去除，否则load data 会误将其视作另一行记录进行导入
(id,city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,imei_c_avg,index_rate_change,create_time,update_time,dr) -- 每一行文本按顺序对应的表字段，建议不要省略
;


show create table dws_db_prd.dws_newest_investment_pop_rownumber_quarter ;
CREATE TABLE dws_db_prd.dws_newest_investment_pop_rownumber_quarter_aaaa (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` int(11) NOT NULL COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` decimal(10,2) DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period` varchar(10) DEFAULT NULL COMMENT '时间周期',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '数据更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识（1 无效  0 有效）',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='投资型人气热度排行榜单表';

load data local infile 'D:/EJU/after_20210520/03_DataAnalysis/02_data/bak/202110/bak_20211015_dws_newest_investment_pop_rownumber_quarter/bak_20211015_dws_newest_investment_pop_rownumber_quarter.txt__e091d487_d755_4513_a379_2ab983aa6b17' into table dws_db_prd.dws_newest_investment_pop_rownumber_quarter_aaaa 
CHARACTER SET utf8 -- 可选，避免中文乱码问题
FIELDS TERMINATED BY '\t' -- 字段分隔符，每个字段(列)以什么字符分隔，默认是 \t
	OPTIONALLY ENCLOSED BY '' -- 文本限定符，每个字段被什么字符包围，默认是空字符
	ESCAPED BY '\\' -- 转义符，默认是 \
LINES TERMINATED BY '\n' -- 记录分隔符，如字段本身也含\n，那么应先去除，否则load data 会误将其视作另一行记录进行导入
(id,city_id,newest_id,imei_newest,imei_city,rate,imei_c_max,imei_c_min,index_rate,sort_id,period,create_time,update_time,dr) -- 每一行文本按顺序对应的表字段，建议不要省略
;


truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter_aaaa;

truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter_aaaa;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1021

## 爬取21城的poi
delete from dws_db_prd.dws_tag_purchase_poi where newest_id in('8fb5b8943470e8d382f0c3f444b4e2d6','2afb29f71540f4211717f2227236deb9','6ee7f1bb2e38aa2c5b91e0f038efbac4','634027bbc377533dc028d94047d9d50c','9494297e1ad4b7ab5d6373729ede3041','5fd89981eca512d368cdb33edbffa527','14234cce687007be9868948d0aec0a3b','1f3b3feb06fb0ebe544477d8a0c0e694','90367e97464d65dd53ce9ce657ae4ef8','649f648a1a341af0e2bff3e2b11b73d5','fbc125f6dbd53291a1de2de02eb5ceda','4e42b40873f0a4d08faa0179d2bea058','5d83d6e53a6c055f178c4a148f59e1be','6b9fafb1b508b7464a8917320779e018','d85fa285a6bbda27398c4106b005ee95','b07f1c1425a1adbbd2939a3686ab61a6','ca0b360989248477135d3d98c1fbc3bd','d6f17f90e174fd99114582ac1ba332cc','2b8b54a1c69624166865cb9b7c58537e','dd11364ebc265bbc92d00d86375854fb','7753ac78c10f8268d43054e0ce59787a');


select * from odsdb.ori_newest_period_admit_info where dr = 0;

update odsdb.ori_newest_period_admit_info set dr = 1 where dr = 0;

insert into odsdb.ori_newest_period_admit_info (ods_table_name,city_id,newest_id,newest_name,address,lnglat,poi_num,dr,create_time,update_time)
select 'newest_info',city_id,newest_id,newest_name,address,concat(cast(lng as char)+0,',',0+cast(lat as char)),0,0,now(),now() from dws_db_prd.dws_newest_info where newest_id in (
'8fb5b8943470e8d382f0c3f444b4e2d6',
'2afb29f71540f4211717f2227236deb9',
'6ee7f1bb2e38aa2c5b91e0f038efbac4',
'634027bbc377533dc028d94047d9d50c',
'9494297e1ad4b7ab5d6373729ede3041',
'5fd89981eca512d368cdb33edbffa527',
'14234cce687007be9868948d0aec0a3b',
'1f3b3feb06fb0ebe544477d8a0c0e694',
'90367e97464d65dd53ce9ce657ae4ef8',
'649f648a1a341af0e2bff3e2b11b73d5',
'fbc125f6dbd53291a1de2de02eb5ceda',
'4e42b40873f0a4d08faa0179d2bea058',
'5d83d6e53a6c055f178c4a148f59e1be',
'6b9fafb1b508b7464a8917320779e018',
'd85fa285a6bbda27398c4106b005ee95',
'b07f1c1425a1adbbd2939a3686ab61a6',
'ca0b360989248477135d3d98c1fbc3bd',
'd6f17f90e174fd99114582ac1ba332cc',
'2b8b54a1c69624166865cb9b7c58537e',
'dd11364ebc265bbc92d00d86375854fb',
'7753ac78c10f8268d43054e0ce59787a'
);





select t1.city_id,t1.county_id,t2.newest_id,t2.issue_code,t3.city_id,t3.city_name,t3.county_id,t3.county_name,t3.issue_s from 
  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id ,city_id ,county_id ) t1
left join 
  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
on t1.newest_id = t2.newest_id
inner join 
  (select * from dwb_db.dim_issue where dr = 0 and county_id is not null) t3
on t1.city_id = t3.city_id and t1.county_id = t3.county_id
where INSTR(t2.issue_code, t3.issue_s);


select a.newest_id,a.newest_name,a.city_id,a.county_id,b.county_name,a.address,max(a.issue_code) issue_code,0,now(),now() from 
  (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address from 
    (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '310000' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
  inner join
    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
  on t1.newest_id = t2.newest_id
  inner join (select city_id,region_id,city_name,region_name dws_db_prd.dim_geography where g group by  city_id,region_id,city_name,region_name) a
left join 
	(select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name,t3.county_id,t3.county_name from 
	  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id ,city_id ,county_id ) t1
	left join 
	  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
	on t1.newest_id = t2.newest_id
	left join 
	  (select * from dwb_db.dim_issue where dr = 0) t3
	on t1.city_id = t3.city_id and t1.county_id = t3.county_id
	where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id,t3.county_id,t3.county_name) b  
on a.newest_id = b.newest_id where b.newest_id is null group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address,b.county_name;



## 重跑榜单表数据
truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter;

truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter ;


delete from dws_db_prd.dws_newest_popularity_rownumber_quarter where period = '2018Q3';

update dws_db_prd.dws_newest_popularity_rownumber_quarter set city_id = '310115' where imei_c_avg = '-999' and newest_id = '917ec2f2b9962a0da21b37ee5d8e144a';
update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set city_id = '310115' where city_id = '310112' and newest_id = '917ec2f2b9962a0da21b37ee5d8e144a';







-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1021
## 通过高德api获得的城市和经纬度
show create table dws_db_prd.crawler_city_newest_lnglat_gd;
CREATE TABLE temp_db.crawler_city_newest_lnglat_gd (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(400) DEFAULT NULL COMMENT '楼id',
  `newest_name` varchar(400) NOT NULL COMMENT '楼名',
  `address` varchar(800) DEFAULT NULL COMMENT '楼地址',
  `city_id` varchar(100) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(100) DEFAULT NULL COMMENT '城市名',
  `lng` varchar(100) DEFAULT NULL COMMENT '原经纬',
  `lat` varchar(100) DEFAULT NULL COMMENT '原经纬',
  `gd_city` varchar(100) DEFAULT NULL COMMENT '高德城',
  `gd_district` varchar(100) DEFAULT NULL COMMENT '高德区县',
  `gd_lat` varchar(100) DEFAULT NULL COMMENT '高德经纬',
  `gd_lng` varchar(100) DEFAULT NULL COMMENT '高德经纬',
  `create_time` text COMMENT '插入时间',
  `county_name` varchar(400) DEFAULT NULL COMMENT '区县名',
  `change_clomn_index` varchar(5) DEFAULT NULL COMMENT '修改去区县城市标识  1 修改区县     2 修改城市    3 修改城市和区县',
  PRIMARY KEY (`id`),
  KEY `idx_tmp_crawler_city_newest_lnglat_gd_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=7330 DEFAULT CHARSET=utf8mb4 COMMENT='爬虫城市最新城市-经纬度-高德';

insert into temp_db.crawler_city_newest_lnglat_gd(newest_id,newest_name,address,city_id,city_name,lng,lat,gd_city,gd_district,gd_lat,gd_lng,create_time,county_name,change_clomn_index) select newest_id,newest_name,address,city_id,city_name,lng,lat,gd_city,gd_district,gd_lat,gd_lng,create_time,county_name,change_clomn_index from dws_db_prd.crawler_city_newest_lnglat_gd where newest_id is null;

delete from dws_db_prd.crawler_city_newest_lnglat_gd where newest_id is null ;


select newest_name,address,city_name,gd_district from temp_db.crawler_city_newest_lnglat_gd ;

select * from temp_db.tmp_newest_in_city_error ;

update temp_db.tmp_newest_in_city_error a ,(select newest_name,address,city_name,gd_district from temp_db.crawler_city_newest_lnglat_gd where create_time='2021-10-21 10:05:59') b set a.region = b.gd_district 
where a.newest_name = b.newest_name and a.city = b.city_name and a.address = b.address ;

select * from dws_db_prd.dws_newest_info where newest_name in (select newest_name from dws_db_prd.dws_newest_info where newest_name in (select newest_name from temp_db.tmp_newest_in_city_error ) group by newest_name having count(newest_name)>1) ;

update dws_db_prd.dws_newest_info a,(select t1.newest_id ,t2.region_id from (select newest_id,region,city from temp_db.tmp_newest_in_city_error where dr = 0 and newest_name in ('万和四季','万科-环萃园','万科翡翠天御府','万科鑫苑SkyPark云璞','上海湾','世茂-璀璨天悦','中南正荣海上明悦','中鑫花苑','乐颐小镇','光樾华庭','合景云溪四季庭','合生国际名都','吉宝旭辉熹阅','君地蔚林半岛','太仓熙岸原著','如意佳园','安吉-石榴玉兰湾','崇明岛大爱城','御景澜庭','恒大海上威尼斯','悦映澜庭','新力上海柳岸春风','新华联滨江雅苑','新南浔孔雀城-都会江南','旭辉梦想城','景秀江南','柳岸春风景苑','棕榈湾花园','江南桃源','泰禾华发姑苏院子','湖畔景园','爱家-曦霖樾','电建地产-明悦苑','碧桂园-湖山源著','碧桂园-狮山源著','碧桂园十里外滩','绿地铂瑞公寓','绿地长岛','聚福苑天玺','臻之美舍','苏河公园','观海华苑','金海湾-龙门府','鑫远-太湖健康城','锦博佳园','阳光城中南翡丽云邸','韵湖国际','韵湖豪庭','骏宏-龙湾府','黄浦湾-玉象府')) t1 left join (select city_id ,city_name,region_id ,region_name from dwb_db.dwb_dim_geography_55city group by city_id ,city_name,region_id ,region_name)t2 on t1.city = t2.city_name and t1.region = t2.region_name) b 
set a.county_id = b.region_id where a.newest_id = b.newest_id;

select newest_name from dws_db_prd.dws_newest_info where newest_name in (select newest_name from temp_db.tmp_newest_in_city_error ) group by newest_name having count(newest_name)=1;

##修改楼盘表对应正确城市
select * from dwb_db.dwb_dim_geography_55city ddgc where city_name = '嘉兴市' and region_name = '海盐县';

update dws_db_prd.dws_newest_info set county_id = '330424' where newest_id = 'c942b89c2d147096d0eaef4d906143c0';

select * from dwb_db.dwb_dim_geography_55city ddgc where city_name = '苏州市' and region_name = '吴江区';
update dws_db_prd.dws_newest_info set county_id = '320509' where newest_id = '17dbafb509af4151bc0e8969df571b5c';


select * from dwb_db.dwb_dim_geography_55city ddgc where city_name = '嘉兴市' and region_name = '南湖区';

update dws_db_prd.dws_newest_info set county_id = '330402' where newest_id = 'a1956d5b928a3b45dcd013374fca12ea';



select * from dwb_db.dwb_dim_geography_55city ddgc where city_name = '南通市' and region_name = '如皋市';

update dws_db_prd.dws_newest_info set county_id = '320682' where newest_id = '2f5503ae0a65a072e8f43d68e655213f';
update dws_db_prd.dws_newest_info set county_id = '320682' where newest_id = '61a809609010fe5a758e14b3f20d493b';

select * from dwb_db.dwb_dim_geography_55city ddgc where city_name = '湖州市' and region_name = '长兴县';

update dws_db_prd.dws_newest_info set county_id = '330522' where newest_id = 'f931fc508df023baeeeb15a997e588a3';


select * from dwb_db.dwb_dim_geography_55city where city_name = '南通市' and region_name = '启东市';

update dws_db_prd.dws_newest_info set county_id = '320681' where newest_id = 'a09fa424e403f093dc1ad877e1ad598c';


select * from dwb_db.dwb_dim_geography_55city where city_name = '苏州市' and region_name = '太仓市';

update dws_db_prd.dws_newest_info set county_id = '320585' where newest_id = 'ca0b360989248477135d3d98c1fbc3bd';


select * from dwb_db.dwb_dim_geography_55city where city_name = '嘉兴市' and region_name = '秀洲区';

update dws_db_prd.dws_newest_info set county_id = '330411' where newest_id = '948ee320995d453db8a6322bfc073af2';


select * from dws_db_prd.dws_newest_info where newest_name = '香悦四季';

select * from dws_db_prd.dws_newest_info where newest_name = '大理大华-锦绣华城';

update dws_db_prd.dws_newest_info set dr = 1 where newest_id in ('7dae2029d10cfa1a6b24fd0060ecc79a','84be99387c460a2b850fc79919c62158','53234bfe2375747ca4e53da30ebf671d','d1b4c797211ffe5ca12890bfaf5104b5','331daaa40ed5d03efedd17a2c5f861e4','cd83c7cbc9c6e84d549929dc4e0d28d5','5a85bd3178d40eeb16a1a05970fd058f','b60a33ce77c8a91710df9b0ac1a4c2bc','c5469d16b150bdd87dcd3003834f0b46','dedcec9cd7bce87082806070b0cdea72','3c7fd0ec1628524d2a0949c5598a1afc','b0ade1b6f389c0c4e91d1b285bb7c804','55c58a9195dedcfb325c4121c0fae5a4','5f5581e6b2b0a3e3d095abe3835690fc','7a42a72d4b6c3091cf340890158472b1','8896b66014148af9915e632aa45b0a83','8dc361a06dcd7d1e92a2314fac06b0e9','665ccf58b75432e513f9febd8b571473','16d2dda8bdae4a99cbaa18e5b5d1d15e','18e88bddb3d5e7185284cc6e755a581c','fe434ab9eac2afb8205b39a1f453a817','0d69530ce541ada658f6766ab2a6f8e4','a1e7206ec14317977bd86a4e3ff6f068','f0749f5a41b2369989dca779bff1fd54','873fa36f4b2800def2f08927dc9f5523','a5328926a6370c26ead474c38700a5cd','4e99d8e8b5c6c9e238e57431fd9bde84','d72b5abc155c03f376cfb36a510dcb36','1e25b358785aea629b1d3103546caa27','0af473d22ebdac2c9abeae6b6f276930','cd518390fadf8a05afc2ae2ecc47420f','9f3631b112a83efec293fd2bcb94d126','9d009b2a363d17ebcb03eac972aefc57','25c1705574bc2268d8435a681b8bce59','cd27da2967e34d977ddbdf5de2a0cd47','af8bc6f61bc35578ce852090a69dfd7f','3dc69ed9cc129164c39a40fc75be0ce7','eebeeec5205a145035b9589ce106fa4b','abde2394b37a0e5e7fa47827bf61c1cc','a9be5388107833177d4fb49052654751','bc9691e02a48fe49c2d0cacfad6d7b61');

update dws_db_prd.dws_newest_period_admit set dr = 1 where newest_id in ('7dae2029d10cfa1a6b24fd0060ecc79a','84be99387c460a2b850fc79919c62158','53234bfe2375747ca4e53da30ebf671d','d1b4c797211ffe5ca12890bfaf5104b5','331daaa40ed5d03efedd17a2c5f861e4','cd83c7cbc9c6e84d549929dc4e0d28d5','5a85bd3178d40eeb16a1a05970fd058f','b60a33ce77c8a91710df9b0ac1a4c2bc','c5469d16b150bdd87dcd3003834f0b46','dedcec9cd7bce87082806070b0cdea72','3c7fd0ec1628524d2a0949c5598a1afc','b0ade1b6f389c0c4e91d1b285bb7c804','55c58a9195dedcfb325c4121c0fae5a4','5f5581e6b2b0a3e3d095abe3835690fc','7a42a72d4b6c3091cf340890158472b1','8896b66014148af9915e632aa45b0a83','8dc361a06dcd7d1e92a2314fac06b0e9','665ccf58b75432e513f9febd8b571473','16d2dda8bdae4a99cbaa18e5b5d1d15e','18e88bddb3d5e7185284cc6e755a581c','fe434ab9eac2afb8205b39a1f453a817','0d69530ce541ada658f6766ab2a6f8e4','a1e7206ec14317977bd86a4e3ff6f068','f0749f5a41b2369989dca779bff1fd54','873fa36f4b2800def2f08927dc9f5523','a5328926a6370c26ead474c38700a5cd','4e99d8e8b5c6c9e238e57431fd9bde84','d72b5abc155c03f376cfb36a510dcb36','1e25b358785aea629b1d3103546caa27','0af473d22ebdac2c9abeae6b6f276930','cd518390fadf8a05afc2ae2ecc47420f','9f3631b112a83efec293fd2bcb94d126','9d009b2a363d17ebcb03eac972aefc57','25c1705574bc2268d8435a681b8bce59','cd27da2967e34d977ddbdf5de2a0cd47','af8bc6f61bc35578ce852090a69dfd7f','3dc69ed9cc129164c39a40fc75be0ce7','eebeeec5205a145035b9589ce106fa4b','abde2394b37a0e5e7fa47827bf61c1cc','a9be5388107833177d4fb49052654751','bc9691e02a48fe49c2d0cacfad6d7b61');

select newest_id from dwb_db.a_dwb_customer_browse_log adcbl  where newest_id  in (select old_id from dws_db.relation_newest) group by newest_id ;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1020
##不属于上海的楼盘
show create table dwb_db.dim_issue ;
CREATE TABLE temp_db.tmp_newest_in_city_error (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(100) DEFAULT NULL COMMENT '楼盘名',
  `city_id` int(11) DEFAULT NULL COMMENT '原城市id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `issue_code` varchar(200) DEFAULT NULL COMMENT '楼盘预售证',
  `city` varchar(200) DEFAULT NULL COMMENT '真所在城市',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建时',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更时',
  PRIMARY KEY (`id`),
  KEY `idx_tmp_newest_in_city_error_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='楼盘所在城市有误统计表';


## 修改楼盘的城市
select city_id,newest_name from dwb_db.a_dwb_customer_browse_log_1013 where newest_name like '%锦绣华城%' group by city_id,newest_name ;

select t1.newest_id ,t2.city_id from 
  (select newest_id,city from temp_db.tmp_newest_in_city_error where dr = 0) t1
left join 
  (select city_id ,city_name from dwb_db.dwb_dim_geography_55city group by city_id ,city_name)t2
on t1.city = t2.city_name;

update temp_db.tmp_newest_in_city_error set city = '苏州市' where concat(city,'市') in ('太仓市','常熟市','昆山市');

update temp_db.tmp_newest_in_city_error set city = concat(city,'市')  where city != '苏州市';

select * from dws_db_prd.dim_geography where region_name in ('大理市','太仓市','常熟市','昆山市');

select newest_id ,city from temp_db.tmp_newest_in_city_error where dr = 0;

update dws_db_prd.dws_newest_info set dr = 1 where newest_id in (select newest_id from temp_db.tmp_newest_in_city_error where dr = 1);

update dws_db_prd.dws_newest_period_admit set dr = 1 where newest_id in (select newest_id from temp_db.tmp_newest_in_city_error where dr = 1);

update dws_db_prd.dws_newest_info a,(select t1.newest_id ,t2.city_id from (select newest_id,city from temp_db.tmp_newest_in_city_error where dr = 0) t1 left join (select city_id ,city_name from dwb_db.dwb_dim_geography_55city group by city_id ,city_name)t2 on t1.city = t2.city_name) b set a.city_id = b.city_id where a.newest_id = b.newest_id;

-- 320500  320600   330100  330400  330500  610400

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1020
## 根据预售证查看不属于上海的楼盘
insert into temp_db.tmp_newest_in_city_error(newest_id,newest_name,city_id,address,issue_code,dr,create_time,update_time)
select a.newest_id,a.newest_name,a.city_id,a.address,max(a.issue_code) issue_code,0,now(),now() from 
  (select t2.newest_id,t1.newest_name,t1.city_id,t1.county_id,t2.issue_code,t1.address from 
    (select newest_id,newest_name,city_id,county_id,address from dws_db_prd.dws_newest_info where city_id = '310000' and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id) group by newest_id,city_id,county_id,address,newest_name ) t1
  inner join
    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code group by newest_id ,issue_code) t2
  on t1.newest_id = t2.newest_id) a
left join 
  (select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name from 
    (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id   ,city_id ,county_id ) t1
  left join 
    (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
  on t1.newest_id = t2.newest_id
  left join 
    (select * from dwb_db.dim_issue where dr = 0) t3
  on t1.city_id = t3.city_id
  where INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id) b 
on a.newest_id = b.newest_id where b.newest_id is null group by a.newest_id,a.newest_name,a.city_id,a.county_id,a.address;



select t2.newest_id,max(issue_code) code,t3.city_id,t3.city_name,t3.county_id,t3.county_name from 
  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id ,city_id ,county_id ) t1
left join 
  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
on t1.newest_id = t2.newest_id
left join 
  (select * from dwb_db.dim_issue where dr = 0) t3
on t1.city_id = t3.city_id and t1.county_id = t3.county_id
where not INSTR(t2.issue_code, t3.issue_s) group by t2.newest_id,t3.city_name,t3.city_id,t3.county_id,t3.county_name ;


select t1.city_id,t1.county_id,t2.newest_id,t2.issue_code,t3.city_id,t3.city_name,t3.county_id,t3.county_name,t3.issue_s from 
  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and city_id = '310000'  group by newest_id ,city_id ,county_id ) t1
left join 
  (select newest_id ,issue_code from dws_db_prd.dws_newest_issue_code where dr = 0 group by newest_id ,issue_code) t2
on t1.newest_id = t2.newest_id
inner join 
  (select * from dwb_db.dim_issue where dr = 0 and county_id is not null) t3
on t1.city_id = t3.city_id and t1.county_id = t3.county_id
where INSTR(t2.issue_code, t3.issue_s)






## 上海预售证简称

update dwb_db.dim_issue set dr = 1;

insert into dwb_db.dim_issue(city_id,city_name,county_id,county_name,issue_s,dr,create_time,update_time,order_index) select city_id ,city_name ,region_id county_,region_name ,'NULL',0,now(),now(),1  from dws_db_prd.dim_geography where grade = 4 and country_id is not null group by city_id ,city_name ,region_id ,region_name ;

delete from dwb_db.dim_issue where dr = 0;

delete from dwb_db.dim_issue where dr = 0 and city_id not in ('310000');

select * from dws_db_prd.dws_newest_issue_code where city_name = '南通市';



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1019

select * from dwb_db.dwb_newest_issue_offer;


select * from dwb_db.dwb_issue_supply_city where period = '2021Q3'; 
select * from dwb_db.dwb_issue_supply_city where dr = 0;

update dwb_db.dwb_newest_issue_offer set dr = 1 where city = '扬州市';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1018

select city_id from dws_db_prd.dws_newest_investment_pop_rownumber_quarter where period='2020Q4' and substr(city_id,5,2)='00' group by city_id ;

select cityid from dws_db_prd.dws_supply where period='2020Q4' group by cityid ;

select newest_id,sum(issue_room) issue_room ,issue_quarter from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_quarter ;

select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id;

select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name ;

select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from
  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a
left join 
  (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b
on a.city_id = b.city_id and a.county_id = b.region_id;


select t2.*,t1.issue_room,t1.issue_quarter from 
  (select newest_id,sum(issue_room) issue_room ,issue_quarter from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_quarter) t1
left join 
  (select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a left join (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b on a.city_id = b.city_id and a.county_id = b.region_id) t2
on t1.newest_id = t2.newest_id;

## 城市季度的供应套数
select city_id,city_name,city_id county_id, city_name county_name,sum(issue_room) issue_room ,issue_quarter from 
  (select newest_id,sum(issue_room) issue_room ,issue_quarter from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_quarter) t1
left join 
  (select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a left join (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b on a.city_id = b.city_id and a.county_id = b.region_id) t2
on t1.newest_id = t2.newest_id 
group by city_id,city_name,issue_quarter;

## 区县季度的供应套数
select city_id,city_name,county_id,county_name,sum(issue_room) issue_room ,issue_quarter from 
  (select newest_id,sum(issue_room) issue_room ,issue_quarter from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_quarter) t1
left join 
  (select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a left join (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b on a.city_id = b.city_id and a.county_id = b.region_id) t2
on t1.newest_id = t2.newest_id 
group by city_id,city_name,county_id,county_name,issue_quarter;

select newest_id,sum(issue_room) issue_room ,issue_month from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_month ;

select t2.*,t1.issue_room,t1.issue_month from 
  (select newest_id,sum(issue_room) issue_room ,issue_month from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_month ) t1
left join 
  (select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a left join (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b on a.city_id = b.city_id and a.county_id = b.region_id) t2
on t1.newest_id = t2.newest_id
 
## 区县月份的供应套数
select city_id,city_name,county_id,county_name,sum(issue_room) issue_room ,issue_month from 
  (select newest_id,sum(issue_room) issue_room ,issue_month from dwb_db.dwb_newest_issue_offer where newest_id is not null and dr != 1 group by newest_id,issue_month) t1
left join 
  (select newest_id,a.city_id,city_name,region_id county_id,region_name county_name from(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) a left join (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b on a.city_id = b.city_id and a.county_id = b.region_id) t2
on t1.newest_id = t2.newest_id 
group by city_id,city_name,county_id,county_name,issue_month;


## 查看时间为空的数据
select issue_code url from temp_db.city_newest_deal_data_check where city_name = '贵阳' and floor_name is not null and floor_name!='' and issue_date = '' and open_date = '' group by issue_code ;

## 更新数据
update temp_db.city_newest_deal_data_check set business = '贵州浩荣房地产开发有限公司' , issue_date = '2021年8月24日' , issue_area = '48084.65' where issue_code = '20210021';
update temp_db.city_newest_deal_data_check set business = '贵州华泽地产有限公司' , issue_date = '2021年8月27日' , issue_area = '17030.76' where issue_code = '20210023';
update temp_db.city_newest_deal_data_check set business = '贵阳峰瑞房地产开发有限公司' , issue_date = '2021年8月28日' , issue_area = '5030.76' where issue_code = '20210024';
update temp_db.city_newest_deal_data_check set business = '贵阳峰瑞房地产开发有限公司' , issue_date = '2021年8月28日' , issue_area = '9097.44' where issue_code = '20210025';
update temp_db.city_newest_deal_data_check set business = '贵阳恒大云景房地产开发有限公司' , issue_date = '2021年6月24日' , issue_area = '21034.38' where issue_code = '2021108';
update temp_db.city_newest_deal_data_check set business = '贵阳市金城汇置业有限公司' , issue_date = '2021年6月26日' , issue_area = '43860.37' where issue_code = '2021114';
update temp_db.city_newest_deal_data_check set business = '贵阳新世界房地产有限公司' , issue_date = '2021年7月10日' , issue_area = '3592.68' where issue_code = '2021127';
update temp_db.city_newest_deal_data_check set business = '贵阳恒大鑫丰房地产开发有限公司' , issue_date = '2021年8月3日' , issue_area = '24248.96' where issue_code = '2021129';
update temp_db.city_newest_deal_data_check set business = '贵阳中渝置地房地产开发有限公司' , issue_date = '2021年7月24日' , issue_area = '6028.62' where issue_code = '2021131';
update temp_db.city_newest_deal_data_check set business = '永鹏生活服务（贵州）有限公司' , issue_date = '2021年8月1日' , issue_area = '15402.52' where issue_code = '2021137';
update temp_db.city_newest_deal_data_check set business = '贵阳龙湖度势文化产业发展有限公司' , issue_date = '2021年8月1日' , issue_area = '11757.1' where issue_code = '2021138';
update temp_db.city_newest_deal_data_check set business = '贵阳远汇房地产开发有限公司' , issue_date = '2021年8月7日' , issue_area = '22638.58' where issue_code = '2021139';
update temp_db.city_newest_deal_data_check set business = '贵阳雅正房地产开发有限公司' , issue_date = '2021年8月24日' , issue_area = '43232.43' where issue_code = '2021144';
update temp_db.city_newest_deal_data_check set business = '贵阳盘江置业有限公司' , issue_date = '2021年9月3日' , issue_area = '31328.62' where issue_code = '2021151';
update temp_db.city_newest_deal_data_check set business = '贵阳融创宸扬房地产开发有限公司' , issue_date = '2021年9月15日' , issue_area = '18151.22' where issue_code = '2021157';
update temp_db.city_newest_deal_data_check set business = '贵州华胜永信置业有限公司' , issue_date = '2021年9月17日' , issue_area = '14841.93' where issue_code = '2021158';
update temp_db.city_newest_deal_data_check set business = '贵州中广文创城置业有限公司' , issue_date = '2021年9月18日' , issue_area = '59388.5' where issue_code = '2021160';
update temp_db.city_newest_deal_data_check set business = '贵州恒丰伟业房地产开发有限公司' , issue_date = '2021年9月18日' , issue_area = '25410.4' where issue_code = '2021161';
update temp_db.city_newest_deal_data_check set business = '贵阳合纵置业有限公司' , issue_date = '2021年9月18日' , issue_area = '15141.82' where issue_code = '2021162';
update temp_db.city_newest_deal_data_check set business = '贵州天亿置业有限公司' , issue_date = '2021年9月19日' , issue_area = '71100.01' where issue_code = '2021165';
update temp_db.city_newest_deal_data_check set business = '贵州谦和房地产开发有限公司' , issue_date = '2021年9月23日' , issue_area = '78176.38' where issue_code = '2021167';
update temp_db.city_newest_deal_data_check set business = '贵阳德盛置业有限公司' , issue_date = '2021年9月26日' , issue_area = '16943.21' where issue_code = '2021170';
update temp_db.city_newest_deal_data_check set business = '贵阳德盛置业有限公司' , issue_date = '2021年9月29日' , issue_area = '23620.32' where issue_code = '2021173';
update temp_db.city_newest_deal_data_check set business = '贵阳合纵置业有限公司' , issue_date = '2021年9月30日' , issue_area = '7477.68' where issue_code = '2021174';
update temp_db.city_newest_deal_data_check set business = '贵阳美承房地产开发有限公司' , issue_date = '2021年9月30日' , issue_area = '62233.22' where issue_code = '2021176';
update temp_db.city_newest_deal_data_check set business = '贵阳广晟鑫德房地产开发有限公司' , issue_date = '2021年9月30日' , issue_area = '23455.99' where issue_code = '2021177';
update temp_db.city_newest_deal_data_check set business = '贵州恒丰伟业房地产开发有限公司' , issue_date = '2021年10月1日' , issue_area = '46961.13' where issue_code = '2021179';
update temp_db.city_newest_deal_data_check set business = '贵州中天浩宇房地产开发有限公司' , issue_date = '2021年10月1日' , issue_area = '32533.38' where issue_code = '2021180';
update temp_db.city_newest_deal_data_check set business = '贵州省清镇市森伟房地产有限公司' , issue_date = '2021年9月30日' , issue_area = '8623.92' where issue_code = '202137';


 select url,city_name,concat(city_name,'市') gd_city,floor_name,floor_name_new,clean_floor_name,floor_name floor_name_clean,address,business,issue_code,issue_date,
   str_to_date(replace(replace(replace(replace(replace(replace(replace(replace(replace(substring_index(case when length(issue_date) = 8 then concat(substr(issue_date,1,4),'-',substr(issue_date,5,2),'-',substr(issue_date,7,2)) when length(issue_date) > 10 then '9999-09-09' when length(issue_date) = 9 then '9999-09-09' when length(issue_date) < 8 then '9999-09-09' when issue_date = '' then '9999-09-09' when issue_date is null then '9999-09-09' else issue_date end , ',',1) ,'年','-'),'月','-'),'日','') ,'/','-') ,'.','-'),'0:00:00',''),'--','-'),'-19-','-9-'),'-30-','-3-'),'%Y-%m-%d') issue_date_clean,
   open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,now()
    from temp_db.city_newest_deal_data_check where city_name = '贵阳' and floor_name is not null and floor_name!='' and url = 'https://www.gyfc.net.cn/pro_query/index.aspx?yszh=20210021&qu=10';

select 
  str_to_date(case when length(issue_date) < 8 or issue_date = '' or issue_date is null then '9999-09-09' when length(issue_date) = 8 then concat(substr(issue_date,1,4),'-',substr(issue_date,5,2),'-',substr(issue_date,7,2)) else replace(replace(replace(replace(replace(replace(replace(replace(replace(substring_index(issue_date, ',',1) ,'年','-'),'月','-'),'日','') ,'/','-') ,'.','-'),'0:00:00',''),'--','-'),'-19-','-9-'),'-30-','-3-') end,'%Y-%m-%d') 
from temp_db.city_newest_deal_data_check where city_name = '贵阳' and floor_name is not null and floor_name!='' and url = 'https://www.gyfc.net.cn/pro_query/index.aspx?yszh=20210021&qu=10';


select issue_date,length(issue_date) from odsdb.city_newest_deal where city_name = '贵阳' and insert_time = '2021-10-11 16:29:52' group by issue_date ;


select * from temp_db.city_newest_deal_data_check where city_name = '武汉' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '武汉' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '武汉' group by open_date ;



   
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------




-- 1018
#服务器运行脚本
update dwb_db.a_dwb_customer_browse_log t1 join dws_db.relation_newest t2 on t1.newest_id = t2.old_id set t1.newest_id = t2.newest_id;

SHOW processlist;	

#阿里云新建数塔数据库
create table test (id int(11) , name varchar(100));
INSERT into test values(1,'张三'),(2,'李四'); 

create user 'ShuT'@'%' identified by 'password123456~!';
flush privileges;
create database ST;
select * from mysql.`user` u ;

-- 前言
-- 当你需要使mysql的用户授权访问某个数据库的权限时，执行命令后会出现Access denied for user ‘root’@’%’ to database…的情形
-- 缘由
-- 授权的权限没有打开
-- 解决
-- mysql> UPDATE mysql.user SET Grant_priv='Y', Super_priv='Y' WHERE User='root';
-- Query OK, 1 row affected (0.00 sec)
-- Rows matched: 1  Changed: 1  Warnings: 0
-- 
-- mysql> flush privileges;
-- Query OK, 0 rows affected (0.00 sec)

UPDATE mysql.user SET Grant_priv='Y', Super_priv='Y' WHERE User='root';

flush privileges;

GRANT ALL PRIVILEGES ON ST.* TO 'ShuT'@'%' IDENTIFIED BY 'password123456~!' WITH GRANT OPTION;

flush privileges;

#阿里云新建数塔数据库 --测试

show databases;

drop table test;

CREATE table test (id int(11) ,name varchar(20));

INSERT into test values(1,'张三'),(2,'李四');

CREATE table test.test (id int(11) ,name varchar(20));

select User,Host,authentication_string from mysql.user;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1015

update temp_db.city_newest_deal_data_check set city_name = '九江' where city_name = '';

select * from temp_db.city_newest_deal_data_check where city_name = '三亚' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '三亚' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '三亚' group by open_date ;

update dwb_db.dwb_newest_issue_offer set dr = 1 where issue_quarter = '2021Q2';


-- 人气榜单数据问题修复
select city_id from dws_db_prd.dws_newest_popularity_rownumber_quarter where period = '2021Q1' and right(city_id,2)='00' group by city_id ;

delete from dws_db_prd.dws_newest_popularity_rownumber_quarter where period = '2021Q1';


-- 测试加载数据

show create table dwb_db.a_dwb_customer_browse_log ;
CREATE TABLE temp_db.a_dwb_customer_browse_log (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `ori_id` int(11) DEFAULT NULL COMMENT '源数据ID',
  `ori_table` varchar(50) DEFAULT NULL COMMENT '源数据表',
  `imei` varchar(14) DEFAULT NULL COMMENT '解密后的IMEI',
  `city_id` varchar(20) DEFAULT NULL COMMENT '城市ID',
  `county_id` varchar(20) DEFAULT NULL COMMENT '区/县ID',
  `newest_name` varchar(50) DEFAULT NULL COMMENT '数塔楼盘名',
  `visit_month` varchar(6) DEFAULT NULL COMMENT '浏览月份',
  `visit_date` date DEFAULT NULL COMMENT '浏览日期',
  `pv` int(11) DEFAULT '0' COMMENT '浏览次数',
  `source` varchar(255) DEFAULT NULL COMMENT '来源，安卓or苹果',
  `idate` varchar(20) DEFAULT NULL,
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id,城市+数塔楼盘名与别名表关联获取楼盘id(uuid)',
  `create_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_user` varchar(45) DEFAULT NULL COMMENT '创建人',
  `update_date` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_user` varchar(45) DEFAULT NULL COMMENT '更新人',
  `current_week` varchar(20) DEFAULT NULL COMMENT '当前周数(period)',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=52880669 DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='客户浏览楼盘日志表（每日增量）';

load data local infile 'D:/EJU/after_20210520/03_DataAnalysis/02_data/bak/202110/bak_20211011_a_dwb_customer_browse_log/bak_20211011_a_dwb_customer_browse_log.txt' into table temp_db.a_dwb_customer_browse_log
CHARACTER SET utf8 -- 可选，避免中文乱码问题
FIELDS TERMINATED BY '\t' -- 字段分隔符，每个字段(列)以什么字符分隔，默认是 \t
	OPTIONALLY ENCLOSED BY '' -- 文本限定符，每个字段被什么字符包围，默认是空字符
	ESCAPED BY '\\' -- 转义符，默认是 \
LINES TERMINATED BY '\n' -- 记录分隔符，如字段本身也含\n，那么应先去除，否则load data 会误将其视作另一行记录进行导入
(id,ori_id,ori_table,imei,city_id,county_id,newest_name,visit_month,visit_date,pv,source,idate,newest_id,create_date,create_user,update_date,update_user,current_week) -- 每一行文本按顺序对应的表字段，建议不要省略
;



CREATE TABLE temp_db.user_info (
`id`  int UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'ID',
`name`  varchar(36) NULL COMMENT '姓名',
`age`  int NULL COMMENT '年龄',
`address`  varchar(255) NULL COMMENT '地址',
`create_date`  datetime NULL COMMENT '创建时间',
PRIMARY KEY (`id`)
)
COMMENT='用户信息表'
;
load data local infile 'C:/Users/86133/Desktop/test.txt' into table temp_db.user_info
CHARACTER SET utf8 -- 可选，避免中文乱码问题
FIELDS TERMINATED BY '||' -- 字段分隔符，每个字段(列)以什么字符分隔，默认是 \t
	OPTIONALLY ENCLOSED BY '' -- 文本限定符，每个字段被什么字符包围，默认是空字符
	ESCAPED BY '\\' -- 转义符，默认是 \
LINES TERMINATED BY '\n' -- 记录分隔符，如字段本身也含\n，那么应先去除，否则load data 会误将其视作另一行记录进行导入
(id, name, age, address, create_date) -- 每一行文本按顺序对应的表字段，建议不要省略
;



DROP PROCEDURE ecommerce.BatchInsertCustomer IF EXISTS;
delimiter //
CREATE PROCEDURE BatchInsertCustomer(IN start INT,IN loop_time INT)
  BEGIN
      DECLARE Var INT;
      DECLARE ID INT;
      SET Var = 0;
      SET ID= start;
      WHILE Var < loop_time DO
          insert into customer(ID, email, name, password, phone, birth, sex, avatar, address, regtime, lastip, modifytime) 
          values (ID, 
                  CONCAT(ID, '@sina.com'), 
                  CONCAT('name_', rand(ID)*10000 mod 200), 
                  123456, 
                  13800000000, 
                  adddate('1995-01-01', (rand(ID)*36520) mod 3652), 
                  Var%2, 
                  'http:///it/u=2267714161, 58787848&fm=52&gp=0.jpg', 
                  '北京市海淀区', 
                  adddate('1995-01-01', (rand(ID)*36520) mod 3652), 
                  '8.8.8.8', 
                  adddate('1995-01-01', (rand(ID)*36520) mod 3652));
          SET Var = Var + 1;
          SET ID= ID + 1;
      END WHILE;
//
delimiter

ALTER TABLE customer DISABLE KEYS;
CALL BatchInsertCustomer(1, 2000000);
ALTER TABLE customer ENABLE KEYS;


-- 重跑数据

select newest_id from dws_db.relation_newest group by newest_id ;

truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter ;

truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter ;

-- 重跑poi

truncate table dws_db_prd.bak_20211009_dws_tag_purchase_poi; 

insert into dws_db_prd.bak_20211015_dws_tag_purchase_poi select * from dws_tag_purchase_poi ;

delete from dws_db_prd.dws_tag_purchase_poi where newest_id in ('49c8f3b4c8f41eeb1ab92533471f3207','05169b5d0d953bbb661cedca375c8461','a19149bb4d3a33d9592b942930631b53','52072f72606f672dd74a6bce8cf69b4e','9b8070a2bf9c11eb86162cea7f6c2bde','76123c2ea4aa6349d58fc2b5e4f7dca7','811bcc15d93225d44bc767ba33cc733e','35e12b091b2ff43fdef4462e9105397c','7d1e53880baba561c2c8f0c1997739ac','47177376ef70fd0c4faef88fe380affc','95d7683edd652671a9e7d15d0bdb08f6','c9d4330f0bb1fce7c7845bb0128c2c7c','e51d8e6d5ce2e753799fc7a46d5ddf6c','1647715e568d69c861e67b6364e7e424','8600847fe37de6dcfa956532c4b2d92a','83fabc23adf89670956a13bc440e8fc5');

select * from odsdb.ori_newest_period_admit_info ;

update odsdb.ori_newest_period_admit_info set dr = 1 where dr = 0;

insert into odsdb.ori_newest_period_admit_info(ods_id,ods_table_name,city_id,newest_id,newest_name,address,lnglat,poi_num,dr,create_time,update_time) select null ods_id,'dws_db.dws_newest_info' ,city_id,newest_id,newest_name,address,concat(lng,',',lat) lnglat,0,0,now(),now() from dws_db_prd.dws_newest_info where dr=0 and newest_id in ('49c8f3b4c8f41eeb1ab92533471f3207','05169b5d0d953bbb661cedca375c8461','a19149bb4d3a33d9592b942930631b53','52072f72606f672dd74a6bce8cf69b4e','9b8070a2bf9c11eb86162cea7f6c2bde','76123c2ea4aa6349d58fc2b5e4f7dca7','811bcc15d93225d44bc767ba33cc733e','35e12b091b2ff43fdef4462e9105397c','7d1e53880baba561c2c8f0c1997739ac','47177376ef70fd0c4faef88fe380affc','95d7683edd652671a9e7d15d0bdb08f6','c9d4330f0bb1fce7c7845bb0128c2c7c','e51d8e6d5ce2e753799fc7a46d5ddf6c','1647715e568d69c861e67b6364e7e424','8600847fe37de6dcfa956532c4b2d92a','83fabc23adf89670956a13bc440e8fc5');

select ori_table,count(1),1 from dwb_db.a_dwb_customer_browse_log where ori_table like 'cust_browse_log_2021%' group by ori_table
union all
select ori_table,count(1),2 from dwb_db.a_dwb_customer_browse_log_1001 where ori_table like 'cust_browse_log_2021%' group by ori_table;



-- 浏览日志表缺失数据补充
delete from dwb_db.a_dwb_customer_browse_log where ori_table = 'cust_browse_log_201807';

insert into dwb_db.a_dwb_customer_browse_log(ori_id,ori_table,imei,city_id,county_id,newest_name,visit_month,visit_date,pv,source,idate,newest_id,create_date,create_user,update_date,update_user,current_week) select ori_id,ori_table,imei,city_id,county_id,newest_name,visit_month,visit_date,pv,source,idate,newest_id,create_date,create_user,update_date,update_user,current_week from dwb_db.a_dwb_customer_browse_log_1001 where ori_table in ('cust_browse_log_201801','cust_browse_log_201802','cust_browse_log_201803','cust_browse_log_201804','cust_browse_log_201805','cust_browse_log_201806','cust_browse_log_201807');


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1014

-- 修改7城市样例楼盘的问题数据
update dws_db_prd.dws_newest_issue_code set dr = 1 where newest_id = '624c1de427a4da6e1ab385eac1ae5cfe' and issue_code = '2019500031';
update dws_db_prd.dws_customer_visit_reality_rate set reality_rate = 0.011 ,city_rate_cre = (0.011/city_reality_rate)-1 where period = '2021Q2' and newest_id = '3b23151ce5af3724edf985857051a885';

select * from dws_db_prd.dws_customer_visit_reality_rate where newest_id = '3b23151ce5af3724edf985857051a885';

update dws_db_prd.dws_customer_visit_reality_rate set reality_rate = 0.011 ,city_rate_cre = (0.011/city_reality_rate)-1 where period = '2021Q2' and newest_id = '3b23151ce5af3724edf985857051a885';

select (0.011/city_reality_rate)-1 from dws_db_prd.dws_customer_visit_reality_rate where period='2021Q2' and newest_id = '3b23151ce5af3724edf985857051a885';

select * from dws_db_prd.dws_newest_issue_code where newest_id = '624c1de427a4da6e1ab385eac1ae5cfe';

update dws_db_prd.dws_newest_issue_code set dr = 1 where newest_id = '624c1de427a4da6e1ab385eac1ae5cfe' and issue_code = '2019500031';

select * from dws_db_prd.dws_newest_issue_code where newest_id = '624c1de427a4da6e1ab385eac1ae5cfe'and issue_code = '2019500031'; 


--  7城市杨丽楼盘效验查找 '上海市','北京市','广州市','深圳市','南京市','武汉市','重庆市'

select t1.newest_id,t1.newest_name,t2.intention,t1.city_id,t4.city_name,t5.issue_code from 
  (select * from dws_db_prd.dws_newest_info where 
    city_id in (select city_id from dwb_db.dwb_dim_geography_55city where city_name in ('重庆市') group by city_id,city_name)
    and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and browse in ('A','B','C') and portrait in ('A','B','C') group by newest_id)
    and building_area is not null and unit_price is not null
    ) t1
inner join
  (select newest_id,max(intention) intention from dwb_db.dwb_newest_customer_info where period = `quarter` and intention > 1 group by newest_id) t2 
on t1.newest_id = t2.newest_id
inner join 
  (select newest_id,max(layout_area) layout_area from dws_db_prd.dws_newest_layout group by newest_id) t3
on t1.newest_id = t3.newest_id
inner join 
  (select city_id,city_name from dwb_db.dwb_dim_geography_55city where city_name in ('重庆市') group by city_id,city_name) t4
on t1.city_id = t4.city_id
inner join 
  (select newest_id,max(issue_code) issue_code from dws_db_prd.dws_newest_issue_code group by newest_id) t5
on t1.newest_id = t5.newest_id
where newest_name in ('桂语九里','龙湖开元','揽江雅苑');




## 单体标签拆分

select id,LENGTH(tex)-LENGTH(REPLACE(tex ,'}, {','')) num,substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",1)  `1`,substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",2),"}, {'tag_value': [",1),"], 'tagid': ",-1) `1_id`,substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",2),"}, {'tag_value': [",-1) `2`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",3),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `2_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",3),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `3`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",4),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `3_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",4),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `4`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",5),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `4_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",5),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `5`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",6),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `5_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",6),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `6`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",7),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `6_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",7),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `7`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",8),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `7_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",8),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `8`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",9),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `8_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",9),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `9`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",10),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `9_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",10),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `10`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",11),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `10_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",11),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `11`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",12),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `11_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",12),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `12`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",13),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `12_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",13),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `13`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",14),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `13_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",14),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `14`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",15),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `14_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",15),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `15`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",16),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `15_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",16),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `16`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `16_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `17`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `17_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",18),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `18`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",18),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `18_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",19),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `19`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",19),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `19_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",20),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `20`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",20),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `20_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",21),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `21`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",21),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `21_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",22),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `22`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",22),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `22_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",23),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `23`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",23),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `23_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",24),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `24`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",24),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `24_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",25),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `25`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",25),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `25_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",26),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `26`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",26),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `26_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",27),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `27`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",27),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `27_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",28),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `28`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",28),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `28_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",29),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `29`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",29),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `29_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",30),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `30`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",30),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `30_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",31),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `31`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",31),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `31_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",32),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `32`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",32),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `32_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",33),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `33`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",33),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `33_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",34),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `34`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",34),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `34_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",35),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `35`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",35),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `35_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",36),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `36`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",36),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `36_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",37),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `37`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",37),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `37_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",38),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `38`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",38),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `38_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",39),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `39`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",39),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `39_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",40),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `40`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",40),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `40_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",41),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `41`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",41),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `41_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",42),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `42`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",42),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `42_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",43),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `43`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",43),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `43_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",44),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `44`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",44),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `44_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",45),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `45`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",45),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `45_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",46),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `46`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",46),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `46_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",47),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `47`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",47),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `47_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",48),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `48`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",48),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `48_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",49),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `49`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",49),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `49_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",50),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `50`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",50),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `50_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",51),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `51`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",51),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `51_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",52),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `52`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",52),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `52_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",53),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `53`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",53),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `53_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",54),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `54`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",54),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `54_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",55),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `55`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",55),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `55_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",56),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `56`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",56),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `56_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",57),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `57`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",57),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `57_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",58),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `58`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",58),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `58_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",59),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `59`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",59),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `59_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",60),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `60`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",60),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `60_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",61),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `61`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",61),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `61_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",62),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `62`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",62),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `62_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",63),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `63`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",63),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `63_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",64),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `64`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",64),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `64_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",65),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `65`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",65),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `65_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",66),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `66`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",66),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `66_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",67),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `67`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",67),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `67_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",68),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `68`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",68),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `68_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",69),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `69`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",69),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `69_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",70),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `70`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",70),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `70_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",71),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `71`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",71),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `71_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",72),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `72`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",72),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `72_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",73),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `73`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",73),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `73_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",74),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `74`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",74),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `74_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",75),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `75`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",75),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `75_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",76),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `76`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",76),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `76_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",77),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `77`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",77),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `77_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",78),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `78`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",78),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `78_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",79),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `79`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",79),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `79_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",80),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `80`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",80),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `80_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",81),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `81`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",81),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `81_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",82),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `82`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",82),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `82_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",83),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `83`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",83),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `83_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",84),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `84`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",84),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `84_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",85),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `85`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",85),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `85_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",86),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `86`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",86),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `86_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",87),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `87`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",87),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `87_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `88`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `88_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `88`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",89),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `88_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",89),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `89`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",90),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `89_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",90),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `90`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",91),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `90_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",91),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `91`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",92),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `91_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",92),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `92`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",93),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `92_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",93),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `93`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",94),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `93_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",94),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `94`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",95),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `94_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",95),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `95`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",96),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `95_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",96),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `96`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",97),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `96_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",97),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `97`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",98),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `97_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",98),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `98`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",99),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `98_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",99),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `99`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",100),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `99_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",100),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `100`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",101),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `100_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",101),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `101`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",102),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `101_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",102),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `102`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",103),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `102_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",103),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `103`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",104),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `103_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",104),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `104`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",105),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `104_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",105),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `105`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",106),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `105_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",106),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `106`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",107),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `106_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",107),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `107`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",108),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `107_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",108),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `108`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",109),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `108_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",109),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `109`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",110),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `109_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",110),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `110`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",111),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `110_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",111),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `111`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",112),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `111_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",112),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `112`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",113),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `112_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",113),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `113`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",114),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `113_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",114),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `114`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",115),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `114_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",115),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `115`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",116),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `115_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",116),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `116`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",117),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `116_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",117),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `117`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",118),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `117_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",118),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `118`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",119),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `118_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",119),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `119`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",120),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `119_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",120),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `120`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",121),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `120_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",121),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `121`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",122),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `121_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",122),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `122`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",123),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `122_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",123),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `123`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",124),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `123_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",124),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `124`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",125),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `124_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",125),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `125`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",126),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `125_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",126),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `126`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",127),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `126_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",127),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `127`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",128),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `127_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",128),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `128`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",129),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `128_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",129),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `129`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",130),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `129_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",130),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `130`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",131),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `130_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",131),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `131`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",132),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `131_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",132),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `132`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",133),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `132_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",133),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `133`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",134),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `133_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",134),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `134`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",135),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `134_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",135),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `135`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",136),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `135_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",136),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `136`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",137),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `136_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",137),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `137`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",138),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `137_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",138),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `138`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",139),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `138_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",139),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `139`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",140),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `139_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",140),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `140`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",141),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `140_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",141),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `141`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",142),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `141_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",142),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `142`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",143),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `142_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",143),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `143`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",144),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `143_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",144),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `144`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",145),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `144_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",145),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `145`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",146),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `145_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",146),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `146`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",147),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `146_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",147),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `147`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",148),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `147_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",148),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `148`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",149),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `148_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",149),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `149`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",150),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `149_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",150),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `150`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",151),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `150_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",151),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `151`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",152),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `151_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",152),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `152`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",153),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `152_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",153),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `153`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",154),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `153_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",154),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `154`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",155),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `154_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",155),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `155`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",156),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `155_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",156),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `156`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",157),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `156_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",157),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `157`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",158),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `157_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",158),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `158`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",159),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `158_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",159),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `159`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",160),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `159_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",160),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `160`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",161),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `160_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",161),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `161`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",162),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `161_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",162),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `162`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",163),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `162_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",163),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `163`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",164),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `163_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",164),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `164`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",165),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `164_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",165),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `165`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",166),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `165_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",166),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `166`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",167),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `166_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",167),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `167`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",168),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `167_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",168),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `168`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",169),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `168_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",169),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `169`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",170),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `169_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",170),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `170`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",171),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `170_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",171),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `171`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",172),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `171_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",172),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `172`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",173),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `172_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",173),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `173`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",174),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `173_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",174),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `174`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",175),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `174_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",175),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `175`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",176),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `175_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",176),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `176`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",177),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `176_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",177),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `177`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",178),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `177_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",178),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `178`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",179),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `178_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",179),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `179`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",180),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `179_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",180),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `180`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",181),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `180_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",181),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `181`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",182),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `181_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",182),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `182`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",183),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `182_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",183),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `183`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",184),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `183_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",184),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `184`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",185),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `184_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",185),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `185`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",186),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `185_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",186),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `186`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",187),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `186_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",187),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `187`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",188),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `187_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",188),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `188`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",189),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `188_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",189),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `189`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",190),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `189_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",190),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `190`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",191),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `190_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",191),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `191`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",192),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `191_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",192),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `192`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",193),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `192_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",193),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `193`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",194),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `193_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",194),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `194`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",195),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `194_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",195),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `195`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",196),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `195_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",196),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `196`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",197),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `196_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",197),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `197`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",198),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `197_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",198),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `198`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",199),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `198_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",199),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `199`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",200),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `199_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",200),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `200`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",201),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `200_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",201),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `201`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",202),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `201_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",202),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `202`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",203),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `202_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",203),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `203`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",204),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `203_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",204),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `204`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",205),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `204_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",205),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `205`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",206),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `205_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",206),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `206`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",207),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `206_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",207),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `207`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",208),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `207_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",208),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `208`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",209),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `208_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",209),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `209`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",210),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `209_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",210),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `210`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",211),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `210_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",211),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `211`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",212),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `211_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",212),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `212`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",213),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `212_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",213),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `213`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",214),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `213_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",214),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `214`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",215),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `214_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",215),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `215`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",216),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `215_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",216),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `216`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",217),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `216_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",217),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `217`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",218),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `217_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",218),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `218`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",219),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `218_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",219),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `219`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",220),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `219_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",220),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `220`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",221),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `220_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",221),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `221`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",222),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `221_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",222),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `222`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",223),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `222_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",223),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `223`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",224),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `223_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",224),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `224`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",225),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `224_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",225),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `225`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",226),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `225_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",226),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `226`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",227),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `226_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",227),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `227`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",228),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `227_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",228),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `228`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",229),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `228_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",229),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `229`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",230),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `229_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",230),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `230`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",231),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `230_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",231),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `231`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",232),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `231_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",232),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `232`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",233),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `232_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",233),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `233`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",234),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `233_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",234),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `234`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",235),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `234_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",235),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `235`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",236),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `235_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",236),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `236`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",237),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `236_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",237),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `237`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",238),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `237_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",238),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `238`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",239),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `238_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",239),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `239`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",240),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `239_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",240),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `240`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",241),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `240_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",241),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `241`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",242),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `241_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",242),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `242`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",243),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `242_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",243),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `243`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",244),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `243_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",244),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `244`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",245),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `244_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",245),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `245`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",246),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `245_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",246),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `246`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",247),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `246_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",247),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `247`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",248),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `247_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",248),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `248`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",249),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `248_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",249),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `249`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",250),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `249_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",250),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `250`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",251),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `250_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",251),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `251`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",252),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `251_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",252),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `252`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",253),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `252_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",253),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `253`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",254),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `253_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",254),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `254`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",255),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `254_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",255),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `255`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",256),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `255_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",256),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `256`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",257),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `256_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",257),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `257`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",258),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `257_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",258),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `258`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",259),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `258_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",259),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `259`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",260),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `259_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",260),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `260`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",261),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `260_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",261),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `261`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",262),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `261_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",262),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `262`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",263),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `262_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",263),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `263`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",264),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `263_id`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",264),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `264`,substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",265),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `264_id` from temp_db.test 

SELECT substring_index( substring_index( ( SELECT tex FROM test ), '}, {', help_topic_id + 1 ), '}, {',- 1 ) FROM mysql.help_topic 

select distinct a.*,substring_index(substring_index(a.tex, "'}, {'", b.help_topic_id + 1), '}, {',-1) NAME from test a JOIN mysql.help_topic b ON b.help_topic_id < ( LENGTH (a.tex) - LENGTH(REPLACE(a.tex, "'}, {'",''))+1);

create table dws.test (id int(11) , tex text);
insert into dws.test(id,tex) values(1,"[{'tag_value': ['是'], 'tagid': 'b35'}, {'tag_value': ['美食', '甜点'], 'tagid': 'b44'}, {'tag_value': ['济南市'], 'tagid': 'b63'}, {'tag_value': ['1.2w-1.4w'], 'tagid': 'b4'}, {'tag_value': ['[金融行业*-+农业银行*-+ABC]'], 'tagid': 'b34'}, {'tag_value': ['火车'], 'tagid': 'b59'}, {'tag_value': ['[广州市-龙归, 广州市-太和, 广州市-城郊, 济南市-泺源, 广州市-街口, 广州市-江埔]'], 'tagid': 'b8'}, {'tag_value': ['高'], 'tagid': '232'}, {'tag_value': ['已婚'], 'tagid': '16'}, {'tag_value': ['无'], 'tagid': '11'}, {'tag_value': ['重度活跃'], 'tagid': 'b28'}, {'tag_value': ['金融行业-农业银行'], 'tagid': 'b32'}, {'tag_value': ['是'], 'tagid': 'b51'}, {'tag_value': ['金融行业'], 'tagid': 'b33'}, {'tag_value': ['高'], 'tagid': 'b15'}, {'tag_value': ['二手闲置', '零售百货'], 'tagid': '26'}, {'tag_value': ['是'], 'tagid': 'b115'}, {'tag_value': ['欧珀'], 'tagid': 'c1'}, {'tag_value': [1199], 'tagid': '6'}, {'tag_value': ['Android'], 'tagid': '5'}, {'tag_value': ['团购'], 'tagid': '28'}, {'tag_value': ['查考试资料', '综合新闻', '读邮件', '政治军事', '外文阅读', '旅游攻略', '医疗咨询', '健身攻略', '论坛微博', '查药方', '查食谱'], 'tagid': '30'}, {'tag_value': ['手机'], 'tagid': '10'}, {'tag_value': ['K1'], 'tagid': '1'}, {'tag_value': ['是'], 'tagid': 'b48'}, {'tag_value': ['广州市'], 'tagid': 'b67'}, {'tag_value': ['广州市-从化区-新时代家园', '广州市-白云区-东湖映月', '广州市-从化区-嘉东广场（商用）', '广州市-越秀区-大中华幸福城', '广州市-越秀区-三季SOHO(商用)', '济南市-历下区-回民小区', '广州市-从化区-莱茵水岸', '广州市-从化区-嘉骏幸福里', '济南市-历下区-徐家花园', '广州市-从化区-名城御景绿洲', '济南市-历下区-世茂国际广场', '广州市-从化区-珠光云岭湖', '广州市-从化区-远达广场', '广州市-从化区-和府花园'], 'tagid': 'b72'}, {'tag_value': ['广州市', '济南市'], 'tagid': 'b70'}, {'tag_value': ['广州-白云-东湖映月', '广州-从化-兴业里', '广州-从化-洪山南路', '广州-白云-龙归城', '广州-从化-人盛大厦', '广州-从化-建云西路', '广州-从化-雍晟时代明苑', '广州-从化-德茵苑', '广州-从化-朝阳街小区', '广州-从化-彩云花园', '广州-从化-禤围', '广州-从化-紫泉别墅', '广州-从化-绿色家园-碧溪阁', '广州-白云-家和·东湖映月', '广州-从化-明辉楼', '广州-从化-团星村团星城基', '广州-从化-从化教师新村', '广州-从化-河滨苑', '广州-从化-广物·荔山雅筑', '广州-从化-文华阁', '广州-从化-镇南路小区', '广州-从化-怡富绿色家园', '广州-从化-上城湾畔B区', '广州-从化-东成路小区', '广州-从化-环城西路教师村', '广州-从化-城内路小区', '广州-从化-南山楼', '广州-从化-团星村', '广州-从化-上城湾畔C区别墅', '广州-从化-世纪绿洲花园'], 'tagid': 'b113'}, {'tag_value': ['广州市#白云区', '济南市#历下区', '广州市#越秀区', '广州市#从化区']");

select t.id,SUBSTRING_INDEX(t.NAME,"], 'tagid': '" ,-1) id,SUBSTRING_INDEX(SUBSTRING_INDEX(t.NAME,"'], 'tagid':" ,1),"tag_value': ['",-1) value,NAME from (
    select distinct a.*,substring_index(substring_index(a.tex, "'}, {'", b.help_topic_id + 1), '}, {',-1) NAME from test a JOIN mysql.help_topic b ON b.help_topic_id < ( LENGTH (a.tex) - LENGTH(REPLACE(a.tex, "'}, {'",''))+1)
) t

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1013
## 预售证信息清洗入库
select * from temp_db.city_newest_deal_data_check where city_name = '西安' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '西安' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '西安' group by open_date ;

select issue_date from odsdb.city_newest_deal where city_name = '石家庄' group by issue_date ;

select open_date from odsdb.city_newest_deal where city_name = '石家庄' group by open_date ;

update temp_db.city_newest_deal_data_check set open_date = '2021-08-30' where city_name = '成都' and substr(issue_code,7,9) <= '202138311'  and open_date = '现房';
update temp_db.city_newest_deal_data_check set open_date = '2021-09-30' where city_name = '成都' and substr(issue_code,7,9) > '202138311' and open_date = '现房';

## 浏览日志表再次迁移
SELECT
TABLE_NAME,
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'dw_a'
AND TABLE_NAME LIKE  '%a_dwb_customer_browse_log%';

delete from dwb_db.a_dwb_customer_browse_log where visit_month >= '202101';

truncate table dwb_db.a_dwb_customer_browse_log;

insert into dwb_db.a_dwb_customer_browse_log select * from dwb_db.a_dwb_customer_browse_log_1001;

SHOW processlist;	


## 单体标签拆分
create table temp_db.test (id int(11) , tex text);
insert into temp_db.test(id,tex) values(1,"[{'tag_value': ['是'], 'tagid': 'b35'}, {'tag_value': ['美食', '甜点'], 'tagid': 'b44'}, {'tag_value': ['济南市'], 'tagid': 'b63'}, {'tag_value': ['1.2w-1.4w'], 'tagid': 'b4'}, {'tag_value': ['[金融行业*-+农业银行*-+ABC]'], 'tagid': 'b34'}, {'tag_value': ['火车'], 'tagid': 'b59'}, {'tag_value': ['[广州市-龙归, 广州市-太和, 广州市-城郊, 济南市-泺源, 广州市-街口, 广州市-江埔]'], 'tagid': 'b8'}, {'tag_value': ['高'], 'tagid': '232'}, {'tag_value': ['已婚'], 'tagid': '16'}, {'tag_value': ['无'], 'tagid': '11'}, {'tag_value': ['重度活跃'], 'tagid': 'b28'}, {'tag_value': ['金融行业-农业银行'], 'tagid': 'b32'}, {'tag_value': ['是'], 'tagid': 'b51'}, {'tag_value': ['金融行业'], 'tagid': 'b33'}, {'tag_value': ['高'], 'tagid': 'b15'}, {'tag_value': ['二手闲置', '零售百货'], 'tagid': '26'}, {'tag_value': ['是'], 'tagid': 'b115'}, {'tag_value': ['欧珀'], 'tagid': 'c1'}, {'tag_value': [1199], 'tagid': '6'}, {'tag_value': ['Android'], 'tagid': '5'}, {'tag_value': ['团购'], 'tagid': '28'}, {'tag_value': ['查考试资料', '综合新闻', '读邮件', '政治军事', '外文阅读', '旅游攻略', '医疗咨询', '健身攻略', '论坛微博', '查药方', '查食谱'], 'tagid': '30'}, {'tag_value': ['手机'], 'tagid': '10'}, {'tag_value': ['K1'], 'tagid': '1'}, {'tag_value': ['是'], 'tagid': 'b48'}, {'tag_value': ['广州市'], 'tagid': 'b67'}, {'tag_value': ['广州市-从化区-新时代家园', '广州市-白云区-东湖映月', '广州市-从化区-嘉东广场（商用）', '广州市-越秀区-大中华幸福城', '广州市-越秀区-三季SOHO(商用)', '济南市-历下区-回民小区', '广州市-从化区-莱茵水岸', '广州市-从化区-嘉骏幸福里', '济南市-历下区-徐家花园', '广州市-从化区-名城御景绿洲', '济南市-历下区-世茂国际广场', '广州市-从化区-珠光云岭湖', '广州市-从化区-远达广场', '广州市-从化区-和府花园'], 'tagid': 'b72'}, {'tag_value': ['广州市', '济南市'], 'tagid': 'b70'}, {'tag_value': ['广州-白云-东湖映月', '广州-从化-兴业里', '广州-从化-洪山南路', '广州-白云-龙归城', '广州-从化-人盛大厦', '广州-从化-建云西路', '广州-从化-雍晟时代明苑', '广州-从化-德茵苑', '广州-从化-朝阳街小区', '广州-从化-彩云花园', '广州-从化-禤围', '广州-从化-紫泉别墅', '广州-从化-绿色家园-碧溪阁', '广州-白云-家和·东湖映月', '广州-从化-明辉楼', '广州-从化-团星村团星城基', '广州-从化-从化教师新村', '广州-从化-河滨苑', '广州-从化-广物·荔山雅筑', '广州-从化-文华阁', '广州-从化-镇南路小区', '广州-从化-怡富绿色家园', '广州-从化-上城湾畔B区', '广州-从化-东成路小区', '广州-从化-环城西路教师村', '广州-从化-城内路小区', '广州-从化-南山楼', '广州-从化-团星村', '广州-从化-上城湾畔C区别墅', '广州-从化-世纪绿洲花园'], 'tagid': 'b113'}, {'tag_value': ['广州市#白云区', '济南市#历下区', '广州市#越秀区', '广州市#从化区']");


select * 
select 
    substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",1)  `1`,
    substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",2),"}, {'tag_value': [",1),"], 'tagid': ",-1) `1_id`,
    substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",2),"}, {'tag_value': [",-1) `2`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",3),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `2_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-1),"], 'tagid': ",3),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `3`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",4),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `3_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",4),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `4`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",5),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `4_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",5),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `5`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",6),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `5_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",6),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `6`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",7),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `6_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",7),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `7`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",8),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `7_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",8),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `8`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",9),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `8_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",9),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `9`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",10),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `9_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",10),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `10`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",11),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `10_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",11),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `11`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",12),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `11_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",12),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `12`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",13),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `12_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",13),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `13`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",14),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `13_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",14),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `14`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",15),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `14_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",15),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `15`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",16),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `15_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",16),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `16`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `16_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `17`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",17),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `17_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",18),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `18`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",18),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `18_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",19),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `19`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",19),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `19_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",20),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `20`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",20),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `20_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",21),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `21`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",21),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `21_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",22),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `22`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",22),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `22_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",23),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `23`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",23),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `23_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",24),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `24`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",24),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `24_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",25),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `25`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",25),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `25_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",26),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `26`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",26),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `26_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",27),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `27`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",27),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `27_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",28),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `28`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",28),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `28_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",29),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `29`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",29),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `29_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",30),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `30`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",30),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `30_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",31),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `31`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",31),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `31_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",32),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `32`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",32),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `32_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",33),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `33`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",33),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `33_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",34),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `34`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",34),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `34_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",35),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `35`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",35),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `35_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",36),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `36`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",36),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `36_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",37),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `37`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",37),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `37_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",38),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `38`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",38),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `38_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",39),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `39`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",39),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `39_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",40),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `40`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",40),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `40_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",41),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `41`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",41),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `41_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",42),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `42`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",42),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `42_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",43),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `43`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",43),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `43_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",44),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `44`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",44),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `44_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",45),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `45`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",45),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `45_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",46),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `46`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",46),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `46_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",47),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `47`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",47),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `47_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",48),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `48`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",48),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `48_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",49),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `49`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",49),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `49_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",50),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `50`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",50),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `50_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",51),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `51`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",51),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `51_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",52),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `52`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",52),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `52_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",53),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `53`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",53),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `53_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",54),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `54`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",54),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `54_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",55),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `55`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",55),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `55_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",56),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `56`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",56),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `56_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",57),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `57`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",57),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `57_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",58),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `58`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",58),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `58_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",59),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `59`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",59),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `59_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",60),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `60`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",60),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `60_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",61),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `61`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",61),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `61_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",62),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `62`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",62),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `62_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",63),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `63`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",63),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `63_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",64),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `64`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",64),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `64_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",65),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `65`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",65),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `65_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",66),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `66`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",66),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `66_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",67),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `67`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",67),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `67_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",68),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `68`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",68),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `68_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",69),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `69`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",69),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `69_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",70),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `70`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",70),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `70_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",71),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `71`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",71),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `71_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",72),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `72`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",72),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `72_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",73),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `73`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",73),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `73_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",74),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `74`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",74),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `74_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",75),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `75`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",75),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `75_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",76),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `76`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",76),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `76_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",77),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `77`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",77),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `77_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",78),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `78`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",78),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `78_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",79),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `79`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",79),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `79_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",80),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `80`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",80),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `80_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",81),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `81`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",81),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `81_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",82),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `82`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",82),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `82_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",83),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `83`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",83),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `83_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",84),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `84`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",84),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `84_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",85),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `85`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",85),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `85_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",86),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `86`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",86),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `86_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",87),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `87`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",87),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `87_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `88`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `88_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",88),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `88`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",89),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `88_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",89),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `89`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",90),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `89_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",90),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `90`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",91),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `90_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",91),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `91`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",92),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `91_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",92),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `92`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",93),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `92_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",93),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `93`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",94),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `93_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",94),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `94`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",95),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `94_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",95),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `95`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",96),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `95_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",96),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `96`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",97),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `96_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",97),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `97`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",98),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `97_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",98),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `98`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",99),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `98_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",99),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `99`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",100),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `99_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",100),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `100`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",101),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `100_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",101),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `101`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",102),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `101_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",102),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `102`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",103),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `102_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",103),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `103`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",104),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `103_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",104),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `104`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",105),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `104_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",105),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `105`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",106),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `105_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",106),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `106`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",107),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `106_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",107),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `107`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",108),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `107_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",108),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `108`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",109),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `108_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",109),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `109`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",110),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `109_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",110),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `110`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",111),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `110_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",111),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `111`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",112),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `111_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",112),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `112`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",113),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `112_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",113),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `113`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",114),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `113_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",114),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `114`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",115),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `114_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",115),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `115`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",116),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `115_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",116),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `116`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",117),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `116_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",117),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `117`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",118),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `117_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",118),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `118`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",119),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `118_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",119),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `119`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",120),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `119_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",120),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `120`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",121),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `120_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",121),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `121`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",122),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `121_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",122),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `122`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",123),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `122_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",123),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `123`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",124),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `123_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",124),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `124`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",125),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `124_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",125),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `125`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",126),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `125_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",126),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `126`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",127),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `126_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",127),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `127`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",128),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `127_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",128),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `128`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",129),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `128_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",129),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `129`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",130),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `129_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",130),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `130`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",131),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `130_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",131),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `131`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",132),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `131_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",132),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `132`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",133),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `132_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",133),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `133`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",134),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `133_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",134),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `134`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",135),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `134_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",135),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `135`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",136),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `135_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",136),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `136`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",137),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `136_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",137),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `137`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",138),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `137_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",138),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `138`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",139),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `138_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",139),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `139`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",140),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `139_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",140),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `140`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",141),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `140_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",141),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `141`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",142),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `141_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",142),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `142`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",143),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `142_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",143),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `143`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",144),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `143_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",144),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `144`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",145),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `144_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",145),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `145`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",146),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `145_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",146),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `146`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",147),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `146_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",147),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `147`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",148),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `147_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",148),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `148`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",149),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `148_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",149),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `149`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",150),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `149_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",150),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `150`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",151),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `150_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",151),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `151`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",152),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `151_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",152),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `152`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",153),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `152_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",153),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `153`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",154),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `153_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",154),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `154`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",155),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `154_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",155),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `155`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",156),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `155_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",156),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `156`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",157),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `156_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",157),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `157`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",158),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `157_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",158),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `158`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",159),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `158_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",159),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `159`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",160),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `159_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",160),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `160`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",161),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `160_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",161),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `161`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",162),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `161_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",162),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `162`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",163),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `162_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",163),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `163`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",164),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `163_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",164),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `164`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",165),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `164_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",165),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `165`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",166),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `165_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",166),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `166`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",167),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `166_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",167),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `167`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",168),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `167_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",168),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `168`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",169),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `168_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",169),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `169`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",170),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `169_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",170),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `170`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",171),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `170_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",171),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `171`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",172),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `171_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",172),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `172`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",173),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `172_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",173),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `173`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",174),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `173_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",174),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `174`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",175),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `174_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",175),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `175`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",176),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `175_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",176),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `176`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",177),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `176_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",177),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `177`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",178),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `177_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",178),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `178`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",179),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `178_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",179),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `179`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",180),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `179_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",180),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `180`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",181),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `180_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",181),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `181`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",182),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `181_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",182),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `182`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",183),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `182_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",183),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `183`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",184),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `183_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",184),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `184`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",185),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `184_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",185),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `185`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",186),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `185_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",186),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `186`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",187),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `186_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",187),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `187`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",188),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `187_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",188),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `188`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",189),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `188_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",189),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `189`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",190),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `189_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",190),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `190`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",191),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `190_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",191),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `191`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",192),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `191_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",192),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `192`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",193),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `192_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",193),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `193`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",194),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `193_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",194),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `194`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",195),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `194_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",195),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `195`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",196),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `195_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",196),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `196`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",197),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `196_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",197),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `197`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",198),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `197_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",198),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `198`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",199),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `198_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",199),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `199`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",200),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `199_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",200),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `200`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",201),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `200_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",201),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `201`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",202),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `201_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",202),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `202`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",203),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `202_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",203),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `203`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",204),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `203_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",204),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `204`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",205),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `204_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",205),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `205`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",206),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `205_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",206),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `206`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",207),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `206_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",207),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `207`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",208),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `207_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",208),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `208`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",209),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `208_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",209),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `209`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",210),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `209_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",210),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `210`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",211),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `210_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",211),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `211`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",212),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `211_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",212),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `212`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",213),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `212_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",213),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `213`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",214),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `213_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",214),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `214`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",215),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `214_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",215),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `215`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",216),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `215_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",216),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `216`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",217),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `216_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",217),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `217`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",218),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `217_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",218),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `218`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",219),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `218_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",219),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `219`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",220),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `219_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",220),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `220`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",221),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `220_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",221),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `221`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",222),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `221_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",222),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `222`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",223),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `222_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",223),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `223`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",224),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `223_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",224),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `224`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",225),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `224_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",225),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `225`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",226),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `225_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",226),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `226`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",227),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `226_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",227),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `227`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",228),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `227_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",228),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `228`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",229),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `228_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",229),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `229`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",230),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `229_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",230),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `230`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",231),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `230_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",231),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `231`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",232),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `231_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",232),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `232`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",233),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `232_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",233),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `233`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",234),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `233_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",234),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `234`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",235),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `234_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",235),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `235`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",236),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `235_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",236),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `236`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",237),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `236_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",237),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `237`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",238),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `237_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",238),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `238`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",239),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `238_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",239),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `239`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",240),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `239_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",240),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `240`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",241),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `240_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",241),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `241`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",242),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `241_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",242),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `242`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",243),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `242_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",243),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `243`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",244),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `243_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",244),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `244`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",245),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `244_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",245),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `245`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",246),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `245_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",246),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `246`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",247),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `246_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",247),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `247`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",248),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `247_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",248),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `248`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",249),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `248_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",249),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `249`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",250),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `249_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",250),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `250`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",251),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `250_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",251),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `251`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",252),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `251_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",252),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `252`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",253),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `252_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",253),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `253`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",254),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `253_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",254),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `254`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",255),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `254_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",255),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `255`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",256),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `255_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",256),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `256`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",257),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `256_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",257),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `257`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",258),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `257_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",258),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `258`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",259),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `258_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",259),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `259`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",260),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `259_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",260),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `260`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",261),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `260_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",261),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `261`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",262),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `261_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",262),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `262`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",263),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `262_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",263),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `263`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",264),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `263_id`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",264),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",-1) `264`,
    substring_index(substring_index(substring_index(substring_index(substring_index(tex,"[{'tag_value': [",-2),"], 'tagid': ",265),"}, {",-2),"], 'tagid': ",-1),"}, {'tag_value': [",1) `264_id`,
    LENGTH(tex)-LENGTH(REPLACE(tex ,'}, {','')) num
from temp_db.test ;


select LENGTH(tex)-LENGTH(REPLACE(tex ,'}, {','')) from temp_db.test ;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1012

update a_dwb_customer_browse_log_202107_202109 set newest_id = 'eaea7ecaf0a0f649a924849492791588' ,city_id = '310000' ,county_id = '310115' where newest_name = '保利云上拾光'

select * from odsdb.city_newest_deal where city_name = '上海' and issue_code like '%预字0000229号%';

-- 添加预售证信息  添加动态信息
select * from dws_db_prd.dws_newest_issue_code where newest_id = 'eaea7ecaf0a0f649a924849492791588'; 
select * from dws_db_prd.dws_newest_provide_sche where newest_id = 'eaea7ecaf0a0f649a924849492791588'; 

insert into dws_db_prd.dws_newest_issue_code(city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time) values(
'310000','上海市','310115','浦东新区','eaea7ecaf0a0f649a924849492791588','保利云上拾光','浦东南六公路与宝溪路交叉口','上海建万置业有限公司','浦东新区房管（2021）预字0000229号','2021-09-02','2021Q3','202109',null,null,null,0,now(),now());

insert into dws_db_prd.dws_newest_provide_sche (newest_id,`date`,period,provide_title,provide_sche) values(
'eaea7ecaf0a0f649a924849492791588','2021-04-21','2021Q2','保利云上拾光已开启巡展','保利云上拾光目前已在长泰广场、川沙百联开启巡展，项目打造约74-99㎡装修高层，约104-125㎡流水墅境'),
('eaea7ecaf0a0f649a924849492791588','2021-05-02','2021Q2','保利云上拾光一期开盘主力75-99平','保利云上拾光一期开盘主力户型:约75平左右两房两厅一卫，约87平左右三房两厅一卫，约99平左右三房两厅两卫'),
('eaea7ecaf0a0f649a924849492791588','2021-05-21','2021Q2','保利云上拾光开盘时间待定','保利云上拾光开盘时间待定，项目规划了1044套15-18层小高层、192套叠加。主力产品为建面约74-99㎡高层产品，价格待定'),
('eaea7ecaf0a0f649a924849492791588','2021-05-31','2021Q2','保利云上拾光预计于6月开放售楼处','保利云上拾光预计于6月开放售楼处，该项目位于浦东川沙，将推建面约75-125㎡高层&叠加'),
('eaea7ecaf0a0f649a924849492791588','2021-06-06','2021Q2','保利云上拾光位于上海浦东川沙','保利云上拾光位于浦东川沙六灶南六公路宝溪路，目前巡展点在金科路地铁4号出口长泰广场。本次首推户型有三个，75平方二房，87平方三房，99平方三房。'),
('eaea7ecaf0a0f649a924849492791588','2021-06-21','2021Q2','保利云上拾光价格待定','保利云上拾光共1372户。轨交11号线迪士尼站，16号线野生动物园站， 2号线川沙站； 公交1036路、六灶2路，川沙5路； 自驾临近南六公路和申嘉湖高速、川南奉公路；。绿化率35%。智源小学、六灶小学、六灶中学；,绿都绣云里、比斯特上海购物村、绿地东海岸时代广场、天和商业广场、两港装饰城、绿地乐和城；,上海交通大学医学院附属第九人民医院祝桥分院（在建中）；,海天湖公园、六灶健身公园、祝桥航空产业园、商飞整装基地、鹿园工业园'),
('eaea7ecaf0a0f649a924849492791588','2021-06-23','2021Q2','保利云上拾光将于6月26日开放售楼处与样板间','保利云上拾光将于6月26日开放售楼处与样板间，将推出建面74-99㎡的2-3房的高层和104-125的叠墅房源'),
('eaea7ecaf0a0f649a924849492791588','2021-07-08','2021Q3','保利云上拾光价格待定','保利云上拾光价格待定。主力户型为75平-99平2居-3居。绿化率35%。保利云上拾光项目为高层。共1372户。'),
('eaea7ecaf0a0f649a924849492791588','2021-07-23','2021Q3','保利云上拾光价格待定','保利云上拾光价格待定。主力户型为75平-99平2居-3居。项目规划建设1372户。带装修交付。绿化率35%。保利云上拾光南六公路与宝溪路路口。轨交11号线迪士尼站，16号线野生动物园站， 2号线川沙站； 公交1036路、六灶2路，川沙5路； 自驾临近南六公路和申嘉湖高速、川南奉公路；。容积率2。项目为高层板楼'),
('eaea7ecaf0a0f649a924849492791588','2021-07-23','2021Q3','保利云上拾光目前已开启验资','保利云上拾光目前已开启验资，本期将推出75平-99平的2居-3居，价格待定。预计将于8月开启认筹，具体时间待定'),
('eaea7ecaf0a0f649a924849492791588','2021-08-07','2021Q3','保利云上拾光价格待定','保利云上拾光高层13栋、叠加12栋，15-18F约75-87-99㎡高层，4F约104-125㎡叠加。绿化率35%。带装修交付。保利云上拾光楼盘地址：南六公路与宝溪路路口。'),
('eaea7ecaf0a0f649a924849492791588','2021-08-11','2021Q3','保利云上拾光预计2021年10月入市','保利云上拾光预计2021年10月入市，目前项目验资中，售价待定，预计推出主力户型为建面约75平-99平2居-3居'),
('eaea7ecaf0a0f649a924849492791588','2021-08-26','2021Q3','保利云上拾光价格待定','保利云上拾光价格待定。容积率2。绿化率35%。保利云上拾光项目为高层。共1372户。保利云上拾光毛坯,带装修交付'),
('eaea7ecaf0a0f649a924849492791588','2021-09-02','2021Q3','保利云上拾光已取证认筹','保利云上拾光已于9月2日取证认筹，均价45000元/平，本次推出75平-99平的2居-3居房源'),
('eaea7ecaf0a0f649a924849492791588','2021-09-06','2021Q3','保利云上拾光于9月6日正式开启认筹','保利云上拾光于9月6日上午9点至9月12日上午12点正式开启认购，本次开盘将推出75平-99平的2-3房带装修房源，均价45000元/平'),
('eaea7ecaf0a0f649a924849492791588','2021-10-11','2021Q4','保利云上拾光10月11日开盘','保利云上拾光于10月11日开盘，本次开盘推出75平-99平的2-3房带装修房源，均价45000元/平');

select * from dws_db_prd.dws_newest_issue_code where newest_id = '04a7ee9906177471287182d2bf79274e'; 
select * from dws_db_prd.dws_newest_provide_sche where newest_id = '04a7ee9906177471287182d2bf79274e'; 

insert into dws_db_prd.dws_newest_issue_code(city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time) values(
'110000','北京市','110115','大兴区','04a7ee9906177471287182d2bf79274e','合生me悦','大兴南五环德茂站200米','北京合亦盛景置业有限公司','京房售证字（2021）开8号','2021-07-23','2021Q3','202107',null,null,null,0,now(),now());

insert into dws_db_prd.dws_newest_provide_sche (newest_id,`date`,period,provide_title,provide_sche) values(
'04a7ee9906177471287182d2bf79274e','2021-04-23','2021Q2','合生me悦55-120㎡户型待售','合生me悦，五环德茂地铁200米，55-120㎡一居至四居火爆蓄客中，合生汇品牌馆现已盛大开放，欢迎莅临品鉴！	'),
('04a7ee9906177471287182d2bf79274e','2021-05-22','2021Q2','合生me悦项目55-120㎡一居至四居待售','合生me悦，周边有14座公园环绕，围合城南低密生态住区，55-120㎡一居至四居火爆蓄客中，合生汇品牌馆现已盛大开放。价格待定。'),
('04a7ee9906177471287182d2bf79274e','2021-05-24','2021Q2','合生me悦价格待定','合生me悦项目共23栋楼,2栋东西向+21栋南北向，10-15层。绿化率30%。旧宫镇第一中心幼儿园、德茂幼儿园,旧宫中学、旧宫实验小学、旧宫第一中心小学、德茂中学、旧宫镇第二中心小学。从教育方面为您的孩子提供优质教育资源。,暂无资料,住总万科广场、山姆会员店、大族广场、力宝广场、爱琴海购物公园、航天万源广场，除此之外，西红门商圈的荟聚购物中心、宜家家居，也都能满足您全家吃喝玩乐一站式购物需求。,北京航天总医院、天坛医院（新址）、北京同仁医院（南区）、旧宫医院、亦庄医院、大兴中西医结合医院,暂无资料,暂无资料,项目紧邻北京最大的湿地公园---南海子公园。作为北京四大郊野公园之一，具有600余年的历史。是辽、金、元、明、清五朝皇家猎场，另外项目周边更拥有14座生态公园，具有非常高的生态价值，也被誉为公园地产。预计2021年下半年开盘。敬请关注。'),
('04a7ee9906177471287182d2bf79274e','2021-05-26','2021Q2','合生me悦项目待售','合生me悦项目，合生悦系 潮领新锐 继2018年成功打造使馆区的缦合·北京，分钟寺CBD国际生态区的合生缦云等高端品牌缦系，2021年合生开启全新“精品”悦系，首个悦系作品——me悦，领潮城市新锐，革新理想人居范本。'),
('04a7ee9906177471287182d2bf79274e','2021-07-07','2021Q3','合生me悦价格待定','合生me悦容积率2.34。项目周边路网发达，交通便捷，构成“三横、四纵、三轨道、一电车，多公交的的立体交通网络。①“三横”是指南五环（800米）、南四环（6公里）以及黄亦路。②“四纵”：是指京开高速、南中轴路、德贤路以及京台高速，其中德贤路双向8车道，全程无红绿灯，平常很少堵车。③“三轨道”：8号线（德茂站），亦庄线（旧宫站），s6规划轻轨（2023年开通，可直接换乘，开通后5站直达通州），东侧约200米即地铁8号线德茂站，11站到前门；13条地铁线接驳换乘，瞬时直达北京各核心区。④“一电车”：是指T1支1线，距离项目1公里，后期通车后约20分钟直达亦庄核心区。⑤周边公交车有341、453路可直达亦庄核心区，526、快速公交线1可直达木樨园、前门。带装修交付。旧宫镇第一中心幼儿园、德茂幼儿园,旧宫中学、旧宫实验小学、旧宫第一中心小学、德茂中学、旧宫镇第二中心小学。从教育方面为您的孩子提供优质教育资源。,暂无资料,住总万科广场、山姆会员店、大族广场、力宝广场、爱琴海购物公园、航天万源广场，除此之外，西红门商圈的荟聚购物中心、宜家家居，也都能满足您全家吃喝玩乐一站式购物需求。,北京航天总医院、天坛医院（新址）、北京同仁医院（南区）、旧宫医院、亦庄医院、大兴中西医结合医院,暂无资料,暂无资料,项目紧邻北京最大的湿地公园---南海子公园。作为北京四大郊野公园之一，具有600余年的历史。是辽、金、元、明、清五朝皇家猎场，另外项目周边更拥有14座生态公园，具有非常高的生态价值，也被誉为公园地产。敬请关注。'),
('04a7ee9906177471287182d2bf79274e','2021-07-22','2021Q3','合生me悦预计2021年下半年开盘','合生me悦预计2021年下半年开盘。价格待定。主力户型为52平-120平1居-4居。2024年整体交房。容积率2.34。合生me悦项目为小高层,高层板楼。共1436户。绿化率30%。带装修交房。敬请关注。'),
('04a7ee9906177471287182d2bf79274e','2021-07-23','2021Q3','合生me悦项目待售','合生me悦地处五环旁，位于北京南中轴，是合生创展精品悦系的首个作品。对话城市新生代群体，合生me悦以契合国际潮流的全新理念，打造建筑面积约55-120㎡精智美宅，焕新潮流人居作品，为京城再造新浪潮。'),
('04a7ee9906177471287182d2bf79274e','2021-07-26','2021Q3','合生me悦均价63000元/平在售','合生me悦项目售楼处位于北京南五环德茂站南海子公园南门200米，项目已于7月25日开盘，在售建筑面积约55-120㎡精智美宅，均价63000元/平'),
('04a7ee9906177471287182d2bf79274e','2021-08-10','2021Q3','合生me悦均价为：63000元/平方米','合生me悦63000元/平方米。新获得预售许可证：京房售证字（2021）开8号。绿化率30%。在售户型有：52平-120平1居-4居。共1436户。'),
('04a7ee9906177471287182d2bf79274e','2021-08-13','2021Q3','合生me悦项目55-120平米在售','合生me悦项目附近相邻住总万科广场、山姆会员店、大族广场、力宝广场、爱琴海购物公园、航天万源广场，除此之外，西红门商圈的荟聚购物中心、宜家家居'),
('04a7ee9906177471287182d2bf79274e','2021-08-18','2021Q3','合生me悦项目均价63000元/平在售','合生me悦项目毗邻地铁，多轨出行 即距地铁8号线德茂站约200米，11站到前门。此外13条地铁线接驳相连，瞬时直达北京各核心区，项目在售建筑面积约55-120㎡精智美宅，均价63000元/平。'),
('04a7ee9906177471287182d2bf79274e','2021-08-24','2021Q3','合生me悦开盘28分钟售完户型，8月底即将再次加推','合生me悦项目8月底将加力推出：55—120平的1-4居精智美宅，火爆预售季，席次递减中，最后抢购别错过！'),
('04a7ee9906177471287182d2bf79274e','2021-09-08','2021Q3','合生me悦均价为：63000元/平方米','合生me悦63000元/平方米。项目规划建设1436户。2021年7月25日开盘'),
('04a7ee9906177471287182d2bf79274e','2021-09-18','2021Q3','合生me悦项目新品加推在即','合生me悦中秋献礼，新品加推在即，建筑面积约72㎡两室两厅一卫爆款王、95㎡三室两厅两卫神户型、120㎡四室两厅两卫四叶草户型，三大经典户型，限量发售'),
('04a7ee9906177471287182d2bf79274e','2021-09-30','2021Q3','合生me悦项目均价63000元/平在售','合生me悦项目售楼处位于北京南五环德茂站南海子公园南门200米，项目建筑面积约72㎡两室两厅一卫爆款王、95㎡三室两厅两卫神户型、120㎡四室两厅两卫四叶草户型，三大经典户型，限量发售。')
;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1012
## 七城样例数据查找
select city_id,city_name from dwb_db.dwb_dim_geography_55city where city_name in ('上海市','北京市','广州市','深圳市','南京市','武汉市','重庆市') group by city_id,city_name;

select newest_id from dws_db_prd.dws_newest_issue_code where issue_code like '%2021%' group by newest_id ;


select t1.newest_id,t1.newest_name,t2.intention,t1.city_id,t1.unit_price,t3.layout_area,t4.city_name,t5.issue_code from 
  (select * from dws_db_prd.dws_newest_info where 
    city_id in (select city_id from dwb_db.dwb_dim_geography_55city where city_name in ('上海市','北京市','广州市','深圳市','南京市','武汉市','重庆市') group by city_id,city_name)
    and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 and period = '2021Q2' and browse in ('A') and portrait in ('C') group by newest_id)
    and building_area is not null and unit_price is not null
    ) t1
inner join
  (select newest_id,intention from dwb_db.dwb_newest_customer_info where period = `quarter` and intention > 100 group by newest_id,intention) t2 
on t1.newest_id = t2.newest_id
inner join 
  (select newest_id,max(layout_area) layout_area from dws_db_prd.dws_newest_layout group by newest_id) t3
on t1.newest_id = t3.newest_id
inner join 
  (select city_id,city_name from dwb_db.dwb_dim_geography_55city where city_name in ('上海市','北京市','广州市','深圳市','南京市','武汉市','重庆市') group by city_id,city_name) t4
on t1.city_id = t4.city_id
inner join 
  (select newest_id,max(issue_code) issue_code from dws_db_prd.dws_newest_issue_code group by newest_id) t5
on t1.newest_id = t5.newest_id
;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1012
## 预售证数据入库
select * from temp_db.city_newest_deal_data_check where city_name = '常州' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '常州' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '常州' group by open_date ;

select issue_date,str_to_date(issue_date ,'%Y-%m-%d %H:%i:%s') a from temp_db.city_newest_deal_data_check where city_name = '常州' group by issue_date;

select issue_date,length(issue_date) from temp_db.city_newest_deal_data_check where city_name = '常州' group by issue_date;

select issue_date,
       case when length(issue_date)=17 then str_to_date(issue_date ,'%Y-%m-%d')
            when length(issue_date)=16 then str_to_date(replace(issue_date,right(issue_date,8),''),'%Y-%m-%d')
            when length(issue_date)=15 then str_to_date(replace(issue_date,right(issue_date,7),''),'%Y-%m-%d')
            else '9999-09-09' end b
from temp_db.city_newest_deal_data_check where city_name = '宝鸡' group by issue_date;


## 查看迫切数量和定向人数的比例

select intention a,count(1) from dws_db_prd.dws_imei_browse_tag where period = '2021Q3' group by intention 
union all 
select urgent a,count(1) from dws_db_prd.dws_imei_browse_tag where period = '2021Q3' group by urgent ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1011  帮忙跑数据
select city_name,newest_name,newest_id from dwb_db.a_dwb_customer_browse_log t1
join dws_db_prd.dim_geography t2
on t1.city_id=t2.city_id
where newest_id is not null and t1.visit_month >= 202107 and t1.visit_month <=202109
group by city_name,newest_name,newest_id;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1011 新浏览日志表数据迁移
select * from dwb_db.a_dwb_customer_browse_log_1011

show create table dwb_db.a_dwb_customer_browse_log_0831 ;
CREATE TABLE dwb_db.a_dwb_customer_browse_log_1011 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `ori_id` int(11) DEFAULT NULL COMMENT '源数据ID',
  `ori_table` varchar(50) DEFAULT NULL COMMENT '源数据表',
  `imei` varchar(14) DEFAULT NULL COMMENT '解密后的IMEI',
  `city_id` varchar(20) DEFAULT NULL COMMENT '城市ID',
  `county_id` varchar(20) DEFAULT NULL COMMENT '区/县ID',
  `newest_name` varchar(50) DEFAULT NULL COMMENT '数塔楼盘名',
  `visit_month` varchar(6) DEFAULT NULL COMMENT '浏览月份',
  `visit_date` date DEFAULT NULL COMMENT '浏览日期',
  `pv` int(11) DEFAULT '0' COMMENT '浏览次数',
  `source` varchar(255) DEFAULT NULL COMMENT '来源，安卓or苹果',
  `idate` varchar(20) DEFAULT NULL,
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id,城市+数塔楼盘名与别名表关联获取楼盘id(uuid)',
  `create_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_user` varchar(45) DEFAULT NULL COMMENT '创建人',
  `update_date` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_user` varchar(45) DEFAULT NULL COMMENT '更新人',
  `current_week` varchar(20) DEFAULT NULL COMMENT '当前周数(period)',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='客户浏览楼盘日志表Q3季度';

truncate table dwb_db.a_dwb_customer_browse_log_1011;
insert into dwb_db.a_dwb_customer_browse_log_1011 select * from dwb_db.a_dwb_customer_browse_log ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1011  预售证数据入库
select * from temp_db.city_newest_deal_data_check where city_name = '宝鸡' limit 10;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '宝鸡' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '宝鸡' group by open_date ;

select issue_date,str_to_date(issue_date ,'%Y-%m-%d %H:%i:%s') a from temp_db.city_newest_deal_data_check where city_name = '宝鸡' group by issue_date;


select issue_date from odsdb.city_newest_deal where city_name = '唐山' group by issue_date ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 1011  预售证数据入库，mysql安装

select * from odsdb.city_newest_deal where city_name = '成都' and open_date like '%00:00%';

CREATE database dws;
CREATE TABLE dws.dws_tag_purchase_poi (id int(1) );
insert into dws.dws_tag_purchase_poi(id) values(1),(2);

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1009  预售证数据入库
select * from temp_db.city_newest_deal_data_check where city_name = '成都' limit 10;

select substr(open_date,6,5) from temp_db.city_newest_deal_data_check where city_name = '上海' group by substr(open_date,6,5) ;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '成都' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '成都' group by open_date ;

select * from temp_db.city_newest_deal_data_check limit 10;

select city_id,city_name,city_level_desc from dwb_db.dwb_dim_geography_55city group by city_id,city_name,city_level_desc ; 

select * from odsdb.city_newest_deal where city_name = '中山';

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1009  新添poi数据
update odsdb.ori_newest_period_admit_info set dr = 1 where dr = 0;

show create table dws_db_prd.dws_tag_purchase_poi;
CREATE TABLE dws_db_prd.bak_20211009_dws_tag_purchase_poi (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(255) NOT NULL COMMENT '城市名称',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼盘id',
  `tag_value` varchar(255) NOT NULL COMMENT '三级标签值',
  `tag_detail` varchar(255) NOT NULL COMMENT '配套详情',
  `pure_distance` varchar(255) NOT NULL COMMENT '直线距离',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `tag_value2` varchar(255) NOT NULL COMMENT '二级标签值',
  PRIMARY KEY (`id`),
  KEY `ind_lng_lat` (`city_id`,`tag_detail`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=51511617 DEFAULT CHARSET=utf8mb4 COMMENT='配套信息表--20211009备份';

insert into dws_db_prd.bak_20211009_dws_tag_purchase_poi select * from dws_db_prd.dws_tag_purchase_poi;

delete from dws_db_prd.dws_tag_purchase_poi where newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10)='2021-09-30' or (substr(create_time,1,10)='2021-09-29' and city_name=gd_city and county_name=gd_district) group by newest_id) ;

select * from dws_db_prd.dws_tag_purchase_poi where newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10)='2021-09-30' or (substr(create_time,1,10)='2021-09-29' and city_name=gd_city and county_name=gd_district) group by newest_id) limit 10;

select concat(substring_index(lnglat,',',1)+0,',',substring_index(lnglat,',',-1)+0) from odsdb.ori_newest_period_admit_info where dr = 0 limit 10;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 1008  重跑榜单数据
select city_id,newest_id,sort_id,period from dws_db_prd.dws_newest_popularity_rownumber_quarter 
  where city_id in (select city_id from dws_db_prd.dws_newest_popularity_rownumber_quarter where newest_id in (select old_id from dws_db.relation_newest group by old_id) and dr= 0 group by city_id)
and period in (select period from dws_db_prd.dws_newest_popularity_rownumber_quarter where newest_id in (select old_id from dws_db.relation_newest group by old_id) group by period) ;

select city_id from dws_db_prd.dws_newest_popularity_rownumber_quarter where dr = 0 and newest_id in (select old_id from dws_db.relation_newest group by old_id) group by city_id;

select city_id from dws_db_prd.dws_newest_popularity_rownumber_quarter where newest_id in (select old_id from dws_db.relation_newest group by old_id) and right(city_id,3) = '000' group by city_id;

update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set dr = 1 where city_id in ('110000','110111','110116','110119','120000','120111','120112','120115','120119','130100','130102','130108','130200','130202','130203','130207','130281','210100','210102','210104','210113','210114','310000','310101','310104','310105','310109','310112','310114','310115','310117','310118','310151','320100','320106','320200','320205','320214','320281','320282','320300','320305','320312','320322','320400','320411','320412','320413','320500','320506','320508','320509','320581','320582','320583','320600','320612','320681','330100','330105','330109','330110','330112','330122','330200','330213','330400','330402','330421','330424','330482','330483','330600','330603','340100','340111','340124','350100','350128','350200','350213','360100','360111','370100','370102','370105','370112','370113','370200','370203','370283','370600','370611','370800','370811','370883','410100','410104','410122','410184','420100','420102','420103','420111','420114','420115','440100','440104','440105','440106','440111','440112','440113','440114','440115','440118','440300','440303','440304','440305','440306','440307','440309','440310','440400','440402','440600','440604','440606','440607','441200','441202','441300','441302','441303','441322','441900','450100','450103','450108','460100','460106','500000','500105','500156','510100','510117','520100','520103','520181','530100','530102','530103','530114','610100','610104','610112','610113','610114','610116','610122','610124','610300','610302','610400','610402','610404','610423','610425','610428','610481');

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where city_id in ('110000','110111','110116','110119','120000','120111','120112','120115','120119','130100','130102','130108','130200','130202','130203','130207','130281','210100','210102','210104','210113','210114','310000','310101','310104','310105','310109','310112','310114','310115','310117','310118','310151','320100','320106','320200','320205','320214','320281','320282','320300','320305','320312','320322','320400','320411','320412','320413','320500','320506','320508','320509','320581','320582','320583','320600','320612','320681','330100','330105','330109','330110','330112','330122','330200','330213','330400','330402','330421','330424','330482','330483','330600','330603','340100','340111','340124','350100','350128','350200','350213','360100','360111','370100','370102','370105','370112','370113','370200','370203','370283','370600','370611','370800','370811','370883','410100','410104','410122','410184','420100','420102','420103','420111','420114','420115','440100','440104','440105','440106','440111','440112','440113','440114','440115','440118','440300','440303','440304','440305','440306','440307','440309','440310','440400','440402','440600','440604','440606','440607','441200','441202','441300','441302','441303','441322','441900','450100','450103','450108','460100','460106','500000','500105','500156','510100','510117','520100','520103','520181','530100','530102','530103','530114','610100','610104','610112','610113','610114','610116','610122','610124','610300','610302','610400','610402','610404','610423','610425','610428','610481');

select * from dws_db_prd.dws_newest_popularity_rownumber_quarter where city_id = '610425' and period = '2019Q2' and dr = 1;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
