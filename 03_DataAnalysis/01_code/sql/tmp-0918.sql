




--------------------------------------------- =========================== ---------------------------------------------------



select cityid, from dws_db_prd.dws_supply where quarter = '2020Q3' and period != quarter ;

select * from dws_db_prd.dws_supply where period = '2020Q3';


delete from dws_db_prd.dws_supply where period in ('2020Q3','2020Q4','2021Q1','2021Q2');

delete from dwb_db.dwb_issue_supply_county where quarter in ('2020Q4','2021Q1','2021Q2') and period != quarter ;

update dwb_db.dwb_issue_supply_county set supply_value = 0 where supply_value = '-';



insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) 
select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,'2021Q2' quarter from
dwb_db.dwb_issue_supply_county where period = '2021Q2' and dr = 0 
union all 
select  city_name, city_name county_name, city_id city_id, period, sum(supply_value) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'2021Q2' quarter from dwb_db.dwb_issue_supply_county  where period = '2021Q2' and dr = 0  group by city_name,city_id,period;


insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,quarter from dwb_db.dwb_issue_supply_county where dr=0 and quarter in ('2020Q3','2020Q4','2021Q1','2021Q2') and period != quarter ;

insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value) 
select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value from
dwb_db.dwb_issue_supply_county where period = '2020Q4' and dr = 0 
union all 
select  city_name, city_name county_name, city_id city_id, period, sum(supply_value) value from dwb_db.dwb_issue_supply_county 
where period = '2020Q4' and dr = 0  group by city_name,city_id,period;

 
select * from dws_db.dws_supply ds where period != '2020Q3' and city_name != county_name ;






--------------------------------------------- =========================== ---------------------------------------------------


-- 供应套数的月份数据
-- 2021取消
-- 2020Q4是34城

insert into dwb_db.dwb_issue_supply_county(
    city_id,city_name,county_id,county_name,intention,period,quarter,intention_city,rate,supply_num,supply_value,cric_supply_num,num_index,dr,create_time,update_time) 
select t2.city_id,t2.city_name,t2.county_id,t2.county_name,t1.intention,t2.period,t2.quarter,t2.intention_city,t2.rate,t2.supply_num,t2.supply_value,t2.cric_supply_num,t2.num_index,t2.dr,t2.create_time,t2.update_time from 
  (select city_id,county_id,intention,period from dwb_db.dwb_newest_county_customer_num 
    where period != quarter and quarter = '2021Q2' and dr=0 and city_name in ('北京市','上海市','广州市','深圳市')) t1
inner join 
  (select cityid city_id,city_name,city_id county_id,county_name,case when follow_people_num = '-' then 0 else follow_people_num end intention,period,quarter,0 intention_city,null rate,null supply_num, value supply_value,cric_value cric_supply_num,null num_index,dr,create_time,update_time
from dws_db.dws_supply where dr = 0 and quarter = '2021Q2' and quarter != period and city_name != county_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.county_id and t1.period = t2.period;


insert into dwb_db.dwb_issue_supply_county(
    city_id,city_name,county_id,county_name,intention,period,quarter,intention_city,rate,supply_num,supply_value,cric_supply_num,num_index,dr,create_time,update_time) 
select cityid city_id,city_name,city_id county_id,county_name,case when follow_people_num = '-' then 0 else follow_people_num end intention,period,quarter,0 intention_city,null rate,null supply_num, value supply_value,cric_value cric_supply_num,null num_index,dr,create_time,update_time
from dws_db.dws_supply where dr = 0 and quarter = '2021Q2' and quarter != period and city_name != county_name;

insert into dwb_db.dwb_issue_supply_county(
    city_id,city_name,county_id,county_name,intention,period,quarter,intention_city,rate,supply_num,supply_value,cric_supply_num,num_index,dr,create_time,update_time) 
select t2.city_id,t2.city_name,t2.county_id,t2.county_name,t1.intention,t2.period,t2.quarter,t2.intention_city,t2.rate,t2.supply_num,t2.supply_value,t2.cric_supply_num,t2.num_index,t2.dr,t2.create_time,t2.update_time from 
  (select city_id,county_id,intention,period from dwb_db.dwb_newest_county_customer_num 
    where period = quarter and quarter = '2021Q2' and dr=0 and city_name in ('北京市','上海市','广州市','深圳市')) t1
inner join 
  (select cityid city_id,city_name,city_id county_id,county_name,case when follow_people_num = '-' then 0 else follow_people_num end intention,period,quarter,0 intention_city,null rate,null supply_num, value supply_value,cric_value cric_supply_num,null num_index,dr,create_time,update_time
from dws_db.dws_supply where dr = 0 and quarter = '2021Q2' and quarter != period and city_name != county_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.county_id and t1.period = t2.period;





delete from dwb_db.dwb_issue_supply_county where dr=0 and city_name in ('北京市','上海市','广州市','深圳市') and  quarter in ('2020Q3') and quarter != period;


select * from dws_db.dws_supply;
select * from dwb_db.dwb_issue_supply_county;



insert into dwb_db.dwb_issue_supply_county(
    city_id,city_name,county_id,county_name,intention,period,quarter,intention_city,rate,supply_num,supply_value,cric_supply_num,num_index,dr,create_time,update_time) 
  select cityid city_id,city_name,city_id county_id,county_name,case when follow_people_num = '-' then 0 else follow_people_num end intention,period,quarter,0 intention_city,null rate,null supply_num, value supply_value,cric_value cric_supply_num,null num_index,dr,create_time,update_time
  from dws_db.dws_supply where dr = 0 and period = '2020Q4'; 


select cityid,city_name,city_id,county_name,follow_people_num,period,quarter,null,null,null, value,cric_value,null,dr,create_time,update_time
  from dws_db.dws_supply where dr = 0 and period = '2020Q4'
left join 
select * from dwb_db.dwb_newest_county_customer_num where period = '2020Q4'; 





truncate table dws_db.dws_supply ;
insert into dws_db.dws_supply select * from temp_db.dws_supply ;










update dwb_db.dwb_issue_supply_city set supply_num = cric_supply_num where num_index != '-';

select city_id,city_name,county_id,county_name from dwb_db.dwb_newest_county_customer_num where period = '2020Q4' group by city_id,city_name,county_id,county_name
;

select cityid city_id,city_name,city_id county_id,county_name from dws_db.dws_supply where period = '2020Q4' group by cityid,city_name,city_id,county_name;


select * from 
    (select city_id,city_name,county_id,county_name from dwb_db.dwb_newest_county_customer_num where period = '2020Q4' group by city_id,city_name,county_id,county_name) t1
right join 
    (select cityid city_id,city_name,city_id county_id,county_name from dws_db.dws_supply where period = '2020Q4' group by cityid,city_name,city_id,county_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.county_id where t2.city_id is not null;

select city_id,city_name,county_id,county_name from dwb_db.dwb_newest_county_customer_num where period = '2020Q4' and city_name = '惠州市'
group by city_id,city_name,county_id,county_name;

select cityid city_id,city_name,city_id county_id,county_name from dws_db.dws_supply where period = '2020Q4' and city_name = '惠州市' 
group by cityid,city_name,city_id,county_name;



select * from 
    (select city_id,city_name,county_id,county_name from dwb_db.dwb_newest_county_customer_num where period = '2021Q1' group by city_id,city_name,county_id,county_name) t1
left join 
    (select cityid city_id,city_name,city_id county_id,county_name from dws_db.dws_supply where period = '2021Q1' group by cityid,city_name,city_id,county_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.county_id where t2.city_id is not null;


select * from 
    (select city_id,city_name,county_id,county_name from dwb_db.dwb_newest_county_customer_num where period = '2021Q2' group by city_id,city_name,county_id,county_name) t1
left join 
    (select cityid city_id,city_name,city_id county_id,county_name from dws_db.dws_supply where period = '2021Q2' group by cityid,city_name,city_id,county_name) t2
  on t1.city_id = t2.city_id and t1.county_id = t2.county_id 
where t2.city_id is not null;




truncate table dwb_db.dwb_newest_city_customer_num ;
truncate table dwb_db.dwb_newest_county_customer_num ;
truncate table dwb_db.dwb_issue_supply_county ;
truncate table dws_db_prd.dws_supply;
truncate table dws_db_prd.dws_customer_week; 


-- 320500	苏州市
-- 310000	上海市

select * from dwb_db.a_dwb_newest_info adni where city_id = '320500' and city_name = '苏州市';
 

--------------------------------------------- =========================== ---------------------------------------------------

-- 查看Q4，1,2的供应套数差别。
--        补充3个季度的城市套数
--                对比本地数据和cric系统的数据的差距，来估算有哪些城市需要从cric系统上拿出数据的
--                     首先现在supply_city表中记录cric系统的数据，然后设置dr=1，新添数据到表中
--        查看是否有区域的增删
--                有： 重新跑区县的供应套数。  
--                没有： 直接拿取区县的供应套数过来
--                北京，上海，广州，深圳的月度数据需要先看区县是否有改变，如果改变了，月度数据也需要重新算

-- 查看现在的关注人数的区域有咩有变化
-- 有变化的重跑有变化的季度或者城市


-- 根据3季度的城市数据来推测城市数据的准确性，决定用哪一方的数据


-- 供应套数的月份数据


select a.*,b.*,c.intention from 
    (select city_name,city_id,period,value,cric_value,value_from_index from dws_db.dws_supply where dr = 0 and city_county_index = 1 and period_index = 1) a
right join 
    (select city_name,city_id,period,supply_num,cric_supply_num,num_index from dwb_db.dwb_issue_supply_city where dr = 0 
--     and period >= '2020Q4'
    ) b
  on a.city_name = b.city_name and a.city_id = b.city_id and a.period = b.period
left join 
  (select city_id,city_name,intention,period from dwb_db.dwb_newest_city_customer_num where dr = 0) c
  on c.city_name = b.city_name and c.city_id = b.city_id and c.period = b.period
where b.city_name = '扬州市';

select * from dwb_db.dwb_issue_supply_city disc where dr = 0 and city_name = '扬州市';

update dwb_db.dwb_issue_supply_city set supply_num = cric_supply_num where num_index != '-';









--------------------------------------------- =========================== ---------------------------------------------------

select cal_date,period from dws_db_prd.dim_period_date dpd where  cal_date >= '2018-01-01' and cal_date <'2018-03-31';


truncate table dwb_db.dwb_customer_add_new_code ;

delete from dwb_db.dwb_customer_add_new_code where substr(visit_quarter,1,4) = '2020' ;


select imei,city_id,visit_month,count(1) from dwb_db.a_dwb_customer_browse_log adcbl where visit_date in ('2018-03-31','2018-04-01') group by imei,city_id,visit_month ;


select imei,city_id,count(1) from dwb_db.dwb_customer_add_new_code dcanc where visit_week = '2018-13' group by imei,city_id 


--------------------------------------------- =========================== ---------------------------------------------------







-- 判断一下有影响和没有影响的表区分并记录
   -- 无影响
      -- （dws_newest_provide_sche，dwb_customer_add_new_code，dwb_newest_customer_info，dwb_dim_geography_55city，dwb_newest_issue_offer，dwb_issue_supply_city）
   -- 有影响
      -- （dwb_newest_city_customer_num，dwb_newest_county_customer_num，dws_supply，dwb_issue_supply_county，
      -- dws_newest_popularity_rownumber_quarter，dws_newest_investment_pop_rownumber_quarter，dws_newest_issue_code，dws_newest_offer_rate，dws_customer_cre
      -- dws_customer_month，dws_customer_sum，dws_customer_week）
truncate table dws_db.dws_customer_cre;
insert into dws_db.dws_customer_cre select * from temp_db.dws_customer_cre;
truncate table dws_db.dws_customer_month;
insert into dws_db.dws_customer_month select * from temp_db.dws_customer_month;
truncate table dws_db.dws_customer_sum;
insert into dws_db.dws_customer_sum select * from temp_db.dws_customer_sum;
truncate table dws_db.dws_customer_week;
insert into dws_db.dws_customer_week select * from temp_db.dws_customer_week;
truncate table dws_db.dws_newest_investment_pop_rownumber_quarter;
insert into dws_db.dws_newest_investment_pop_rownumber_quarter select * from temp_db.dws_newest_investment_pop_rownumber_quarter;
truncate table dws_db.dws_newest_popularity_rownumber_quarter;
insert into dws_db.dws_newest_popularity_rownumber_quarter select * from temp_db.dws_newest_popularity_rownumber_quarter;
truncate table dws_db.dws_newest_provide_sche;
insert into dws_db.dws_newest_provide_sche select * from temp_db.dws_newest_provide_sche;
truncate table dws_db.dws_supply;
insert into dws_db.dws_supply select * from temp_db.dws_supply;
truncate table dws_db.dws_newest_issue_code;
insert into dws_db.dws_newest_issue_code select * from temp_db.dws_newest_issue_code;
truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter;
truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter;
truncate table dws_db_prd.dws_customer_month; 
truncate table dws_db_prd.dws_customer_sum;
truncate table dws_db_prd.dws_customer_cre; 
truncate table dws_db_prd.dws_customer_week; 


insert into dws_db.dws_customer_cre(city_id,newest_id,exist,imei_num,period)
select b.city_id,a.* from 
  (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter 
  union all 
  select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter) a
left join 
  dwb_db.a_dwb_newest_info b on a.newest_id=b.newest_id 

select * from dwb_db.a_dws_newest_period_admit limit 10;




-- 先把没有影响的表迁移到新库
-- 再把有影响的表重新泡一下

truncate table dws_db_prd.dws_newest_provide_sche ;
insert into dws_db_prd.dws_newest_provide_sche select * from dws_db.dws_newest_provide_sche ;

truncate table dwb_db.dwb_newest_customer_info ;
truncate table dwb_db.dwb_newest_city_customer_num ;
truncate table dwb_db.dwb_newest_county_customer_num ;
truncate table dwb_db.dwb_issue_supply_county ;
truncate table dws_db_prd.dws_supply;

select * from dws_db_prd.dws_supply;



insert into dws_db_prd.dws_newest_issue_code (city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time)
select b.city_id,b.city_name,b.region_id county_id,b.region_name county_name,b.uuid newest_id,b.newest_name,b.address,b.developer business,a.issue_number issue_code,a.issue_date,
       a.issue_quarter,a.issue_month,a.issue_area,a.room_sum supply_num,a.housing_id,0 dr ,now() create_time ,now() update_time from 
  (select housing_id,issue_number,issue_date,case when issue_date is not null then date_format(issue_date,'%Y%m') else issue_date end issue_month,case when issue_date is not null then concat(substr(issue_date,1,4) ,'Q',QUARTER(issue_date)) else issue_date end  issue_quarter,room_sum,issue_area from dwb_db.dim_housing_issue where is_del = 0) a
left join 
  (select t1.*,t2.region_name from  (select id,uuid,newest_name,city_id,city_name,region_id,address,developer from dwb_db.dim_housing) t1 left join (select city_id ,region_id ,region_name  from dws_db.dim_geography dg where grade = 4 group by city_id ,region_id ,region_name ) t2 on t1.city_id = t2.city_id and t1.region_id=t2.region_id) b
on a.housing_id = b.id ;

show create table dws_db.dws_newest_issue_code ;
CREATE TABLE dws_db_prd.dws_newest_issue_code (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` int(11) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `county_id` int(11) DEFAULT NULL COMMENT '区县id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区县名称',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(100) DEFAULT NULL COMMENT '楼盘名称',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `business` varchar(255) DEFAULT NULL COMMENT '开发商',
  `issue_code` varchar(200) DEFAULT NULL COMMENT '预售证号',
  `issue_date` date DEFAULT NULL COMMENT '预售证发证时间',
  `issue_quarter` varchar(10) DEFAULT NULL COMMENT '发证季度',
  `issue_month` varchar(10) DEFAULT NULL COMMENT '发证月份',
  `issue_area` int(11) DEFAULT NULL COMMENT '预售许可面积',
  `supply_num` int(11) DEFAULT NULL COMMENT '预售证供应套数',
  `housing_id` int(11) DEFAULT NULL COMMENT '源表id',
  `dr` int(2) DEFAULT '0' COMMENT '有效标识 1 无效  0有效',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY ```idx_newest_issue_code_newest_id``` (`newest_id`) USING BTREE,
  KEY ```idx_newest_issue_code_issue_code``` (`issue_code`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=327676 DEFAULT CHARSET=utf8mb4 COMMENT='预征信息表';



update dws_db.dws_imei_browse_tag set cre = '增长' where cre='0';

insert
	into
	dws_db.dws_supply(city_name,
	county_name,
	city_id,
	period,
	value,
	local_issue_value,
	local_room_sum_value,
	cric_value,
	value_from_index,
	county_name_merge,
	city_county_index,
	period_index,
	update_time,
	dr,
	create_time,
	follow_people_num,
	cityid,
	`quarter`)
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
	'2018Q1' `quarter`
from
	dwb_db.dwb_issue_supply_county
where
	period = '2018Q1'
	and dr = 0
union all
select
	city_name,
	city_name county_name,
	city_id city_id,
	period,
	sum(supply_value) value,
	null local_issue_value ,
	null local_room_sum_value,
	null cric_value,
	null value_from_index ,
	null county_name_merge,
	'1' city_county_index,
	'1' period_index,
	now() update_time,
	0 dr,
	now() create_time,
	null follow_people_num,
	city_id cityid,
	'2018Q1' `quarter`
from
	dwb_db.dwb_issue_supply_county
where
	period = '2018Q1'
	and dr = 0
group by
	city_name,
	city_id,
	period;


--------------------------------------------- =========================== ---------------------------------------------------



update dws_db.dws_newest_info set lng = '113.348286' where newest_id = 'e5a64a044ac8e17ff9a7911f71c0ced9';
update dws_db.dws_newest_info set lat = '22.1518030' where newest_id = 'e5a64a044ac8e17ff9a7911f71c0ced9';

update dws_db.dws_newest_info set lng = '120.671921' where newest_id = 'd2e8b55aff9edda3f2b2965f362dc6ea';
update dws_db.dws_newest_info set lat = '27.994414' where newest_id = 'd2e8b55aff9edda3f2b2965f362dc6ea';

update dws_db.dws_newest_info set lng = '120.4907620' where newest_id = '5a96732b652b481e499dace5d8c9bf86';
update dws_db.dws_newest_info set lat = '36.3803690' where newest_id = '5a96732b652b481e499dace5d8c9bf86';

update dws_db.dws_newest_info a,dwb_db.dwb_newest_info b set a.lat=b.lat where a.newest_id = b.newest_id and a.lng < '40';
update dws_db.dws_newest_info a,dwb_db.dwb_newest_info b set a.lng=b.lng where a.newest_id = b.newest_id and a.lng < '40';


select * from 
  (select newest_id,lng,lat from dws_db.dws_newest_info) a
left join 
  (select newest_id,lng,lat from dwb_db.dwb_newest_info) b
on a.newest_id = b.newest_id where a.lng != b.lng ;


select city_name,county_name,city_id,period,value,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter from dws_supply ds ;

insert into dws_db.dws_supply (city_name,county_name,city_id,period,value,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter)
values('杭州市','钱塘区','330114','2021Q2','4536','2','1','2021-07-27 09:27:13.0','0','2021-07-27 16:52:07.0','1758','330100','2021Q2') ;

update dws_db.dws_supply set value = '2762' where city_name = '杭州市' and quarter = '2021Q2' and dr = 0 and city_id = '330104';

select * from temp_db.dws_supply ds where city_name = '杭州市' and quarter = '2021Q2' and dr = 0;

--------------------------------------------- =========================== ---------------------------------------------------



-- dws_supply 
-- dws_newest_popularity_rownumber_quarter 
-- dws_newest_investment_pop_rownumber_quarter 
-- dws_ 
-- dws_customer_month 
-- dws_customer_week 
-- dws_customer_sum 
-- dws_newest_provide_sche 
-- dws_newest_issue_code 

show create table dws_db.dws_newest_provide_sche ;
CREATE TABLE temp_db.dws_newest_provide_sche (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `newest_id` varchar(200) DEFAULT NULL COMMENT '楼盘id',
  `date` date DEFAULT NULL COMMENT '日期',
  `period` varchar(255) DEFAULT NULL COMMENT '时间周期',
  `provide_title` varchar(500) DEFAULT NULL COMMENT '动态标题',
  `provide_sche` text COMMENT '动态正文',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=883993 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态表';



insert into temp_db.dws_newest_issue_code(id,city_id,period,newest_id,dr,gd_city,floor_name,newest_name,address,business,issue_code,issue_date,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,update_time,from_code,county_id) select * from dws_db.dws_newest_issue_code_copy1 ;




--------------------------------------------- =========================== ---------------------------------------------------


select city_id ,city_name,county_id,county_name from dwb_db.a_dwb_newest_info adni where city_name is not null and county_id is not null and county_name is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') group by  city_id ,city_name,county_id,county_name;

-- 所有涉及到城市名称和区县名称的都要重新跑
-- 所有涉及到housing表的都要关联到newest_info来获取id
-- 备份housing和newest_info
-- 根据id更新newest中的名字
-- 根据uuid更新housing中的id和名字


-- 更新newest_info表城市区县名称
-- 更新housing表城市区县id和城市名称


select city_id,city_name,county_id,county_name from dwb_db.a_dwb_newest_info;

select uuid,city_id,region_id county_id,city_name from dwb_db.dim_housing;


update dwb_db.a_dwb_newest_info a , (select city_id,city_name,region_id county_id ,region_name county_name from dws_db.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) b set a.city_name=b.city_name where a.city_id = b.city_id and a.county_id = b.county_id;

update dwb_db.a_dwb_newest_info a , (select city_id,city_name,region_id county_id ,region_name county_name from dws_db.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) b set a.county_name=b.county_name where a.city_id = b.city_id and a.county_id = b.county_id;

update dwb_db.a_dwb_newest_info set city_name = '中山市' where city_id = '442000';

update dwb_db.dim_housing a,dwb_db.a_dwb_newest_info b set (a.city_id,region_id,a.city_name)=(b.city_id,b.county_id,b.city_name) where a.uuid = b.newest_id ;

update dwb_db.dim_housing a,dwb_db.a_dwb_newest_info b set a.city_id=b.city_id where a.uuid = b.newest_id ;

update dwb_db.dim_housing a,dwb_db.a_dwb_newest_info b set a.region_id=b.county_id where a.uuid = b.newest_id ;

update dwb_db.dim_housing a,dwb_db.a_dwb_newest_info b set a.city_name=b.city_name where a.uuid = b.newest_id ;





-- 数据


























--------------------------------------------- =========================== ---------------------------------------------------


-- 查看现在的关注人数的区域有咩有变化
-- 有变化的重跑有变化的季度或者城市

-- 无变化的话供需比加工



select a.*,b.follow_people_num,b.value,b.cric_value from 
 (select city_id,city_name,county_id,county_name,intention,period from dwb_db.dwb_newest_county_customer_num where period != quarter and quarter = '2020Q4') a
left join 
 (select cityid city_id ,city_name,city_id county_id ,county_name,value,follow_people_num,period,cric_value from dws_db.dws_supply where period_index =2 and city_county_index =2 and dr =0 and quarter = '2020Q4') b
on a.city_id=b.city_id and a.city_name=b.city_name and a.county_id=b.county_id and a.county_name=b.county_name and a.period=b.period
where a.city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400')
-- and a.city_name in ('北京市','上海市','广州市','深圳市')
; 

select city_id from dwb_db.dwb_dim_geography_55city ddgc where dr=0 group by city_id;

--------------------------------------------- =========================== ---------------------------------------------------








-- 备份
-- 迁移
-- 建表
-- 预售证sql
show create table dws_db.dws_newest_issue_code;
drop table dws_db.dws_newest_issue_code;


CREATE TABLE dws_db.dws_newest_issue_code (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` int(11) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `county_id` int(11) NOT NULL COMMENT '区县id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区县名称',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(100) DEFAULT NULL COMMENT '楼盘名称',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `business` varchar(60) DEFAULT NULL COMMENT '开发商',
  `issue_code` varchar(200) DEFAULT NULL COMMENT '预售证号',
  `issue_date` date DEFAULT NULL COMMENT '预售证发证时间',
  `issue_quarter` varchar(10) DEFAULT NULL COMMENT '发证季度',
  `issue_month` varchar(10) DEFAULT NULL COMMENT '发证月份',
  `issue_area` int(11) DEFAULT NULL COMMENT '预售许可面积',
  `supply_num` int(11) DEFAULT NULL COMMENT '预售证供应套数',
  `housing_id` int(11) DEFAULT NULL COMMENT '源表id',
  `dr` int(2) DEFAULT 0 COMMENT '有效标识 1 无效  0有效',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY ```idx_newest_issue_code_newest_id``` (`newest_id`) USING BTREE,
  KEY ```idx_newest_issue_code_issue_code``` (`issue_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='预征信息表';

drop table dwb_db.dim_housing;
show create table dw_a.dim_housing ;
CREATE TABLE dwb_db.dim_housing (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `uuid` varchar(32) DEFAULT NULL COMMENT 'uuid',
  `newest_name` varchar(50) DEFAULT '0' COMMENT '楼盘名称',
  `dim_city_id` int(10) DEFAULT NULL COMMENT '城市ID',
  `city_id` int(11) DEFAULT NULL COMMENT '城市ID',
  `region_id` int(11) DEFAULT NULL COMMENT '区县ID',
  `city_name` varchar(50) DEFAULT '0' COMMENT '城市',
  `gd_lng` decimal(10,7) DEFAULT NULL COMMENT '高德经度',
  `gd_lat` decimal(10,7) DEFAULT NULL COMMENT '高德纬度',
  `address` varchar(255) DEFAULT '0' COMMENT '楼盘地址',
  `unit_price` decimal(18,2) DEFAULT '0.00' COMMENT '楼盘单价（元/平米）',
  `developer` varchar(255) DEFAULT '0' COMMENT '开发商',
  `investor` varchar(255) DEFAULT '0' COMMENT '投资商',
  `brander` varchar(255) DEFAULT '0' COMMENT '品牌商',
  `land_area` double DEFAULT NULL COMMENT '总占地面积',
  `building_area` double DEFAULT NULL COMMENT '总建筑面积',
  `arch_style` varchar(255) DEFAULT '0' COMMENT '建筑风格',
  `green_rate` varchar(10) DEFAULT '0' COMMENT '绿化率',
  `volume_rate` decimal(8,2) DEFAULT NULL COMMENT '容积率',
  `building_type` varchar(255) DEFAULT '0' COMMENT '建筑类型',
  `right_term` int(3) DEFAULT '0' COMMENT '产权年限',
  `property_cate` varchar(10) DEFAULT '0' COMMENT '物业类型',
  `household_num` int(8) DEFAULT '0' COMMENT '规划户数',
  `property_comp` varchar(255) DEFAULT '0' COMMENT '物业公司',
  `property_fee` decimal(8,2) DEFAULT NULL COMMENT '物业费',
  `park_rate` varchar(255) DEFAULT '0' COMMENT '车位配比',
  `park_num` int(5) DEFAULT '0' COMMENT '车位数量',
  `park_price` decimal(10,2) DEFAULT NULL COMMENT '车位价格',
  `orientation` varchar(255) DEFAULT '0' COMMENT '朝向',
  `building_num` varchar(255) DEFAULT '0' COMMENT '楼栋数',
  `avg_distance` varchar(100) DEFAULT '0' COMMENT '平均楼间距',
  `ong_park_num` varchar(100) DEFAULT '0' COMMENT '地上车位数',
  `ung_park_num` varchar(100) DEFAULT '0' COMMENT '地下车位数',
  `build_density` varchar(255) DEFAULT '0' COMMENT '建筑密度',
  `floor_num` text COMMENT '建筑层数',
  `ambitus` text COMMENT '周边配套（园区配套,|分隔）-----要拆分到poi配套事实表',
  `bike_park_number` varchar(255) DEFAULT '0' COMMENT '非机动车停车位',
  `decoration` varchar(255) DEFAULT '0' COMMENT '装修情况',
  `estate_pic` text COMMENT '楼盘效果图',
  `heat_mode` varchar(280) DEFAULT NULL COMMENT '供暖方式',
  `power_mode` varchar(50) DEFAULT NULL COMMENT '供电方式',
  `water_mode` varchar(50) DEFAULT NULL COMMENT '供水方式',
  `ring_locate` varchar(50) DEFAULT NULL COMMENT '环线位置',
  `deco_level` varchar(50) DEFAULT NULL COMMENT '装修标准',
  `mate_equip` text COMMENT '建材设备',
  `designer` varchar(200) DEFAULT NULL COMMENT '建筑设计单位',
  `builder` varchar(200) DEFAULT NULL COMMENT '施工单位',
  `provide_sche` longtext COMMENT '加推时间/楼盘时刻/楼盘动态',
  `recent_opening_time` date DEFAULT NULL COMMENT '最近开盘时间',
  `recent_delivery_time` date DEFAULT NULL COMMENT '最近交房时间',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '修改时间',
  `is_del` tinyint(1) DEFAULT '0' COMMENT '0正常1删除',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `uuid` (`uuid`),
  KEY `idx_query` (`newest_name`,`city_name`) USING BTREE,
  KEY `idx_cityName` (`city_name`)
) ENGINE=InnoDB AUTO_INCREMENT=761247 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘信息表';


insert into dws_db.dws_newest_issue_code (city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time)
select b.city_id,b.city_name,b.region_id county_id,b.region_name county_name,b.uuid newest_id,b.newest_name,b.address,b.developer business,a.issue_number issue_code,a.issue_date,
       a.issue_quarter,a.issue_month,a.issue_area,a.room_sum supply_num,a.housing_id,0 dr ,now() create_time ,now() update_time from 
  (select housing_id,issue_number,issue_date,case when issue_date is not null then date_format(issue_date,'%Y%m') else issue_date end issue_month,case when issue_date is not null then concat(substr(issue_date,1,4) ,'Q',QUARTER(issue_date)) else issue_date end  issue_quarter,room_sum,issue_area from dwb_db.dim_housing_issue where is_del = 0 group by issue_date,housing_id,issue_number,room_sum,issue_area) a
left join 
  (select t1.*,t2.region_name from  (select id,uuid,newest_name,city_id,city_name,region_id,address,developer from dwb_db.dim_housing) t1 left join (select city_id ,region_id ,region_name  from dws_db.dim_geography dg where grade = 4 group by city_id ,region_id ,region_name ) t2 on t1.city_id = t2.city_id and t1.region_id=t2.region_id) b
on a.housing_id = b.id ;



truncate table dws_db.dws_newest_issue_code;





SHOW processlist;

kill 296471;

truncate table dwb_db.dim_housing_issue;
truncate table dwb_db.dim_housing;





--------------------------------------------- =========================== ---------------------------------------------------




-- 备份跑输
truncate table dws_db.dws_customer_month ;
truncate table dws_db.dws_customer_week ;
truncate table dws_db.dws_customer_sum ;



--------------------------------------------- =========================== ---------------------------------------------------




-- 和city_qua对照一下

truncate table dws_db.dws_newest_popularity_rownumber_quarter; 
truncate table dws_db.dws_newest_investment_pop_rownumber_quarter ; 

-- 跑数
insert into dws_db.dws_customer_cre(city_id,newest_id,exist,imei_num,period)
select b.city_id,a.* from 
  (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter 
  union all 
  select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter) a
left join 
  dwb_db.a_dwb_newest_info b on a.newest_id=b.newest_id 


truncate table dws_db.dws_customer_cre; 

select * from dwb_db.a_dws_newest_period_admit a,dwb_db.a_dwb_newest_info b where a.newest_id = b.newest_id and a.city_id != b.city_id ;
select city_id,newest_id from dwb_db.a_dws_newest_period_admit adnpa ;

select city_id,newest_id from dwb_db.a_dwb_newest_info adni ;


--------------------------------------------- =========================== ---------------------------------------------------





select a.city_id,a.city_name,a.period,a.supply_num,b.intention from dwb_db.dwb_issue_supply_city a left join dwb_db.dwb_newest_city_customer_num b on a.city_id = b.city_id and a.period =b.period where a.period = '2018Q1' order by b.intention;

insert
	into
	dws_db.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid)
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
	city_id cityid
from
	dwb_db.dwb_issue_supply_county
where
	period = '2018Q1'
	and dr = 0
union all
select
	city_name,
	city_name county_name,
	city_id city_id,
	period,
	sum(supply_value) value,
	null local_issue_value ,
	null local_room_sum_value,
	null cric_value,
	null value_from_index ,
	null county_name_merge,
	'1' city_county_index,
	'1' period_index,
	now() update_time,
	0 dr,
	now() create_time,
	null follow_people_num,
	city_id cityid
from
	dwb_db.dwb_issue_supply_county
where
	period = '2018Q1'
	and dr = 0
group by
	city_name,
	city_id,
	period


--------------------------------------------- =========================== ---------------------------------------------------



insert into dwb_db.dwb_dim_geography_55city (province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,dr,create_time,update_time)
select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,0,now(),now() from dws_db.dim_geography where city_name in ('南昌市','重庆市','天津市','常州市','绍兴市','郑州市','咸阳市','成都市','南通市','石家庄市','深圳市','北京市','三亚市','杭州市','贵阳市','西安市','武汉市','济南市','合肥市','肇庆市','青岛市','惠州市','长春市','珠海市','扬州市','广州市','南京市','沈阳市','徐州市','苏州市','上海市','保定市','宁波市','湖州市','赣州市','烟台市','济宁市','汕头市','昆明市','宝鸡市','佛山市','福州市','海口市','嘉兴市','九江市','丽水市','南宁市','厦门市','唐山市','温州市','无锡市','长沙市','淄博市') and grade = 4
union all 
select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,0,now(),now() from dws_db.dim_geography where city_name in ('东莞市','中山市') and grade = 3;


update  dwb_db.dwb_dim_geography_55city set dr=1;






--------------------------------------------- =========================== ---------------------------------------------------





-- 清空表
-- 表重建
-- 改代码
-- 手动跑2季度 调度跑3年数据

drop table dws_db.dws_imei_browse_tag ;

CREATE TABLE dws_db.dws_imei_browse_tag (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `period` varchar(255) NOT NULL COMMENT '分析周期',
  `imei` varchar(255) NOT NULL COMMENT 'imei',
  `concern` varchar(255) DEFAULT NULL COMMENT '关注',
  `intention` varchar(255) DEFAULT NULL COMMENT '意向',
  `urgent` varchar(255) DEFAULT NULL COMMENT '迫切',
  `cre` varchar(255) NOT NULL COMMENT '增存',
  PRIMARY KEY (`id`),
KEY `idx_dws_imei_browse_tag_period` (`period`) USING BTREE,
KEY `idx_dws_imei_browse_tag_imei` (`imei`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户浏览标签结果表';

SHOW processlist;

kill 2450100;



--------------------------------------------- =========================== ---------------------------------------------------




select housing_id,issue_number from dwb_db.dim_housing_issue dhi where issue_date > '2018' and issue_number < '2021-07' group by housing_id,issue_number ;

truncate table dws_imei_browse_tag; 


select imei from dws_db.dws_imei_browse_tag dibt where period = '2018Q4' group by imei ;

select imei from dws_db.dws_imei_browse_tag dibt where intention is not null and  period = '2018Q4' group by imei ;

select imei from dws_db.dws_imei_browse_tag dibt where urgent is not null and  period = '2018Q4' group by imei ;

select imei from dws_db.dws_imei_browse_tag dibt where cre =  group by imei ;



select 'imei 总数',count(1),period from dws_db.dws_imei_browse_tag group by imei,period 
union all 
select '意向 总数',count(1),period from dws_db.dws_imei_browse_tag where intention is not null group by imei,period 
union all 
select '迫切 总数',count(1),period from dws_db.dws_imei_browse_tag where urgent is not null group by imei,period ;



delete from dws_db.dws_imei_browse_tag where period = '2020Q1';

select city_id from dwb_db.a_dws_newest_period_admit adnpa group by city_id ;


truncate table dwb_db.dwb_newest_customer_info ;

truncate table dwb_db.dwb_newest_city_customer_num ;

truncate table dwb_db.dwb_newest_county_customer_num ;

truncate table dwb_db.dwb_issue_supply_county ;

truncate table dws_db.dws_supply ;

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dw_a'
AND TABLE_NAME LIKE  '%dim_housing%';
	

insert
	into
	dws_db.dws_supply
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
	city_id cityid
from
	dwb_db.dwb_issue_supply_county where period = '' and dr = 0
union all
select
	city_name,
	city_name county_name,
	city_id city_id,
	period,
	sum(supply_value) value,
	null local_issue_value ,
	null local_room_sum_value,
	null cric_value,
	null value_from_index ,
	null county_name_merge,
	'1' city_county_index,
	'1' period_index,
	now() update_time,
	0 dr,
	now() create_time,
	null follow_people_num,
	city_id cityid
from
	dwb_db.dwb_issue_supply_county where period = '' and dr = 0
group by
	city_name,
	city_id,
	period



--------------------------------------------- =========================== ---------------------------------------------------


SELECT imei,'1' add_new_code FROM  dwb_db.dwb_customer_add_new_code where add_new_code = '0';

SHOW processlist;

kill 2390603;


delete from dwb_db.dwb_customer_add_new_code where visit_quarter = '2021Q2';

-- 备份数据
-- 删除数据
-- 插入数据
-- 更新浏览日志表
truncate table dwb_db.a_dwb_customer_browse_log_0830;


truncate table dwb_db.dwb_customer_add_new_code ;

CREATE TABLE dwb_db.a_dwb_customer_browse_log_0830 (
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
) ENGINE=InnoDB AUTO_INCREMENT=46005571 DEFAULT CHARSET=utf8mb4 COMMENT='客户浏览楼盘日志表（每日增量）';





delete from dwb_db.a_dwb_newest_info where newest_id in ('4ae2fa6382fbdf81653e7722ceb2a682','74812432f4f7091e7707f2f34a9e1009','2ef70fe9b61f9114f0d377d35d2e83a1','22e6a5b75937fc559ae293adb5642faf','0ebb0796e43c32dc509d6545646b4cd1','e0c4fc6a92e27c04e859e44684ec53b7','7013b48e1386f291038629a886534958','92be1f62a01e75b651e4bb71ff01a64e','f430a3f50ceb631d8d13d5e0bdb98edd','e8855c11a499b39f99ae1079310cc632','3c69f7ebd691706b9041849c2d6b72f2','287010d5cf488746d85531824d3df105','f8d96c71021ac080bda67b3d0cffa641','8fc85917f67f04c37c8f569f8be23ce1','aec483080e461a39a4711e8075e438df','fe083e6805ad5f5baf07f74ed105fd79','1a0e6292b015527afd4d9ead6a18003e','823c64d3cb9575182756fbfb5dac0098','bffc9e28e3390026b93698e41036bec2','fd850547a093eedd26757d39905cfc72','0d5968e40c1463dc3eae96ae7e36ea8c','575ada7359e7edb72aeb08c7dd786e54','7af999dfb22fd36306ac08c5705de110','d5749d01f43e54c326fc1268d27b4a8b','d2e12b2b3a91b45aa817c2151e72caa8','697c79ef3e061da7705ce911e3cc853b','46375f44cff0897387d75d48a850a585','726367947f6fc0c87816d2b25990fb63','e109f8247709cc3ff1bf4beeaa4728fb','8368b9bde4a319f407b2f5034943aa01','ff52445024ac12325bbdb03c2ce8a923','b0140a06bdc411f80abe77104916f1df','b18b133858e2dcccee2433040658f271','557bc45f641d9b9f2bab2f5b29a107fd') 
;

select * from dw_a.a_dwb_newest_info where uuid in ('4ae2fa6382fbdf81653e7722ceb2a682','74812432f4f7091e7707f2f34a9e1009','2ef70fe9b61f9114f0d377d35d2e83a1','22e6a5b75937fc559ae293adb5642faf','0ebb0796e43c32dc509d6545646b4cd1','e0c4fc6a92e27c04e859e44684ec53b7','7013b48e1386f291038629a886534958','92be1f62a01e75b651e4bb71ff01a64e','f430a3f50ceb631d8d13d5e0bdb98edd','e8855c11a499b39f99ae1079310cc632','3c69f7ebd691706b9041849c2d6b72f2','287010d5cf488746d85531824d3df105','f8d96c71021ac080bda67b3d0cffa641','8fc85917f67f04c37c8f569f8be23ce1','aec483080e461a39a4711e8075e438df','fe083e6805ad5f5baf07f74ed105fd79','1a0e6292b015527afd4d9ead6a18003e','823c64d3cb9575182756fbfb5dac0098','bffc9e28e3390026b93698e41036bec2','fd850547a093eedd26757d39905cfc72','0d5968e40c1463dc3eae96ae7e36ea8c','575ada7359e7edb72aeb08c7dd786e54','7af999dfb22fd36306ac08c5705de110','d5749d01f43e54c326fc1268d27b4a8b','d2e12b2b3a91b45aa817c2151e72caa8','697c79ef3e061da7705ce911e3cc853b','46375f44cff0897387d75d48a850a585','726367947f6fc0c87816d2b25990fb63','e109f8247709cc3ff1bf4beeaa4728fb','8368b9bde4a319f407b2f5034943aa01','ff52445024ac12325bbdb03c2ce8a923','b0140a06bdc411f80abe77104916f1df','b18b133858e2dcccee2433040658f271','557bc45f641d9b9f2bab2f5b29a107fd') 
;

INSERT INTO dwb_db.a_dwb_newest_info (main_id,newest_id ,platform,url,newest_name,city_id,city_name,county_id,county_name,block_name,ori_lnglat,address,sale_address,sale_phone,ori_alias,alias,former_name,layout,total_price,unit_price,recent_opening_time,recent_delivery_time,issue_date,issue_number,developer,investor,brander,land_area,building_area,arch_style,green_rate,volume_rate,building_type,right_term,property_type,property_sub,household_num,property_comp,property_fee,park_rate,park_num,park_price,orientation,building_num,avg_distance,ong_park_num,ung_park_num,build_density,floor_num,ambitus,bike_park_number,agent,decoration,layout_pic,estate_pic,advantage,disadvantages,pay_meth,sale_status,gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,gd_office_name,is_accurate,heat_mode,power_mode,water_mode,ring_locate,`character`,deco_level,mate_equip,designer,builder,progress,provide_sche,land_time,create_time,update_time,r_id,remark,alias_json,flag) VALUES
	 (NULL,'0d5968e40c1463dc3eae96ae7e36ea8c','0',NULL,'清凤黄金海岸',460200,'三亚市',460202,'海棠区',NULL,'0','临高海南西线高速金牌出口龙波湾度假区查看地图','0','0',NULL,'清凤黄金海岸,黄金海岸','0','2厅2厨5卫,3室2厅2卫,4室2厅2卫,4室2厅3卫','0.00','12000.00','2019-06-20','2021-12-31',NULL,'(2019)临房预字(20)号','暂无数据','暂无数据','暂无数据','790774','424185','0','60%','1.01','板楼,板塔结合,低层,高层','70','0','住宅类','3004','成都市金优物业服务有限责任公司',NULL,NULL,'2190','0.00',NULL,'98',NULL,NULL,'2190','0',NULL,'暂无数据','0','0','毛坯','https://t.focus-img.cn/sh520x390sh/xf/xc/5f1bfb97-19df-40c9-9a73-6098d4d3348f.JPEG',NULL,'0','0','0','在售','0','0','0','109.7899050','18.4078500','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','暂无数据',NULL,NULL,NULL,'【清凤黄金海岸】在售楼栋为1#、2#、9#楼|清凤黄金海岸项目一期在售，在售楼栋为1#、2#、9#楼。户型：1#、2#楼户型为建面101-103平的三房，9#楼户型为建面131-146平四房。在售价格：13000-15000元/㎡。|2020-06-15^【清凤黄金海岸】在售价格：13000-15000元/㎡|清凤黄金海岸项目一期在售，在售楼栋为1#、2#、9#楼。户型：1#、2#楼户型为建面101-103平的三房，9#楼户型为建面131-146平四房。在售价格：13000-15000元/㎡。|2020-05-08^【清凤黄金海岸】项目一期在售，在售楼栋为1#、2#、9#楼|清凤黄金海岸项目一期在售，在售楼栋为1#、2#、9#楼。户型：1#、2#楼户型为建面101-103平的三房，9#楼户型为建面131-146平四房。在售价格：13000-15000元/㎡。|2020-01-05^【清凤黄金海岸】在售:户户观海起价13000元/㎡|清凤黄金海岸项目一期在售，在售楼栋为1#、2#、9#楼。户型：1#、2#楼户型为建面101-103平的三房，9#楼户型为建面131-146平四房。在售价格：13000-15000元/㎡。|2019-01-30^【清凤黄金海岸】项目一期1#2#9#楼在售|清风黄金海岸项目在售一期1#2#9#楼、层高12.15层。2梯3户、1号楼-2号楼户型建筑面积101-103平的三房。9号楼户型建筑面积131-146平四房、预计均价13000-15000元/平。|2019-01-14',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'0ebb0796e43c32dc509d6545646b4cd1','0',NULL,'龙栖湾-波波利海岸',460200,'三亚市',460205,'崖州区',NULL,'0','九所龙栖湾','0','0',NULL,'波波利海岸,龙栖湾-波波利海岸,龙栖湾・波波利海岸,龙栖湾波波利海岸','0','1室1厅1卫,1室1厅1厨1卫,1室1厨1卫,2室1厅1厨1卫,2室2厅1厨2卫,2室2厅2卫,3室2厅1厨2卫,3室2厅2卫,4室2厅2卫','0.00,133.00,231.00,243.00,277.00','22000.00','2015-10-31','2018-09-01',NULL,'乐房售字证(2015)008号,乐房售证字(2011)019号,乐房售证字(2012)011号,乐房售证字(2013)013号','海南龙栖湾发展置业有限公司',NULL,NULL,'190000','385265','0','45%','1.51','板楼^高层^超高层^','70','0','住宅类','3241','海南龙栖湾物业管理服务有限公司','3.80',NULL,'2390','0.00',NULL,'109栋',NULL,NULL,NULL,'0','8栋28层，101套别墅首推3栋高层，34套别墅','交通|公交路线：大巴：三亚-九所^幼儿园|九所幼儿园^银行|中国建设银行^其他|小鱼儿客栈^小区内部配套|小区内部配套：会所（精品酒店），总体规划有高尔夫、游艇码头、5星级酒店，1600亩填海工程等','0','0','带装修','//imgs.soufunimg.com/viewimage/house/2011_07/20/sanya/1311149763481_000/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','108.9806840','18.3801050','0','0','暂无数据',NULL,NULL,NULL,NULL,'3000.0元/平米','1、门窗：①、入户门：采用甲级FM1钢木防火防盗门、带大门套，尺寸1000×2600，带上亮子（门板封闭有压花）②、外窗：Low-E中空玻璃、高透光率，黑色铝合金框③、内门：二次装修做④、折叠门：M1526、黑色铝合金框玻璃门2、地板：公共部位600×600米白色地砖3、公共空间内墙面、电梯厅及走廊墙砖：①、自地面600高度以内及门头以上部位：铺高度100、水平长度600、米白色墙砖，沿水平边倒角、密排。②、墙面其余部位：600x600米白色墙砖，沿水平边倒角。③、大样图增加公共部位墙面铺砖示意4、栏杆：①、内栏杆：玻璃栏杆、高度1.3m、不锈钢栏杆扶手900，扶手与玻璃间距控制在10-15cm左右②、外栏杆：1.1m高玻璃栏杆5、外墙面:亚光白色涂料6、楼梯间:钢制楼梯栏杆及扶手、水泥地面、白色普通内墙涂料','加拿大KDG设计集团中国事务部果核设计咨询（上海）有限公司','吴川市建筑安装工程公司',NULL,'龙栖湾・波波利海岸均价为：22000元/平方米|龙栖湾・波波利海岸22000元/平方米。龙栖湾・波波利海岸项目为高层,超高层。在售户型有：115平-128平2居-3居。物业费：公寓3.8元/平米・月，别墅6.8元/平米・月。共3241户。...[|^龙栖湾・波波利海岸均价为：22000元/平方米|龙栖湾・波波利海岸22000元/平方米。容积率1.51。绿化率45%。带装修交房。物业费：3.8元/m²・月。龙栖湾・波波利海岸在售户型有：115平-128平2居-3居。项目共8栋楼,8栋28层...[|^龙栖湾・波波利海岸在售115-128m²的两房和三房|龙栖湾・波波利海岸位于乐东龙栖湾，目前二期房源在售，在售115-128m²的两房和三房，均价约22000元/m²，硬装交付，预计今年五月份交房。具体详情优惠请咨询售楼处。[|^龙栖湾・波波利海岸价格待定|龙栖湾・波波利海岸价格待定。龙栖湾・波波利海岸楼盘地址：九所龙栖湾。共3241户。带装修交付。容积率1.51。龙栖湾・波波利海岸物业费：公寓3.8元/平米・月，别墅6.8元/平...[|^龙栖湾・波波利海岸价格待定|龙栖湾・波波利海岸价格待定。龙栖湾・波波利海岸楼盘地址：九所龙栖湾。容积率1.51。物业费：3.8元/m²・月。绿化率45%。龙栖湾・波波利海岸项目规划建设3241户。在售户型有...[|^龙栖湾・波波利海岸价格待定|龙栖湾・波波利海岸价格待定。共3241户。绿化率45%。项目为高层,超高层板楼。容积率1.51。龙栖湾・波波利海岸物业费：3.8元/m²・月。九所龙栖湾。主力户型为30.15平-348.7平...[|^龙栖湾・波波利海岸价格待定|龙栖湾・波波利海岸价格待定。带装修交房。九所龙栖湾。主力户型为30.15平-348.7平1居-5居。绿化率45%。龙栖湾・波波利海岸容积率1.51。九所幼儿园,中国建设银行,小鱼儿客栈...[|^龙栖湾・波波利海岸价格待定|龙栖湾・波波利海岸价格待定。龙栖湾・波波利海岸项目为高层,超高层。容积率1.51。共3241户。绿化率45%。龙栖湾・波波利海岸主力户型为30.15平-348.7平1居-5居。物业费：公...[|^龙栖湾・波波利海岸项目销售信息待定|栖湾・波波利海岸项目总体规划分为滨海酒店区、滨海别墅度假区、高尔夫球场、填海海上酒店、住宅综合区等五大功能分区，占地5000余亩,其中陆地部分1400亩，高尔夫占地1800亩...[|^龙栖湾・波波利海岸均价约17100元/m²|龙栖湾・波波利海岸位于乐东龙栖湾，目前在售115-128m²的两房和三房，均价约17100元/m²，特价房源16700元/m²，具体详情优惠请咨询售楼处。[|^龙栖湾・波波利海岸目前有特价房源全款98折|龙栖湾・波波利海岸位于乐东龙栖湾，占地19万平，是大三亚西部开发的龙头项目，目前在售115-128平的两房和三房，均价1.71万/m²，特价房源1.67万/m²，大概98折左右，具体详情...[|^龙栖湾・波波利海岸目前均价1.71万/m²|龙栖湾・波波利海岸位于九所龙栖湾，目前均价1.71万/m²，在售户型115平的2+1房和128平的3+1房，预计今年10月交房，更多详情优惠请咨询售楼处！售楼处电话为：400-890-0000转...[|^龙栖湾・波波利海岸均价为：16000元/平方米|龙栖湾・波波利海岸16000元/平方米。在售户型有：115平-128平2居-3居。物业费：公寓3.8元/平米・月，别墅6.8元/平米・月。绿化率45%。项目为高层,超高层板楼。龙栖湾・波波...[|^龙栖湾・波波利海岸现在均价1.6万/m²|龙栖湾・波波利海岸现在二期平层住宅在售，均价1.6万/m²，115平的2房2厅2卫（可改2+1房）128平的3房两厅两卫（可改3+1房）预计2020年10月左右交房项目地处北纬18度以南的大...[|^龙栖湾・波波利海岸折后均价17000元/m²|龙栖湾・波波利海岸项目占地5000余亩，坐拥山、海、河、田园四种稀缺资源，秉承人文慢生活个性化开发理念，将四种稀缺资源归纳为：纳山藏海半山组团、田园风情桃花源组团、...[|^龙栖湾・波波利海岸山景、园景、远观海景三重景观|龙栖湾・波波利海岸目前在售户型115平米的2+1房及128平的3+1房，折后均价17000元/m²多，硬装交房，装修标准2000元/m²。让您在最这神奇的海岸上，欣赏着“山景、园景、远观海...[|^龙栖湾・波波利海岸折后均价17000元/m²|龙栖湾・波波利海岸目前在售户型115平米的2+1房及128平的3+1房，折后均价17000元/m²多，硬装交房，装修标准2000元/m²。更多详询售楼处。售楼处电话为：400-890-0000转66463...[|^龙栖湾・波波利海岸均价19800元/m²，硬装交房|龙栖湾・波波利海岸目前在售户型115平米的2+1房及128平的3+1房，均价19800元/m²，硬装交房，装修标准2000元/m²。更多详询售楼处。售楼处电话为：400-890-0000转664630。小区...[|^龙栖湾・波波利海岸在售大户型|龙栖湾・波波利海岸由海景高层公寓、数百栋半山度假别墅以及斥资1.5亿打造的无穷大会所艺术中心所组成。项目坐落在北纬18°以南的龙栖湾，享受着25.5°海洋气候的滋润。目前...[|^龙栖湾・波波利海岸均价19800元/m²|龙栖湾・波波利海岸楼盘地址：九所龙栖湾。目前在售户型115平米的2+1房及128平的3+1房，均价19800元/m²，硬装交房，装修标准2000元/m²。更多详询售楼处。售楼处电话为：400...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'1a0e6292b015527afd4d9ead6a18003e','0',NULL,'瀚庭国际',460200,'三亚市',460204,'天涯区',NULL,'0','文昌文城镇清澜经济开发区疏港路与白金路的交界处查看地图','0','0',NULL,'瀚庭国际','0','1厅1厨55卫,1室1厅1卫,1室2厅1卫,4室2厅3卫','0.00,77.00,141.00','17000.00',NULL,NULL,NULL,NULL,'暂无数据','暂无数据','暂无数据','4424.52','23804','0',NULL,'3.00',NULL,'40','0','住宅类','295','暂无数据',NULL,NULL,'112','0.00',NULL,'1',NULL,'1','1','0',NULL,'医院：文昌市中医院清澜分院；^银行：中国银行ATM、交通银行ATM、中国建设银行ATM等；^超市：千百汇大型超市(书香店)、千百汇超市(清澜店)、恒兴超市(逸龙湾分店)等；^商场：逸龙湾商业广场、百合财富广场；^餐饮：高隆湾中西茶园、宜宴四季餐厅、经典小院、渝海轩餐厅、渝大家常菜等；^酒店：文昌逸龙湾度假酒店、金石大酒店、海南白金海岸度假酒店(文昌等；^学校：文昌市第三中学、清澜第二小学、清澜成龙幼儿园(文清大道)、育才幼儿园；^娱乐：文明园茶馆、老地方咖啡清吧(清澜高隆湾店)、紫气东来、高隆湾中西茶园等；^景点：椰子大观园、绿岛文化园、高隆湾、祝嘉故居；^加油站：佳能加油站、恒兴加油站。','0','0','精装修','https://t.focus-img.cn/sh520x390sh/xf/xc/16fa1d43-05d3-457c-bd77-b2f28938d168.JPEG',NULL,'0','0','0','在售','0','0','0','109.2845360','18.3097320','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','暂无数据',NULL,NULL,NULL,'【瀚庭国际】均价17000-18000元/平|瀚庭国际公馆位于海南省文昌市高隆湾经济开发区。项目住宅在售，主力户型建面为53平开间，54-62平开间、100平一房、108平两房，均价17000-18000元/平，预计2018年底毛坯交房。注:以上信息供参考,具体信息仍需参考现场实际销售情况。|2020-09-02^【瀚庭国际】均价17000-18000元/平|瀚庭国际公馆位于海南省文昌市高隆湾经济开发区。项目住宅在售，主力户型建面为53平开间，54-62平开间、100平一房、108平两房，均价17000-18000元/平，预计2018年底毛坯交房。注:以上信息供参考,具体信息仍需参考现场实际销售情况。|2020-07-30^【瀚庭国际】均价17000-18000元/平|瀚庭国际公馆位于海南省文昌市高隆湾经济开发区。项目住宅在售，主力户型建面为53平开间，54-62平开间、100平一房、108平两房，均价17000-18000元/平，预计2018年底毛坯交房。|2020-06-13^【瀚庭国际】均价17000-18000元/平|瀚庭国际公馆位于海南省文昌市高隆湾经济开发区。项目住宅在售，主力户型建面为53平开间，54062平开间、100平一房、108平两房，均价17000-18000元/平，预计2018年底毛坯交房。|2020-05-04^【瀚庭国际】位于海南省文昌市高隆湾经济开发区|瀚庭国际公馆位于海南省文昌市高隆湾经济开发区。项目住宅在售，主力户型建面为53平开间，54062平开间、100平一房、108平两房，均价17000-18000元/平，预计2018年底毛坯交房。|2020-01-07',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'22e6a5b75937fc559ae293adb5642faf','0',NULL,'懿品-香格里',460200,'三亚市',460202,'海棠区',NULL,'0','迎宾干道(三月三大道)理文路一侧南行500米','0','0',NULL,'懿品-香格里,懿品・香格里','0','2厅1厨2卫,2室2厅1厨2卫,3室2厅1厨2卫,3室2厅1厨3卫,3室2厅1厨4卫,3室2厅3卫','0.00','20000.00','2015-02-01','2018-06-30',NULL,'(2015)五房预许(0002)号,(2015)海房预字0002号,2015(0002)号,海-五-20191200001','海南通什香格里拉旅业发展有限公司',NULL,NULL,'128920','146000','0','50%','0.50','独栋','70','0','住宅类','1000','展烁物业','3.90',NULL,'225','0.00',NULL,'165栋',NULL,'225',NULL,'0','水岸别墅2+1共8栋；林语别墅2+2共54栋；山景别墅2+2共60栋；合院别墅八户院共10栋、四联排共8栋；联排别墅共12栋；小独栋别墅9栋、大独栋别墅4栋。','交通|市区3路公交车途径懿品・香格里，在候鸟花园站上下车即可，距离1、2路车站仅为100米。^幼儿园|五指山市机关幼儿园^中小学|五指山市第一小学，琼州学院附属中学，海南省第二卫生学校，民族技工学校^综合商场|壹号食府，奇珍大酒店，湘味园，华莱士快餐，草原牧歌火锅，五指山市中心农贸市场，华爵商务酒店，福安泰隆大酒店，福德莱大酒店^医院|海南省第二人民医院、丰源大药房，便民医疗诊所，五指山市妇幼保健所，广安堂药品超市，康顺药店^银行|五指山市邮政局，海南农村信用社，五指山市邮政储蓄所，中国农业银行，中国建设银行^其他|三月三广场，街心公园（规划中），国家奥林匹克中心，中心商业城（在建），市政务中心，五指山市青少年活动中心，五指山市财政局，五指山宣传文化中心，五指山市政府，五指山市财政局^小区内部配套|商业街、游泳池、老年文化中心','0','0','带装修','//imgs.soufunimg.com/viewimage/house/2014_10/24/sanya/1414135396223_000/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.7377400','18.3457760','0','0','暂无数据',NULL,NULL,NULL,NULL,'3000.0元/平米','精装修宽带：有供水系统：市政供水暖气：无',NULL,NULL,NULL,'懿品・香格里在售价格为：20000元/平方米|懿品・香格里20000元/平方米。绿化率50%。迎宾干道(三月三大道)理文路一侧南行500米。物业费：3.9元/m²・月。项目规划建设1000户。懿品・香格里五指山市机关幼儿园,五指山市...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。主力户型为125平-213平2居-3居。带装修交房。迎宾干道(三月三大道)理文路一侧南行500米。项目规划建设1000户。懿品・香格里绿化率50%。物业费...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。懿品・香格里位于：迎宾干道(三月三大道)理文路一侧南行500米。容积率0.5。绿化率50%。在售户型有：125平-213平2居-3居。懿品・香格里带装修交...[|^懿品・香格里在售价格为：20000元/平方米|懿品・香格里20000元/平方米。物业费：3.9元/m²・月。共1000户。在售户型有：125平-213平2居-3居。绿化率50%。懿品・香格里带装修交房。五指山市机关幼儿园,五指山市第一小...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。容积率0.5。带装修交付。懿品・香格里楼盘地址：迎宾干道(三月三大道)理文路一侧南行500米。项目规划建设1000户。懿品・香格里在售户型有：12...[|^懿品・香格里在售价格为：20000元/平方米|懿品・香格里20000元/平方米。懿品・香格里位于：迎宾干道(三月三大道)理文路一侧南行500米。绿化率50%。带装修交付。共1000户。懿品・香格里容积率0.5。项目共165栋楼,水岸...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。容积率0.5。共1000户。带装修交付。迎宾干道(三月三大道)理文路一侧南行500米。懿品・香格里绿化率50%。市区3路公交车途径懿品・香格里，在候...[|^懿品・香格里在售价格为：20000元/平方米|懿品・香格里20000元/平方米。带装修交付。共1000户。迎宾干道(三月三大道)理文路一侧南行500米。主力户型为125平-213平2居-3居。懿品・香格里物业费：3.9元/m²・月。市区3...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。懿品・香格里楼盘地址：迎宾干道(三月三大道)理文路一侧南行500米。容积率0.5。项目规划建设1000户。绿化率50%。懿品・香格里在售户型有：125...[|^懿品・香格里在售价格为：20000元/平方米|懿品・香格里20000元/平方米。绿化率50%。主力户型为125平-213平2居-3居。项目规划建设1000户。容积率0.5。懿品・香格里楼盘地址：迎宾干道(三月三大道)理文路一侧南行500米...[|^懿品・香格里领跑五指山旅游养生地产|懿品・香格里是海南省五指山市半山雨林亲水别墅及雨林度假酒店项目，凭借其稀缺原始雨林环境及山景资源,以低密度、高质品、优服务、贴近自然融入自然为概念，与自然完美融...[|^懿品・香格里均价为：20000元/平方米|懿品・香格里20000元/平方米。带装修交房。物业费：3.9元/m²・月。在售户型有：125平-213平2居-3居。容积率0.5。懿品・香格里项目规划建设1000户。懿品・香格里位于：迎宾干...[|^懿品・香格里别墅均价20000元/m²|懿品・香格里有水岸别墅、林语别墅、山景别墅7栋、合院别墅、联排产品在售，建面约43-151m²一房至三房，均价20000元/m²，详情请咨询售楼处。[|^懿品・香格里在售价格为：15000元/平方米|懿品・香格里15000元/平方米。物业费：3.9元/m²・月。项目规划建设1000户。绿化率50%。容积率0.5。懿品・香格里带装修交房。市区3路公交车途径懿品・香格里，在候鸟花园站上...[|^懿品・香格里在售价格为：15000元/平方米|懿品・香格里15000元/平方米。懿品・香格里位于：迎宾干道(三月三大道)理文路一侧南行500米。在售户型有：125平-213平2居-3居。带装修交房。物业费：3.9元/m²・月。懿品・香...[|^懿品・香格里在售价格为：15000元/平方米|懿品・香格里15000元/平方米。绿化率50%。容积率0.5。带装修交房。在售户型有：125平-213平2居-3居。懿品・香格里物业费：3.9元/m²・月。迎宾干道(三月三大道)理文路一侧南...[|^懿品・香格里均价为：15000元/平方米|懿品・香格里15000元/平方米。带装修交付。容积率0.5。项目规划建设1000户。懿品・香格里楼盘地址：迎宾干道(三月三大道)理文路一侧南行500米。懿品・香格里物业费：3.9元/...[|^懿品・香格里均价为：15000元/平方米|懿品・香格里15000元/平方米。绿化率50%。共1000户。容积率0.5。非毛坯交房。懿品・香格里物业费：3.9元/m²・月。懿品・香格里位于：迎宾干道(三月三大道)理文路一侧南行50...[|^懿品・香格里均价为：15000元/平方米|懿品・香格里15000元/平方米。非毛坯交房。项目规划建设1000户。容积率0.5。懿品・香格里楼盘地址：迎宾干道(三月三大道)理文路一侧南行500米。懿品・香格里绿化率50%。在售...[|^懿品・香格里在售价格为：15000元/平方米|懿品・香格里15000元/平方米。懿品・香格里位于：迎宾干道(三月三大道)理文路一侧南行500米。项目规划建设1000户。主力户型为125平-213平2居-3居。容积率0.5。懿品・香格里...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'2ef70fe9b61f9114f0d377d35d2e83a1','0',NULL,'华润-石梅湾',460200,'三亚市',460203,'吉阳区',NULL,'0','兴隆石梅湾旅游度假区','0','0',NULL,'华润-石梅湾,华润·石梅湾,华润・石梅湾','0','2厅1厨64卫,2室2厅1厨1卫,2室2厅1厨2卫,3室2厅1厨1卫,3室2厅1厨2卫','0.00','23000.00','2020-09-01','2022-07-30',NULL,'(2011)万房预字(022)号,(2012)万房预字(2)号,(2013)万房预字14号,(2017)万房预字(29)号,(2018)万房预字(37)号,(2018)万房预字(53)号,(2019)万房预字(10)号,(2019)万房预字(34)号,(2020)万房预字(02)号,(2020)万房预字(13)号,2016万房预字(17)号','海南华润石梅湾旅游开发有限公司',NULL,NULL,'12000000','2100000','0','60%','0.50','普通住宅^独栋^双拼^旅游商铺','70','0','住宅类','5261','深圳华润物业管理有限公司万宁分公司','4.80',NULL,'0','0.00',NULL,'39栋',NULL,NULL,NULL,'0','四期3栋，五期10栋。四期A1#为16层，A2#为17层，A3#为13层；五期6#、7#、8#、9#、10#、11#、15#为18层，12#、13#为17层，16#为6层洋房。','交通|公交：湾区业主巴士（兴隆、万宁、三亚）^中小学|莲花小学、兴隆第五小学、礼纪中心小学、海南万宁思源实验学校^综合商场|曼特宁商业广场、文化商业广场、仙泉时代广场^医院|万宁市人民医院、海南省农垦南林医院、万宁市妇幼保健院^银行|中国工商银行ATM、中国建设银行ATM、中国邮政储蓄银行^邮政|中国邮政、圆通速递、中通快递^其他|距离兴隆商圈10公里，景点资源丰富，热带花园、热带植物园、巴厘村等，旁有奥特莱斯购物广场^小区内部配套|医疗服务中心、商业街、健身设施、游泳池、会所、露天停车位、垃圾回收点、消防设施','0','0','带装修','//img11.soufunimg.com/viewimage/house/2019_01/14/M0C/11/99/ChCE4Fw8EmqIa6XKAAjRV0W7QDoABJCsQFNQvsACNFv990/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.5203340','18.2217710','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','装修：精装装修详情：拎包入住装修供电系统：与海南省电网联网通讯设备：区内规划建设各种公众媒体网络、通信、宽带互联网等',NULL,NULL,NULL,'|交房时间：预计2022年7月底五期16#、五期7#交房|^|交房时间：预计2022年3月31日四期A1#、四期A2#、四期A3#交房|^|交房时间：二期别墅二三标段预计2021年10月1日|^华润・石梅湾均价约23000元/m²|华润・石梅湾位于海南省万宁市兴隆石梅湾旅游度假区，目前五期高层住宅在售，建筑面积约104-138m²精装瞰海高层，在售起价17000元/m²，均价约23000元/m²。项目总占地面积约1...[|^华润・石梅湾均价为：23000元/平方米|华润・石梅湾23000元/平方米。在售户型有：68平-168平1居-3居。兴隆石梅湾旅游度假区。绿化率60%。预计2022年7月底五期16#、五期7#交房。华润・石梅湾公交：湾区业主巴士（...[|^华润・石梅湾建面约104-138m²精装瞰海高层在售|华润・石梅湾位于海南省万宁市兴隆石梅湾旅游度假区，目前五期高层住宅在售，建筑面积约104-138m²精装瞰海高层，在售起价17000元/m²，均价约23000元/m²。项目总占地面积约1...[|^华润・石梅湾均价为：25000元/平方米|华润・石梅湾25000元/平方米。预计2022年7月底五期16#、五期7#交房。共5261户。绿化率60%。华润・石梅湾楼盘地址：兴隆石梅湾旅游度假区。华润・石梅湾公交：湾区业主巴士（...[|^华润・石梅湾在售价格为：25000元/平方米|华润・石梅湾25000元/平方米。华润・石梅湾楼盘地址：兴隆石梅湾旅游度假区。主力户型为68平-168平1居-3居。预计2022年7月底五期16#、五期7#交房。带装修交付。华润・石梅湾...[|^|交房时间：二期1#、2#预计2020年12月31日交付|^华润・石梅湾五期高层住宅在售|华润・石梅湾位于海南省万宁市兴隆石梅湾旅游度假区，总占地面积约1200万m²，总建筑面积约210万m²，总体容积率0.5，绿化率60%，项目总投资约120亿，打造美好滨海度假湾区生...[|^华润・石梅湾在售均价25000元/m²|华润・石梅湾位于海南省万宁市，目前五期新户型在售，建筑面积105-118m²三房高层户型，在售均价25000元/m²，欢迎您到售楼处选购！社区环境[|^华润・石梅湾均价为：25000元/平方米|华润・石梅湾25000元/平方米。预计2022年7月底五期16#、五期7#交房。物业费：4.8元/m²・月。主力户型为68平-168平1居-3居。容积率0.5。华润・石梅湾五期7#16#预计2020年9月...[|^华润・石梅湾延续美好之旅|华润・石梅湾延续美好之旅，致力打造滨海生活度假旅游综合区，以6公里天然海岸线为界，与4000年青皮林为邻，坐拥西岭山、石梅溪、海田溪等丰富资源，集美好享受于一体，开辟...[|^华润・石梅湾-度假和家结合的地方|华润・石梅湾位于海南万宁石梅湾旅游度假区，项目占地约1200万平方米，配套资源丰富，拥有4000年历史的青皮林带，项目私属产权岛屿-加井岛、游艇码头、多家五星级酒店等。项...[|^华润・石梅湾-度假和家结合的地方|华润・石梅湾位于海南万宁石梅湾旅游度假区，项目占地约1200万平方米，配套资源丰富，拥有4000年历史的青皮林带，项目私属产权岛屿-加井岛、游艇码头、多家五星级酒店等。项...[|^华润・石梅湾五期7#16#预计2020年9月开盘|华润・石梅湾五期7#16#预计2020年9月开盘。25000元/平方米。预计2022年7月底五期16#、五期7#交房。物业费：住宅：4.8别墅：6.8（元/平方米/月）。容积率0.5。华润・石梅湾...[|^华润・石梅湾5重自然景观，享受美好度假生活|华润・石梅湾5重自然景观：靠山，风光秀美的西岭山，于沉稳中静守这片湾的悠然；面海，约6公里天然海滩，日享阳光碧海，夜赏璀璨星海；望岛，潜水胜地加井岛，于清澈海水中...[|^华润・石梅湾五期7#16#预计2020年9月开盘|华润・石梅湾五期7#16#预计2020年9月开盘。25000元/平方米。预计2022年7月底五期16#、五期7#交房。在售户型有：68平-168平1居-3居。物业费：住宅：4.8别墅：6.8（元/平方...[|^华润・石梅湾推出8套特价房源|华润・石梅湾推出8套特惠房源。2-3居花园洋房，建筑面积约105-138m²，优惠单价15967元/m²，总价174万/套起。活动时间2020年10月1号至10月31号。更多详情优惠欢迎咨询售楼处...[|^|交房时间：三期5#预计2020年10月8日|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'7013b48e1386f291038629a886534958','0',NULL,'融创钻石海岸',460200,'三亚市',460204,'天涯区',NULL,'0','清水湾赤岭风景区','0','0',NULL,'融创钻石海岸','0','1室2厅1厨1卫,2厅2厨5卫,2室2厅1厨2卫,3室2厅1厨2卫,7室6厅1厨8卫','0.00',NULL,'2015-01-01','2020-09-01',NULL,'(2014)陵房预字第(0028)号,陵房预字(2009)第15号,陵房预字(2017)第0075号,陵房预字(2017)第0082号,陵房预字(2018)第0063号,陵房预字(2018)第0064号,陵房预许字(2010)第0017号','陵水大溪地农旅业开发有限公司[房企申请入驻]',NULL,NULL,'348666','3490000','0','45%','0.38','板楼^低层^多层^小高层^','70','0','住宅类','743','北京凯莱物业有限公司','12.00',NULL,'629','0.00',NULL,'16栋',NULL,NULL,NULL,'0','别墅（私家车位、泳池）、瞰海公寓（高层）','交通|项目距离三亚市中心约35公里路程；距三亚凤凰机场约45公里路程；距离陵水高铁站约26公里路程。^幼儿园|英州镇海之星幼儿园、英州中心幼儿园^中小学|国际双语学校、赤岭小学、英州中心小学、英州军屯小学^综合商场|cdf免税购物中心、南果农贸市场、便利超市、洪都百货、海韵广场^医院|301解放军医院海南分院、陵水县人民医院、陵水县英州镇中心卫生院^银行|中国工商银行、中国邮政储蓄银行、中国建设银行、中国银行、英州信用社^其他|五星级酒店、赤岭的�D家鱼排、健身会房、SPA馆、业主会所、椰风海贝海滨餐厅、私家Imax影院等高端度假配套。^小区内部配套|幼儿园、社区居委会、医疗服务中心、信报箱、快递柜、超市、游泳池、会所、地下停车场','0','0','带装修','//img11.soufunimg.com/viewimage/house/2018_08/16/M05/0E/DD/ChCE4lt1RlCIVISUAAHG0p9Qm9UABEx1QA3hb0AAcbq033/1400x933.jpg',NULL,'0','0','0','待售','0','0','0','109.4970500','18.2568360','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','装修：精装修供水：市政供水供电：市政供电内墙：高级乳胶漆通讯：三线齐全','澳大利亚柏涛墨尔本建筑设计有限公司','海南楚湘建设工程有限公司',NULL,'融创钻石海岸价格待定|融创钻石海岸价格待定。物业费：12.00元/m²・月。带装修交房。融创钻石海岸位于：清水湾赤岭风景区。融创钻石海岸项目为低层,多层,小高层。融创钻石海岸容积率0.38。别墅（...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。带装修交付。绿化率45%。项目规划建设743户。融创钻石海岸位于：清水湾赤岭风景区。融创钻石海岸容积率0.38。物业费：12.00元/m²・月。融创钻石海岸...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。物业费：12.00元/m²・月。清水湾赤岭风景区。融创钻石海岸项目为低层,多层,小高层。共743户。融创钻石海岸容积率0.38。绿化率45%。带装修交付。项目...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。绿化率45%。物业费：一期别墅12元/平/月二期别墅8元/平/月公寓5.8元/平/月。带装修交房。项目规划建设743户。融创钻石海岸容积率0.38。项目为低层,多...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。项目为低层,多层,小高层板楼。共743户。清水湾赤岭风景区。容积率0.38。融创钻石海岸带装修交付。绿化率45%。物业费：12.00元/m²・月。别墅（私家车...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。容积率0.38。物业费：一期别墅12元/平/月二期别墅8元/平/月公寓5.8元/平/月。融创钻石海岸楼盘地址：清水湾赤岭风景区。绿化率45%。融创钻石海岸别墅...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。清水湾赤岭风景区。带装修交房。融创钻石海岸项目为低层,多层,小高层。物业费：12.00元/m²・月。融创钻石海岸项目距离三亚市中心约35公里路程；距三...[|^|交房时间：2020年9月30号|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。带装修交付。共743户。清水湾赤岭风景区。融创钻石海岸绿化率45%。融创钻石海岸项目为低层,多层,小高层。容积率0.38。项目距离三亚市...[|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。融创钻石海岸位于：清水湾赤岭风景区。项目为低层,多层,小高层板楼。容积率0.38。融创钻石海岸带装修交房。绿化率45%。物业费：一期...[|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。带装修交房。项目为低层,多层,小高层板楼。容积率0.38。融创钻石海岸项目规划建设743户。融创钻石海岸位于：清水湾赤岭风景区。物业...[|^|交房时间：2020年6月30号|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。带装修交房。绿化率45%。物业费：一期别墅12元/平/月二期别墅8元/平/月公寓5.8元/平/月。融创钻石海岸项目为低层,多层,小高层板楼。...[|^融创钻石海岸目前停售|融创钻石海岸目前停售，暂无加推房源信息！[|^融创钻石海岸价格待定|融创钻石海岸价格待定。带装修交房。融创钻石海岸楼盘地址：清水湾赤岭风景区。绿化率45%。2020年9月30号。融创钻石海岸别墅（私家车位、泳池）、瞰海公寓（高层）。项目距...[|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。带装修交付。项目规划建设743户。融创钻石海岸位于：清水湾赤岭风景区。融创钻石海岸项目为低层,多层,小高层。英州镇海之星幼儿园、...[|^融创钻石海岸价格待定|融创钻石海岸价格待定。物业费：一期别墅12元/平/月二期别墅8元/平/月公寓5.8元/平/月。带装修交房。绿化率45%。清水湾赤岭风景区。融创钻石海岸英州镇海之星幼儿园、英州中...[|^融创钻石海岸2020年9月30号|融创钻石海岸2020年9月30号。价格待定。共743户。带装修交房。绿化率45%。融创钻石海岸物业费：一期别墅12元/平/月二期别墅8元/平/月公寓5.8元/平/月。项目为低层,多层,小高...[|^融创钻石海岸目前暂无房源在售，后续加推待定。|融创钻石海岸目前暂无房源在售，后续加推待定。[|^融创钻石海岸目前暂无房源在售|融创钻石海岸目前暂无房源在售，后续加推待定。[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'74812432f4f7091e7707f2f34a9e1009','0',NULL,'海南恒大棋子湾',460200,'三亚市',460204,'天涯区',NULL,'0','昌江县海南棋子湾旅游度假区','0','0',NULL,'海南恒大棋子湾','0','3室2厅1卫,3室2厅1厨2卫,3室2厅2卫,4室2厅1厨2卫,4室2厅2卫,4室3厅1厨4卫,4室3厅4卫','0.00,229.00,240.00,299.00,321.00,323.00,336.00,345.00,586.00',NULL,'2016-05-01',NULL,NULL,'(2017)昌房预字(014)号,(2017)昌房预字(015)号,(2017)昌房预字(023)号,昌房预字第2017060号','昌江广亿房地产开发有限公司',NULL,NULL,'249894','200000','0','65%','0.80','板塔结合^低层^多层^高层^','40','0','住宅类','1658','海南恒大金碧物业','2.50',NULL,'445','0.00',NULL,'暂无资料',NULL,NULL,NULL,'0','18栋6层公寓，其中6、7、8、60、61、62、63号楼8梯28户；9、10、59号楼4梯28户；21栋3层花园洋房，69-81#楼；独栋别墅16套，11-18#，41-49#，108-114#；双拼别墅33-40#。双拼别墅16套，花园洋房126套，酒店式公寓1500套，总套数1658套。','交通|棋子湾旅游区内交通四通八达，海榆西线公路、环岛高速公路、粤海铁路贯穿全境，2015开通西线高铁也在棋子湾内设有一站。^中小学|无^综合商场|无^医院|无^其他|霸王岭国家森林自然保护区、四千亩海尾湿地公园，昌化古镇和昌化岭等优质的风景区。^小区内部配套|超市、游泳池、会所、露天停车位','0','0','带装修','//imgwcs2.soufunimg.com/viewimage/house/2019_04/15/7a5a37b0-a260-4eeb-abd2-fb0dccf3aca4/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.4827460','18.2956760','0','0',NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,'海南恒大棋子湾房源不多，欲购从速|海南恒大棋子湾目尾盘在售，价格约650万/套左右，房源不多，欲购从速，更多详情请咨询售楼处！布局大气从容，视野开阔，全阳光设计，挑高客厅，彰显雍容气质，客厅餐厅各成...[|^海南恒大棋子湾均价为：630万元/套|海南恒大棋子湾630万元/套。带装修交房。物业费：2.5元/m²・月。项目为低层,多层,高层板塔结合。昌江县海南棋子湾旅游度假区。海南恒大棋子湾主力户型为317平4居。霸王岭国...[|^海南恒大棋子湾在售价格为：630万元/套|海南恒大棋子湾630万元/套。海南恒大棋子湾位于：昌江县海南棋子湾旅游度假区。共1658户。容积率0.8。物业费：2.5元/m²・月。海南恒大棋子湾带装修交房。绿化率65%。18栋6层...[|^海南恒大棋子湾在售价格为：630万元/套|海南恒大棋子湾630万元/套。物业费：2.5元/m²・月。带装修交付。容积率0.8。绿化率65%。海南恒大棋子湾霸王岭国家森林自然保护区、四千亩海尾湿地公园，昌化古镇和昌化岭等...[|^海南恒大棋子湾均价为：630万元/套|海南恒大棋子湾630万元/套。绿化率65%。带装修交房。容积率0.8。海南恒大棋子湾位于：昌江县海南棋子湾旅游度假区。海南恒大棋子湾物业费：2.5元/m²・月。项目为低层,多层,...[|^海南恒大棋子湾在售价格为：630万元/套|海南恒大棋子湾630万元/套。海南恒大棋子湾项目为低层,多层,高层。带装修交付。主力户型为317平4居。海南恒大棋子湾楼盘地址：昌江县海南棋子湾旅游度假区。海南恒大棋子湾...[|^海南恒大棋子湾目尾盘在售|海南恒大棋子湾目尾盘在售，317平的独栋别墅，价格约630万/套左右，房源不多，欲购从速，更多详情请咨询售楼处！布局大气从容，视野开阔，全阳光设计，挑高客厅，彰显雍容气...[|^海南恒大棋子湾均价为：630万元/套|海南恒大棋子湾630万元/套。容积率0.8。带装修交付。海南恒大棋子湾项目为低层,多层,高层。海南恒大棋子湾位于：昌江县海南棋子湾旅游度假区。海南恒大棋子湾在售户型有：3...[|^|交房时间：2020-7-31|^海南恒大棋子湾均价为：630万元/套|海南恒大棋子湾630万元/套。物业费：2.5元/m²・月。绿化率65%。海南恒大棋子湾项目为低层,多层,高层。海南恒大棋子湾楼盘地址：昌江县海南棋子湾旅游度假区。海南恒大棋子湾...[|^海南恒大棋子湾均价为：630万元/套|海南恒大棋子湾630万元/套。物业费：公寓、别墅、洋房物业费2.5元/平・月。容积率0.8。绿化率65%。带装修交付。海南恒大棋子湾项目规划建设1658户。2020-7-31。昌江县海南棋...[|^海南恒大棋子湾清盘―3套独栋别墅|海南恒大棋子湾目前仅剩3套房源，317平的独栋别墅，价格在630万左右，预计明年7月底交房，房源不多，欲购从速，更多详情请咨询售楼处！[|^海南恒大棋子湾2020-7-31|海南恒大棋子湾2020-7-31。20000元/平方米起。主力户型为317平4居。物业费：公寓、别墅、洋房物业费2.5元/平・月。海南恒大棋子湾项目为低层,多层,高层。海南恒大棋子湾带装...[|^海南恒大棋子湾20000元/平方米|海南恒大棋子湾20000元/平方米起。2020-7-31。物业费：公寓、别墅、洋房物业费2.5元/平・月。项目规划建设1658户。项目为低层,多层,高层板塔结合。海南恒大棋子湾绿化率65%...[|^海南恒大棋子湾滨海低密美宅，单价20000/m²起|海南恒大棋子湾套内约317m²滨海低密美宅，单价20000/m²起。套内约317m²滨海低密美宅，堂前可植花草满庭；餐客厅大气宽敞，视野开阔；阳光清风不请自来；家庭厅还可变身影音...[|^海南恒大棋子湾套内约317m²滨海低密美宅|海南恒大棋子湾清尾特惠季，购房好时机，套内约317m²滨海低密美宅，仅余6席，单价20000/m²起。滨海低密美宅套内面积约317m²，布局大气从容，视野开阔，全阳光设计，挑高客厅...[|^海南恒大棋子湾独栋别墅在售|海南恒大棋子湾目前独栋别墅在售，套内面积约317m²，19500元/m²起，恒房通：一次性、按揭均享95折优惠。[|^海南恒大棋子湾沿袭全屋品牌装修标准|海南恒大棋子湾沿袭全屋品牌装修标准，臻选知名品牌装修材料，同时以装修空间实用、美观、健康为理念，精细制定厨房、卫生间、衣帽间、储藏空间等，使得每一个居住空间都精...[|^海南恒大棋子湾折后均价17000元/m²|海南恒大棋子湾目前主推花园洋房，建面约124-175m²，起价15173元/m²，带装修，折后均价17000元/m²。[|^海南恒大棋子湾主推花园洋房及别墅|海南恒大棋子湾首期总建面积20万m²，建筑呈“南高北低”的走势，依次向海岸线延伸，所有项目建筑高度均不超过20米，保证业主欣赏海景的视觉效果。目前主推花园洋房及别墅，...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'823c64d3cb9575182756fbfb5dac0098','0',NULL,'翔奥公馆',460200,'三亚市',460203,'吉阳区',NULL,'0','澄迈澄迈县金江镇文化北路县委大院内查看地图','0','0',NULL,'翔奥公馆','0','2厅1厨65卫,2室2厅1卫,3室2厅2卫,4室2厅2卫','0.00','9500.00',NULL,NULL,NULL,NULL,'暂无数据','暂无数据','暂无数据',NULL,NULL,'0',NULL,NULL,NULL,NULL,'0','住宅类',NULL,'暂无数据','1.50',NULL,NULL,'0.00',NULL,'暂无数据',NULL,NULL,NULL,'0',NULL,'暂无数据','0','0','暂无数据','https://t.focus-img.cn/sh520x390sh/xf/xc/e1e20130-a8c9-4e13-84fc-ba86a66c54e4.JPEG',NULL,'0','0','0','在售','0','0','0','109.5155160','18.2356110','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','暂无数据',NULL,NULL,NULL,'【翔奥公馆】目前价格9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-09-05^【翔奥公馆】目前价格9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-08-28^【翔奥公馆】目前价格9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-08-15^【翔奥公馆】目前价格9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-07-26^【翔奥公馆】在售户型建筑面积有66.56㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-06-14^【翔奥公馆】单价9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-05-24^【翔奥公馆】在售户型建筑面积有66.56㎡、64.58㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-04-25^【翔奥公馆】在售户型建筑面积有66.56㎡、64.58㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-03-15^【翔奥公馆】在售户型建筑面积有66.56㎡、64.58㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-03-01^【翔奥公馆】目前价格9500元/㎡|翔奥公馆项目目前价格9500元/㎡，在售户型建筑面积有66.56㎡、64.58㎡，在售户楼栋为1号楼。项目位于金江文明路金一KTV旁。以上信息采集于售楼处，实际销控信息以售楼处咨询为准。|2020-01-11',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'8368b9bde4a319f407b2f5034943aa01','0',NULL,'鑫源锦程',460200,'三亚市',460204,'天涯区',NULL,'0','文昌文城镇文清大道289-1号（市人民法院附近）查看地图','0','0',NULL,'文昌鑫源锦程,鑫源锦程','0','1厅1厨63卫,1室1厅1卫,1室2厅1卫,2室2厅1卫,3室2厅1卫,3室2厅2卫','0.00,19.00,20.00,22.00,26.00,27.00,35.00,36.00,37.00,50.00','12800.00',NULL,NULL,NULL,'(2010)文房预字(024)号,(2018)文房预字(029)号','武汉鑫源房地产开发有限公司','武汉鑫源房地产开发有限公司','暂无数据','26223','66300','0','40%','2.45','高层,板楼','70','0','住宅类','879','海南大祥物业管理有限公司','1.20',NULL,'278','0.00',NULL,'9',NULL,'35','3','0',NULL,'暂无数据','0','0','毛坯','https://t.focus-img.cn/sh520x390sh/xf/zxc/18dc2c18dbe9e84907c2060c72740523.jpg',NULL,'0','0','0','在售','0','0','0','109.2845360','18.3097320','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','毛坯房宽带：入户市政供水',NULL,NULL,NULL,'【鑫源锦程】均价为：12800元/平米带装修交付|鑫源锦程一至三房在售，一房建面为62.98m²，二房建面为80.37m²、82.51m²，三房建面为111.86m²。均价12800元/平方米，带装修交付。一次性95折，按揭97折。现房，即买即可买家电入住。更多详情可致电售楼处。|2021-02-23^【鑫源锦程】单价约10492-10630元/平|文昌鑫源锦程位于海南省文昌市文清大道北侧，北面与西面均临规划道路，属于新区核心区域内，鑫源锦程总用地面26223.34平方米，规划总建筑面积66414平方米，容积率2.45，绿化率40%，本项目由6栋18层高层住宅组成。【在售房源】住宅【房源数量】5套【户型面积】建面80.61-82.42平米两房，建面113.36平米三房【房源价格】单价约10492-10630元/平，总价84.6-118万/套|2020-07-30^【鑫源锦程】单价约10492-10630元/平|文昌鑫源锦程位于海南省文昌市文清大道北侧，北面与西面均临规划道路，属于新区核心区域内，鑫源锦程总用地面26223.34平方米，规划总建筑面积66414平方米，容积率2.45，绿化率40%，本项目由6栋18层高层住宅组成。【在售房源】住宅【房源数量】5套【户型面积】建面80.61-82.42平米两房，建面113.36平米三房【房源价格】单价约10492-10630元/平，总价84.6-118万/套|2020-06-13^【鑫源锦程】建面80.61-82.42平米两房|文昌鑫源锦程位于海南省文昌市文清大道北侧，北面与西面均临规划道路，属于新区核心区域内，鑫源锦程总用地面26223.34平方米，规划总建筑面积66414平方米，容积率2.45，绿化率40%，本项目由6栋18层高层住宅组成。【在售房源】住宅【房源数量】5套【户型面积】建面80.61-82.42平米两房，建面113.36平米三房【房源价格】单价约10492-10630元/平，总价84.6-118万/套|2020-05-05^【鑫源锦程】总用地面26223.34平方米|文昌鑫源锦程位于海南省文昌市文清大道北侧，北面与西面均临规划道路，属于新区核心区域内，鑫源锦程总用地面26223.34平方米，规划总建筑面积66414平方米，容积率2.45，绿化率40%，本项目由6栋18层高层住宅组成。|2020-01-09^【鑫源锦程】推出5套房源在售总价84.6-118万/套|文昌鑫源锦程位于海南省文昌市文清大道北侧，北面与西面均临规划道路，属于新区核心区域内，鑫源锦程总用地面26223.34平方米，规划总建筑面积66414平方米，容积率2.45，绿化率40%，本项目由6栋18层高层住宅组成。【在售房源】住宅【房源数量】5套【户型面积】建面80.61-82.42平米两房，建面113.36平米三房【房源价格】单价约10492-10630元/平，总价84.6-118万/套|2019-12-22^【鑫源锦程】均价11000元/平米|鑫源锦程项目在售房源：在售户型为建筑面积约62平一房，建筑面积约80-82平两房，建筑面积113平温馨三房，均价11000元/平米。|2019-10-06^【鑫源锦程】有房源在售，均价12800元/平|项目有房源在售，均价12800元/平|2019-03-30^【鑫源锦程】所有房源已售罄！|鑫源锦程所有房源已售罄!|2017-05-12^【鑫源锦程】文昌交通便利盘8楼以上5700元/平|鑫源锦程在售户型是40-107平1-3室,8楼以下5600元/平,8楼以上5700元/平,无层差补。工程进度:现房。|2017-04-20',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'92be1f62a01e75b651e4bb71ff01a64e','0',NULL,'石梅山庄',460200,'三亚市',460204,'天涯区',NULL,'0','兴隆石梅湾兴梅大道1号石梅山庄','0','0',NULL,'石梅山庄','0','1厅1厨1卫,1室1厅1厨1卫,1室1厨1卫,1室2厅1厨1卫,2室1厅1厨1卫,2室1厨2卫,2室2厅1厨1卫,2室2厅1厨2卫','0.00','22000.00','2018-07-02','2019-11-28',NULL,'(2009)万房预字(027)号,(2009)万房预字(05)号,(2010)万房预字(04)号,(2012)万房预字(延24)号,(2017)万房预字(60)号,(2018)万房预字(22)号','海南嘉地置业有限公司',NULL,NULL,'580000','400000','0','60%','0.30','板楼^低层^','70','0','住宅类','4867','石梅山庄物业管理有限公司','2.80',NULL,'0','0.00',NULL,'143栋',NULL,NULL,NULL,'0','A区占地329.03亩，由47栋中高层的住宅及一栋综合楼组成；建筑面积为324970.53平方米；B区占地362.83亩，由297栋低层别墅及一栋公建养生馆组成，建筑面积为65601.56平方米；C区占地115.19亩，由94栋低层别墅及一栋公建商业街组成，四期共60栋5-6层洋房，其中板房15栋。','交通|东线高速公路石梅下道，兴隆方向3公里处，东线高速、高铁便捷抵达三亚凤凰机场、博鳌国际商务、海口美兰机场。三亚到兴隆班车在石梅山庄下^中小学|海南省兴隆华侨农场中学^综合商场|兴隆第一百货、兴隆肥二超市^医院|海南兴隆红十字医院^银行|中国银行、中国建设银行^邮政|万宁市兴隆镇邮局^小区内部配套|内部配套：养生馆、商业街','0','0','带装修','//img11.soufunimg.com/viewimage/house/2018_03/28/M00/1B/76/ChCE4lq7EWuIKnFCAAIUTb483GgABAg-gG6RtAAAhRl376/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.4958610','18.2604380','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','装修：精装修通讯：三线齐全供水：市政供水门窗：高级铝合金供电：市政供电内墙：高档涂料',NULL,NULL,NULL,'石梅山庄170万/套起，均价约22000元/平方米|石梅山庄现房在售，精装，即买即住！170万/套起，均价约22000元/平方米，76-98平的两房，138和143平的3房，172平的4房 。石梅山庄规划面积5000亩，建设用地2800亩，综...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。物业费：2.8元/m²・月。兴隆石梅湾兴梅大道1号石梅山庄。石梅山庄项目为低层。在售户型有：76.06平-142.78平2居。石梅山庄带装修交房。容积率0.3。...[|^石梅山庄在售价格为：22000元/平方米|石梅山庄22000元/平方米。主力户型为76.06平-142.78平2居。容积率0.3。物业费：2.8元/m²・月。带装修交付。石梅山庄项目规划建设4867户。石梅山庄位于：兴隆石梅湾兴梅大道...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。共4867户。石梅山庄位于：兴隆石梅湾兴梅大道1号石梅山庄。在售户型有：76.06平-142.78平2居。容积率0.3。石梅山庄项目为低层板楼。带装修交付。绿...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。共4867户。石梅山庄楼盘地址：兴隆石梅湾兴梅大道1号石梅山庄。石梅山庄项目为低层。容积率0.3。石梅山庄物业费：住宅2.8元/平方米・月，别墅5.8元...[|^石梅山庄在售价格为：22000元/平方米|石梅山庄22000元/平方米。兴隆石梅湾兴梅大道1号石梅山庄。容积率0.3。物业费：2.8元/m²・月。主力户型为76.06平-142.78平2居。石梅山庄项目为低层板楼。带装修交付。海南省...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。绿化率60%。项目为低层板楼。主力户型为76.06平-142.78平2居。物业费：住宅2.8元/平方米・月，别墅5.8元/平方米・月。石梅山庄东线高速公路石梅下...[|^石梅山庄在售价格为：22000元/平方米|石梅山庄22000元/平方米。石梅山庄项目为低层。带装修交付。物业费：2.8元/m²・月。项目规划建设4867户。石梅山庄绿化率60%。容积率0.3。兴隆石梅湾兴梅大道1号石梅山庄。东...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。石梅山庄位于：兴隆石梅湾兴梅大道1号石梅山庄。容积率0.3。绿化率60%。带装修交房。石梅山庄项目共75栋楼,A区占地329.03亩，由47栋中高层的住宅及...[|^石梅山庄在售价格为：22000元/平方米|石梅山庄22000元/平方米。项目规划建设4867户。带装修交房。石梅山庄位于：兴隆石梅湾兴梅大道1号石梅山庄。物业费：住宅2.8元/平方米・月，别墅5.8元/平方米・月。石梅山庄...[|^石梅山庄起价1.88万/平方米|石梅山庄目前均价2.2万/平方米，起价1.88万/平方米76-98平的两房，138和143平的3房，172平的4房目前在售都是现房，即买即住，欢迎您到现场品鉴！滨海旅游公路[|^石梅山庄在售价格为：22000元/平方米|石梅山庄22000元/平方米。带装修交付。物业费：2.8元/m²・月。项目为低层板楼。容积率0.3。石梅山庄项目共75栋楼,A区占地329.03亩，由47栋中高层的住宅及一栋综合楼组成；建...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。容积率0.3。共4867户。在售户型有：76.06平-142.78平2居。项目为低层板楼。石梅山庄绿化率60%。带装修交房。东线高速公路石梅下道，兴隆方向3公里...[|^石梅山庄项目三面环山，一面与海湾相邻|石梅山庄项目规划占地5000亩，具有山、林、湖、海、温泉的大型养生度假社区。整个项目三面环山，一面与海湾相邻，原始森林覆盖率达50%，空气中负离子的含量高达8万个/立方厘...[|^石梅山庄均价为：22000元/平方米|石梅山庄22000元/平方米。带装修交付。容积率0.3。在售户型有：76.06平-142.78平2居。物业费：2.8元/m²・月。石梅山庄海南省兴隆华侨农场中学,兴隆第一百货、兴隆肥二超市,...[|^石梅山庄四期洋房在售，主力户型建面约63-119m²|石梅山庄项目四期洋房在售，主力户型建面约63-119m²，含3000元/m²装修标准，均价22000元/m²，一次性付款9折，分期（三个月享92折，半年享94折，一年无折扣）。[|^石梅山庄0.3的容积率和高达60%绿化率|石梅山庄地处兴隆三大国际旅游度假圈，规划面积5000亩，0.3的容积率和高达60%绿化率，生活在石梅山庄，除了得天独厚的自然环境外，其配套设施日益完善，服务水平也将不断提...[|^|交房时间：四期2-5#、9#、29#预计2019年11月28日交房|^石梅山庄均价为：24000元/平方米|石梅山庄24000元/平方米。四期2-5#、9#、29#预计2019年11月28日交房。非毛坯交房。绿化率60%。石梅山庄位于：兴隆石梅湾兴梅大道1号石梅山庄。石梅山庄主力户型为76.06平-1...[|^石梅山庄项目三面环山，一面与海湾相邻|石梅山庄素称是海南具有山、林、湖、海、温泉的大型度假社区。整个项目三面环山，一面与海湾相邻，原始森林覆盖率达50%，空气中负离子的含量高达8万个/立方厘米，可以说是一...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL);
INSERT INTO dwb_db.a_dwb_newest_info (main_id,newest_id,platform,url,newest_name,city_id,city_name,county_id,county_name,block_name,ori_lnglat,address,sale_address,sale_phone,ori_alias,alias,former_name,layout,total_price,unit_price,recent_opening_time,recent_delivery_time,issue_date,issue_number,developer,investor,brander,land_area,building_area,arch_style,green_rate,volume_rate,building_type,right_term,property_type,property_sub,household_num,property_comp,property_fee,park_rate,park_num,park_price,orientation,building_num,avg_distance,ong_park_num,ung_park_num,build_density,floor_num,ambitus,bike_park_number,agent,decoration,layout_pic,estate_pic,advantage,disadvantages,pay_meth,sale_status,gd_city,gd_county,gd_lnglat,gd_lng,gd_lat,gd_office_name,is_accurate,heat_mode,power_mode,water_mode,ring_locate,`character`,deco_level,mate_equip,designer,builder,progress,provide_sche,land_time,create_time,update_time,r_id,remark,alias_json,flag) VALUES
	 (NULL,'aec483080e461a39a4711e8075e438df','0',NULL,'椰林润达御园',460200,'三亚市',460204,'天涯区',NULL,'0','陵水陵水县椰林北大道197号椰林派出所旁查看地图','0','0',NULL,'椰林润达御园','0','1厅1厨61卫,1室1厅1卫,2室2厅1卫','0.00,108.00,159.00','18000.00',NULL,NULL,NULL,NULL,'暂无数据','暂无数据','暂无数据','26000','66000','0','39%','2.30','高层,独栋','70','0','住宅类',NULL,'暂无数据',NULL,NULL,'365','0.00',NULL,'5',NULL,NULL,'365','0',NULL,'大型购物中心，高铁站、公交车站，客运站，体育场，公园，大型医院','0','0','全装修','https://t.focus-img.cn/sh520x390sh/xf/xc/d64afb0e-ab3a-474f-b46c-58e9d9584c2a.JPEG',NULL,'0','0','0','在售','0','0','0','109.5016220','18.2456130','0','0','暂无数据',NULL,NULL,NULL,NULL,'2500.0元/平米','暂无数据',NULL,NULL,NULL,'【椰林润达御园】在售户型：61平一房两厅|椰林润达御园项目目前在售户型：61平一房两厅，85平～91平两房两厅和111平三房两厅，全款97折。更多详情请咨询400免费电话.|2020-07-21^【椰林润达御园】在售户型建筑面积有：88㎡、111㎡|目目前在售均价22000元/㎡，在售户型建筑面积有：88㎡（二居）、111㎡（三居），全款97折。更多详情请咨询400免费电话.|2020-07-13^【椰林润达御园】项目目前在售均价20000元/㎡|椰林润达御园项目目前在售均价20000元/㎡，在售户型建筑面积有：88㎡（二居）、111㎡（三居），全款97折。更多详情请咨询400免费电话.|2020-07-03^【椰林润达御园】两房朝南均价19500-23000/㎡|椰林润达御园项目目前在售户型：61平一房两厅，85平～91平两房两厅和111平三房两厅，全款97折。更多详情请咨询400免费电话.|2020-06-24^【椰林润达御园】主推建筑面积61㎡一房、85-91㎡两房|椰林润达御园项目目前在售均价14000元/㎡，在售户型建筑面积有：88㎡（二居）、111㎡（三居），全款97折。更多详情请咨询400免费电话.|2020-06-16^【椰林润达御园】楼盘均价14000元/平方米|占地面积：2.6万平方米建筑面积:6.6万平方米容积率:2.3绿化率:39%产权年限:70年，更多详情请咨询400免费电话.|2020-06-08^【椰林润达御园】优惠总价80万/套起|目前在售户型有主推建筑面积61㎡一房、85-91㎡两房、111㎡三房，均价14000元/㎡，优惠总价80万/套起，更多详情请咨询400免费电话.|2020-06-01^【椰林润达御园】在售建面61-111㎡三房|在售建面61㎡一房、85-91㎡两房、111㎡三房，均价14000元/㎡，总价80万/套起。更多详情请咨询400免费电话.|2020-05-25^【椰林润达御园】建面61-111㎡三房总价80万/套起|椰林润达御园在售建面61㎡一房、85-91㎡两房、111㎡三房，均价14000元/㎡，总价80万/套起。更多详情请咨询400免费电话.|2020-05-15^【椰林润达御园】仅需84万/套起|户型南北通透、生活交通十分方便，目前推出建筑面积为60㎡1房、88㎡2房、109㎡3房养老住宅，更多详情请咨询400免费电话.|2020-05-06',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'e0c4fc6a92e27c04e859e44684ec53b7','0',NULL,'中和龙沐湾-海润源',460200,'三亚市',460203,'吉阳区',NULL,'0','龙沐湾国际度假区','0','0',NULL,'中和龙沐湾-海润源,中和龙沐湾・海润源','0','1厨1卫,1室1厅1厨1卫,1室2厅1厨1卫,2室2厅1厨1卫,3室2厅1厨2卫,4室2厅1厨2卫','0.00',NULL,'2018-02-22','2019-12-31',NULL,'(2015)乐房预证字(0003)号,乐房售证字(2014)008号,乐房售证字(2014)011号,乐房售证字(2015)003号','海南中和（集团）有限公司',NULL,NULL,'268134','350000','0','58.2%','1.10','板楼^高层^','70','0','住宅类','3500','海南和坤物业管理有限公司','3.00',NULL,'2359','0.00',NULL,'14栋',NULL,NULL,NULL,'0','1、6、7、8、10、11、15、16、17为18层,2、3为19层；9、12为16层；一期海润源共14栋楼','交通|三亚到乐东班车在龙沐湾景区下^中小学|尖峰中学^综合商场|暂无^医院|暂无^小区内部配套|小区内部配套：养生会馆、温泉泡池、蓝球场、网球场、健身场，儿童游乐场。','0','0','带装修','//imgs.soufunimg.com/viewimage/house/2016_06/17/sanya/1466148034402_000/1400x933.jpg',NULL,'0','0','0','待售','0','0','0','109.6112150','18.1892470','0','0','暂无数据',NULL,NULL,NULL,NULL,'2000.0元/平米','结构：框架（钢筋混泥土）通讯：三线齐全外墙：白色涂料供水系统：市政供水暖气：无',NULL,NULL,NULL,'中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。绿化率58.2%。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。项目为高层板楼。物业费：3元/m²・月。中和龙沐湾・海润源共3500户。项目共14栋...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。物业费：3元/m²・月。共3500户。绿化率58.2%。中和龙沐湾・海润源位于：龙沐湾国际度假区。中和龙沐湾・海润源带装修交付。项目为高层板楼。容...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。共3500户。容积率1.1。龙沐湾国际度假区。带装修交房。中和龙沐湾・海润源尖峰中学,暂无,暂无。三亚到乐东班车在龙沐湾景区下。中和龙沐湾・海...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。物业费：3元/m²・月。容积率1.1。带装修交房。中和龙沐湾・海润源三亚到乐东班车在龙沐湾景区...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。带装修交房。绿化率58.2%。容积率1.1。物业费：3元/m²・月。中和龙沐湾・海润源共3500户。中和龙沐湾・海润源项目为高层。项目共14栋楼,1、6、...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。带装修交付。容积率1.1。物业费：3元/m²・月。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。中和龙沐湾・海润源三亚到乐东班车在龙沐湾景区...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。绿化率58.2%。物业费：3元/m²・月。中和龙沐湾・海润源位于：龙沐湾国际度假区。带装修交房。中和龙沐湾・海润源项目规划建设3500户。三亚到乐...[|^中和龙沐湾・海润源项目目前待售|中和龙沐湾・海润源项目目前待售，更多详情咨询售楼处。总面积占地3267亩，容积率1.1，绿化率近60%，项目一期海润源总占地面积402亩，总建筑面积355701.91m²[|^中和龙沐湾・海润源均价为：18000元/平方米|中和龙沐湾・海润源18000元/平方米。容积率1.1。中和龙沐湾・海润源项目为高层。物业费：3元/m²・月。中和龙沐湾・海润源位于：龙沐湾国际度假区。中和龙沐湾・海润源三亚到...[|^中和龙沐湾・海润源目前均价1.8万/m²|中和龙沐湾・海润源项目总面积占地3267亩，容积率1.1，绿化率近60%，我们拥有明媚的阳光、纯净的空气、碧蓝的海水、洁白的沙滩，自然生态环境在首屈一指，是绝佳的度假养生...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。容积率1.1。共3500户。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。带装修交房。中和龙沐湾・海润源物业费：3元/m²・月。绿化率58.2%。项...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。中和龙沐湾・海润源项目为高层。物业费：3元/m²・月。绿化率58.2%。带装修交付。中和龙沐湾・海润源容积率1.1。项目规划建设3500户。中和龙沐...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。容积率1.1。中和龙沐湾・海润源项目为高层。物业费：3元/m²・月。项目规划建设3500户。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。绿化率...[|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。中和龙沐湾・海润源项目为高层。项目规划建设3500户。绿化率58.2%。带装修交付。中和龙沐湾・海润源尖峰中学,暂无,暂无。三亚到乐东班车在龙沐...[|^|交房时间：2019年12月31日交房8#|^中和龙沐湾・海润源价格待定|中和龙沐湾・海润源价格待定。中和龙沐湾・海润源楼盘地址：龙沐湾国际度假区。项目规划建设3500户。绿化率58.2%。容积率1.1。中和龙沐湾・海润源尖峰中学,暂无,暂无。1、6...[|^中和龙沐湾・海润源打造多元化成熟浪漫度假大盘|中和龙沐湾・海润源社区，集品质、创新、养生、度假于一体，用金钥匙打开海南旅游地产的大门，倾心打造具特色的多元化成熟浪漫度假大盘，塑造休闲度假的新理念！直面美丽海...[|^中和龙沐湾・海润源2019年12月31日交房8#|中和龙沐湾・海润源2019年12月31日交房8#。价格待定。容积率1.1。中和龙沐湾・海润源项目为高层。绿化率58.2%。中和龙沐湾・海润源非毛坯交房。物业费：3元/m²・月。三亚到...[|^中和龙沐湾・海润源2019年12月31日交房8#|中和龙沐湾・海润源2019年12月31日交房8#。价格待定。绿化率58.2%。项目规划建设3500户。非毛坯交付。中和龙沐湾・海润源项目为高层板楼。中和龙沐湾・海润源位于：龙沐湾国...[|^中和龙沐湾・海润源新品加推等待通知。|中和龙沐湾・海润源项目价格待定，新品加推等待通知。中和龙沐湾一期“海润源”占地面积402亩,建筑面积35万m²,由2栋19F、10栋18F、2栋16F高层住宅及滨海商业街.会所、地下停...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'e8855c11a499b39f99ae1079310cc632','0',NULL,'长岛蓝湾',460200,'三亚市',460203,'吉阳区',NULL,'0','临高角旅游风景区','0','0',NULL,'临高长岛蓝湾小镇海南长岛蓝湾小镇,长岛,长岛蓝湾','0','1厅1厨5卫,1室1厅1厨1卫,1室2厅1厨1卫,2室2厅1厨1卫','0.00','13000.00','2018-07-13','2019-09-30',NULL,'(2013)临房预字第(28)号,(2013)临房预字第(7)号,(2017)临房预字(025)号,(2017)临房预字(031)号,(2018)临房预字(42)号,(2018)临房预字(43)号,(2018)临房预字(45)号,(2018)临房预字(58)号,(2018)临房预字(59)号,(2018)临房预字(64)号,(2018)临房预字(68)号,临房预许字(2013)28号','海南名角实业开发有限公司[房企申请入驻]',NULL,NULL,'231137.27','326981.47','0','60.1%','1.27','塔楼^低层^高层^','70','0','住宅类','13000','海南三原华庭物业管理有限公司','2.00',NULL,'1215','0.00',NULL,'19栋',NULL,'427','788','0','一期：1#、9#26层11#、17#28层18#、20#22层。','交通|未来长岛蓝湾小区将有免费巴士方便业主出行（长岛蓝湾站），项目距离临高县城10公里，距离海口市区80公里。距离西线高速公路出口15公里，经西线高速可通达各地。距离规划中的环岛西线快速轨道交通站15公里。^中小学|临城第三小学（县一级学校），临高中学，临高第二中学，临高县党校（中等职业技术学校）等^综合商场|百货大楼，福临广场农贸市场，东门市场，宝真超市，万家惠超市^医院|临高县人民医院（临高县人民医院、卫生防疫站、急救中心），妇产科医院，临高文澜江卫生中心，临城康民药店，广安堂药店等^银行|农村信用合作社，农业银行，中国人民银行^小区内部配套|超市、健身设施、儿童游乐场、游泳池、老年文化中心','0','0','带装修','//imgs.soufunimg.com/viewimage/house/2013_10/11/sanya/1381462674253_000/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.5146620','18.2197890','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','含1600元/平精装宽带：入户供水：市政供水电梯服务户数：3梯12户',NULL,NULL,NULL,'长岛蓝湾均价为：13000元/平方米|长岛蓝湾13000元/平方米。长岛蓝湾楼盘地址：临高角旅游风景区。绿化率60.11%。容积率1.27。主力户型为44.37平-78.99平1居-2居。长岛蓝湾物业费：2元/㎡·月。带装修交房。...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。物业费：2元/㎡·月。共13000户。长岛蓝湾项目为低层,高层。临高角旅游风景区。长岛蓝湾项目共19栋楼,一期：1#、9#26层11#、17#28层18#、20#22层...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。物业费：2元/㎡·月。绿化率60.11%。带装修交房。主力户型为44.37平-78.99平1居-2居。长岛蓝湾共13000户。项目共19栋楼,一期：1#、9#26层11#、17...[|^长岛蓝湾均价为：13000元/平方米|长岛蓝湾13000元/平方米。容积率1.27。物业费：住宅2元/平方米.月。临高角旅游风景区。在售户型有：44.37平-78.99平1居-2居。长岛蓝湾项目规划建设13000户。绿化率60.11%。...[|^长岛蓝湾均价为：13000元/平方米|长岛蓝湾13000元/平方米。带装修交房。临高角旅游风景区。长岛蓝湾项目为低层,高层。绿化率60.11%。长岛蓝湾物业费：住宅2元/平方米.月。容积率1.27。主力户型为44.37平-78...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。主力户型为44.37平-78.99平1居-2居。共13000户。临高角旅游风景区。项目为低层,高层塔楼。长岛蓝湾物业费：2元/㎡·月。临城第三小学（县一级学校...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。带装修交房。容积率1.27。临高角旅游风景区。项目为低层,高层塔楼。长岛蓝湾绿化率60.11%。共13000户。在售户型有：44.37平-78.99平1居-2居。一期...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。带装修交房。主力户型为44.37平-78.99平1居-2居。绿化率60.11%。项目为低层,高层塔楼。长岛蓝湾物业费：住宅2元/平方米.月。一期：1#、9#26层11#...[|^长岛蓝湾均价为：13000元/平方米|长岛蓝湾13000元/平方米。项目为低层,高层塔楼。临高角旅游风景区。在售户型有：44.37平-78.99平1居-2居。容积率1.27。长岛蓝湾项目共19栋楼,一期：1#、9#26层11#、17#28层...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。物业费：住宅2元/平方米.月。共5728户。容积率1.27。带装修交付。长岛蓝湾主力户型为44.37平-78.99平1居-2居。未来长岛蓝湾小区将有免费巴士方便业...[|^长岛蓝湾在售均价1.3万/平方米|长岛蓝湾项目距离临高县城10公里，距离海口市区80公里。距离西线高速公路出口15公里，经西线高速可通达各地。距离规划中的环岛西线快速轨道交通站15公里,未来长岛蓝湾小区将...[|^长岛蓝湾在售价格为：12500元/平方米|长岛蓝湾12500元/平方米。容积率1.27。带装修交房。项目规划建设5728户。长岛蓝湾楼盘地址：临高角旅游风景区。长岛蓝湾项目为低层,高层塔楼。绿化率60.11%。主力户型为44....[|^长岛蓝湾在售价格为：12500元/平方米|长岛蓝湾12500元/平方米。绿化率60.11%。长岛蓝湾项目为低层,高层。项目规划建设5728户。容积率1.27。长岛蓝湾物业费：2元/㎡·月。主力户型为44.37平-78.99平1居-2居。带装...[|^长岛蓝湾整体均价12500元/㎡|长岛蓝湾一期、二期同时在售，均价11000—14000元/㎡，整体均价12500元/㎡，全款、按揭均无折扣。一期建面44—56㎡一房，含1600元/㎡装修标准；二期建面48—78㎡，含1600元...[|^长岛蓝湾在售价格为：13000元/平方米|长岛蓝湾13000元/平方米。主力户型为44.37平-78.99平1居-2居。带装修交付。共5728户。绿化率60.11%。长岛蓝湾楼盘地址：临高角旅游风景区。容积率1.27。项目共19栋楼,一期：...[|^长岛蓝湾项目一期、二期同时在售|长岛蓝湾项目一期、二期同时在售，一期建面44—56㎡一房，含1600元/㎡装修标准，均价14000—15000元/㎡，一次性付款8折；二期折后均价13000元/㎡，总价48万起，建面48—78㎡...[|^长岛蓝湾品质精装公寓即买即入住|长岛蓝湾拥有4公里的海岸线，双湾海滩，直伸入海的海礁石，日出日落美景，更有7200㎡滨海泳池，12万㎡园林水系。它以100万平米的规模，是一个集度假养生、休闲娱乐、居住于...[|^长岛蓝湾在售价格为：14000元/平方米|长岛蓝湾14000元/平方米。主力户型为44.37平-78.99平1居-2居。长岛蓝湾楼盘地址：临高角旅游风景区。容积率1.27。非毛坯交房。长岛蓝湾项目规划建设5728户。项目共19栋楼,一...[|^|交房时间：预计2019年9月30日二期1#、2#、13#、14#交房|^长岛蓝湾全线现楼在售|长岛蓝湾全线现楼在售，品质精装公寓即买即入住，在售户型建筑面积为44-78㎡，价格14000-18000元/㎡，详情请咨询售楼处。长岛蓝湾占地约1100亩，总建面约100万㎡，项目整体...[|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'f430a3f50ceb631d8d13d5e0bdb98edd','0',NULL,'碧桂园金沙滩',460200,'三亚市',460202,'海棠区',NULL,'0','西线高速金牌出口龙波湾度假区','0','0',NULL,'碧桂园-金沙滩,碧桂园·金沙滩,碧桂园金沙滩,金沙滩','0','1室1厅1厨1卫,2厅2厨5卫,2室2厅1厨1卫,3室2厅1厨2卫','0.00','12000.00','2019-12-15','2021-11-15',NULL,'(2012)临房预字(26)号,(2013)临房预字(1)号,(2013)临房预字(2)号,(2016)临房预字(03)号,(2016)临房预字(09)号,(2017)临房预字(10)号,(2018)临房预字(54)号,(2019)临房预字(41)号,(2019)临房预字(41)号(2019)临房预字(47)号,(2019)临房预字(47)号,(2020)临房预字(03)号,(2020)临房预字(30)号','海南临高碧桂园方圆房地产开发有限公司',NULL,NULL,'258796','170968','0','40%','0.60','板塔结合^高层^','70','0','住宅类','2353','广东碧桂园物业管理有限公司文昌分公司','3.00',NULL,'423','0.00',NULL,'暂无资料',NULL,NULL,NULL,'0','洋房23层','交通|环岛西线—金牌出口—金澜大道^综合商场|咖啡体验馆、儿童游乐区、休闲书吧、社区便利店^医院|社区医院，临高医院^银行|工商银行，农业银行^其他|温泉会所，高尔夫，公园^小区内部配套|五星级金沙滩度假酒店、一线海景温泉、星级沙滩运动俱乐部、观海温泉养生会馆、夏威夷风情商业街、一线海景无边际泳池','0','0','带装修','//imgs.soufunimg.com/viewimage/house/2012_10/11/hainan/1349922625410_000/1400x933.jpg',NULL,'0','0','0','在售','0','0','0','109.7899050','18.4078500','0','0','暂无数据',NULL,NULL,NULL,NULL,'暂无数据','宽带：入户供水：市政供水电梯服务户数：2梯14户、2梯8户',NULL,NULL,NULL,'|交房时间：预计2021年11月15日三期17#交房|^碧桂园金沙滩均价约12000元/㎡|碧桂园金沙滩有瞰海和不瞰海朝向，其中可瞰海朝向建面约64-66㎡两房两厅一卫，均价约12000元/㎡；不瞰海建面约88-104㎡，均价11000元/㎡，精装修硬装交付不包括家装家电。更...[|^碧桂园金沙滩可瞰海朝向建面约64-66㎡在售|碧桂园金沙滩有瞰海和不瞰海朝向，其中可瞰海朝向建面约64-66㎡，均价13000-15000元/㎡，整体均价14000元/㎡；不瞰海建面约88-104㎡，均价11000元/㎡，精装修硬装交付不包括...[|^碧桂园金沙滩精装修硬装交付不包括家装家电|碧桂园金沙滩有瞰海和不瞰海朝向，其中可瞰海朝向建面约64-66㎡，均价13000-15000元/㎡，整体均价14000元/㎡；不瞰海建面约88-104㎡，均价11000元/㎡，精装修硬装交付不包括...[|^碧桂园金沙滩在售价格为：11000元/平方米|碧桂园金沙滩11000元/平方米。碧桂园金沙滩项目为高层。共2353户。物业费：3.0元/㎡·月。容积率0.6。碧桂园金沙滩带装修交房。碧桂园金沙滩楼盘地址：西线高速金牌出口龙波...[|^碧桂园金沙滩均价为：11000元/平方米|碧桂园金沙滩11000元/平方米。物业费：别墅3.5元/月/平米，公寓3.0元/月/平米，洋房2.0元/月/平米。带装修交付。容积率0.6。绿化率40%。碧桂园金沙滩西线高速金牌出口龙波湾...[|^碧桂园金沙滩新品加推在即|碧桂园金沙滩有瞰海和不瞰海朝向，其中可瞰海朝向建面约64-66㎡，均价14000-15000元/㎡，整体均价14500元/㎡；不瞰海建面约88-104㎡，均价11000元/㎡。更多详情可线上咨询置...[|^碧桂园金沙滩在售价格为：10500元/平方米|碧桂园金沙滩10500元/平方米。预计2021年11月15日三期17#交房。主力户型为104平3居。项目为高层板塔结合。容积率0.6。碧桂园金沙滩物业费：3.0元/㎡·月。绿化率40%。西线高...[|^|交房时间：预计2020年5月三期13#交房|^碧桂园金沙滩在售均价10500元/㎡|碧桂园金沙滩目前在售均价10500元/㎡，现16#、17#洋房在售，104平3房，当天全款可享93折，一个月内付清96折。碧桂园金沙滩酒店以“拥抱自然，拥抱海洋”为规划主题，结合海...[|^碧桂园金沙滩均价为：10000元/平方米|碧桂园金沙滩10000元/平方米。绿化率40%。项目为高层板塔结合。预计2021年11月15日三期17#交房。物业费：别墅3.5元/月/平米，公寓3.0元/月/平米，洋房2.0元/月/平米。碧桂园...[|^碧桂园金沙滩均价在10000-11500元/㎡|碧桂园金沙滩16#和17#在售，建面约104㎡，均价在10000-11500元/㎡，当天全款定房可享93折，一个月内付清全款96折，主体已恢复施工，建至10层。[|^碧桂园金沙滩在售价格为：9500元/平方米|碧桂园金沙滩9500元/平方米。碧桂园金沙滩楼盘地址：西线高速金牌出口龙波湾度假区。主力户型为104平3居。物业费：别墅3.5元/月/平米，公寓3.0元/月/平米，洋房2.0元/月/平...[|^碧桂园金沙滩温泉度假大城，配套成熟|碧桂园金沙滩海岸温泉度假大城，配套成熟，目前洋房在售，建面约104㎡通透三房，均价在8500-9500元/㎡，当天全款定房可享93折，一个月内付清全款96折。[|^碧桂园金沙滩洋房在售，建面约104㎡通透三房|碧桂园金沙滩海岸温泉度假大城，配套成熟，目前洋房在售，建面约104㎡通透三房，均价10000-11000元/㎡，新春特惠88折，更多详情请咨询售楼处。[|^碧桂园金沙滩目前在售楼栋三期17#|碧桂园金沙滩目前在售楼栋三期17#，已经取得预售证，在售建面约102㎡三房两厅两卫，均价10000元/㎡，97折优惠，预计2021年11月15交付。更多详情请电话咨询。[|^碧桂园金沙滩在售户型YJ100|碧桂园金沙滩以自然为中心，回归自然，临海，望海，栖海，离不开海，打造出户户临海，而且与碧桂园小城之春共处临高，实现优质资源共享，让业主感受居家与自然的双重体验。...[|^碧桂园金沙滩三期17#楼已取得预售证|碧桂园金沙滩三期17#楼已取得预售证，在售户型YJ100，建面约102㎡三房两厅两卫栖海住宅，板式结构，总高18层，年终特惠97折，均价10000元。预计2021年11月15交付。[|^|开盘时间：2019年12月15日17#开盘|^|预售证号：【2019】临房预字（41）号|',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL),
	 (NULL,'fd850547a093eedd26757d39905cfc72','0',NULL,'首创禧悅湾',NULL,NULL,NULL,NULL,NULL,'0','万宁兴隆旅游度假区莲清大道首创禧悦湾查看地图','0','0',NULL,'首创禧悅湾,首创芭蕾雨,首创芭蕾雨三期','0','3室2厅2卫','0.00','18500.00',NULL,'2020-12-31',NULL,'(2018)万房预字(35)号,(2018)万房预字(49)号','海南首创奥莱置业有限公司','海南首创奥莱置业有限公司','暂无数据','73050','81600','0','40%','1.01','板楼,小高层','70','0','住宅类','700','北京盛世物业公司','3.20',NULL,'615','0.00',NULL,'20',NULL,NULL,NULL,'0',NULL,'景点：神州半岛旅游度假区、兴隆热带植物园、兴隆热带花园、石梅湾、日月湾等^其他：首创奥特莱斯、特色酒店、大型超市、特色餐饮、休闲会所','0','0','精装修','https://t.focus-img.cn/sh520x390sh/xf/xc/790d73e5-2f24-4942-9b23-1438b83020ae.JPEG',NULL,'0','0','0','在售','0','0','0','110.2251190','18.7652020','0','0',NULL,NULL,NULL,NULL,NULL,'暂无数据','暂无数据',NULL,NULL,NULL,'【首创禧悅湾】102-104㎡阔景洋房在售|家门口的奥特莱斯，千亩成熟大盘，随心畅享一站式配套，102-104㎡阔景洋房在售，一楼带花园。更多详情请拨打400免费电话咨询。|2020-08-21^【首创禧悅湾】配套齐全|首创禧悦湾洋房在售，建面约102-104㎡三房，项目一期占地面积113020平方米，总建筑面积11.9万平方米，建筑密度24.10%，容积率0.99，绿地率40.36%，产品为小高层、洋房、别墅，居住户数1791户，配套齐全，目前一期已经全部交付入住。|2020-08-12^【首创禧悅湾】首创禧悦湾三居室在售均价19000元/㎡|2020年7月3日讯:首创禧悦湾是首创芭蕾雨二期,项目主推建面102-104㎡三室户型,均价约19000元/㎡,了解更多详情,可致电或自行前往售楼处。|2020-08-11^【首创禧悅湾】宜居度假精品社区|首创·禧悦湾，坐落于海南东海岸兴隆度假区，是依托首创·芭蕾雨一期、二期醇熟绽放的基础，倾心造就的宜居度假精品社区。预计交房时间2021年5月。更多详情请拨打400电话免费咨询。|2020-07-10^【首创禧悅湾】整体均价18500元/㎡|万宁首创禧悦湾主体基本完工，在售建面约102-104㎡三居洋房，板式结构，南北通透，整体均价18500元/㎡。预计2021年5月份交房。注:以上信息供参考,具体信息仍需参考现场实际销售情况，更多详情欢迎拨打400来电免费咨询！|2020-07-03^【首创禧悅湾】项目为高层|首创·禧悦湾物业费：3.2元/㎡。首创·禧悦湾项目为高层。绿化率40.01%。带装修交付。|2020-06-17^【首创禧悅湾】2021年5月交房|建面102-104平的精美3房，现单价1.74-1.84万/平方米，21年5月交房。更多详情请拨打400免费电话咨询。|2020-06-04^【首创禧悅湾】五一特价房|首创·禧悦湾推出五一特惠房源，建面102-104平的精美3房，现单价1.74-1.84万/平方米，21年5月交房。特价房源一共22套，房源有限，先到先得，更多详情请拨打400免费电话咨询。|2020-05-19^【首创禧悅湾】2021年5月（暂定）交房|首创·禧悦湾2021年5月（暂定）交房。18500元/平方米。项目规划建设700户。带装修交房。容积率1.01。更多详情请拨打400免费电话咨询。|2020-05-07^【首创禧悅湾】均价18500元/平方米|首创·禧悦湾项目为高层。首创·禧悦湾位于：海南东线高速神州半岛出口兴隆方向500米。绿化率40.01%。更多详情请拨打400免费电话咨询。|2020-04-21',NULL,'2021-08-30 17:58:33.0','2021-08-30 17:58:33.0',NULL,NULL,NULL,NULL);

	

	
	
	
	
--------------------------------------------- =========================== ---------------------------------------------------


FE67D9BB7A60CC

select * from dwb_db.dwb_customer_add_new_code;

CREATE TABLE dwb_db.dwb_customer_add_new_code (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `ods_id` varchar(33) DEFAULT NULL COMMENT '源表id',
  `ods_table_name` varchar(30) DEFAULT NULL COMMENT '源表名称',
  `imei` varchar(30) DEFAULT NULL COMMENT 'imei',
  `visit_week` varchar(30) DEFAULT NULL COMMENT '浏览周度',
  `visit_month` int(30) DEFAULT null COMMENT '浏览月度',
  `visit_quarter` varchar(30) DEFAULT NULL COMMENT '浏览季度',
  `add_new_code` int(2) NOT null default '0' COMMENT '增存量标识',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '增存量的标识：0 增量  1  存量',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_add_new_code_imei` (`imei`) USING BTREE,
  KEY `idx_add_new_visit_week` (`visit_week`) USING BTREE,
  KEY `idx_add_new_visit_month` (`visit_month`) USING BTREE,
  KEY `add_new_code_visit_quarter` (`visit_quarter`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户增全量标识表';


 select ;

SELECT max(id) ods_id,'a_dwb_customer_browse_log' ods_table_name,imei,concat(substr(visit_month,1,4),'-',current_week) visit_week,visit_month,concat(substr(visit_month,1,4),'Q',QUARTER(min(visit_date))) visit_quarter FROM  dwb_db.a_dwb_customer_browse_log 
group by imei,visit_month,current_week limit 10;


select * from dwb_db.a_dwb_customer_browse_log adcbl limit 10;


--------------------------------------------- =========================== ---------------------------------------------------





-- 备份
-- 根据newest_id覆盖相关内容

select t1.*,t2.* from 
  (select newest_id, newest_name, city_id, county_id, address, property_type, property_fee, gd_lng, gd_lat, building_type, land_area, building_area, building_num, floor_num, household_num, right_term, green_rate, volume_rate, park_num, park_rate, decoration, sale_status, sale_address, recent_opening_time, recent_delivery_time, unit_price from dwb_db.a_dwb_newest_info) t1
left join 
  (select newest_id, newest_name, city_id, county_id, address, property_type, property_fee, lng, lat, building_type, land_area, building_area, building_num, floor_num, household_num, right_term, green_rate, volume_rate, park_num, park_rate, decoration, sales_state, sale_address, recent_opening_time, recent_delivery_time, unit_price from dws_db.dws_newest_info where dr = 0) t2
 on t1.newest_id = t2.newest_id 
where t2.newest_id is not null;



update dwb_db.a_dwb_newest_info a,
     (select newest_id, newest_name, city_id, county_id, address, property_type, property_fee, lng, lat, building_type, land_area, building_area, building_num, floor_num, household_num, right_term, green_rate, volume_rate, park_num, park_rate, decoration, sales_state, sale_address, recent_opening_time, recent_delivery_time, unit_price from dws_db.dws_newest_info where dr = 0) b
set a.city_id=b.city_id,
    a.county_id=b.county_id, a.address=b.address, a.property_type=b.property_type, a.property_fee=b.property_fee, a.gd_lng=b.lng, a.gd_lat=b.lat, a.building_type=b.building_type, a.land_area=b.land_area, a.building_area=b.building_area, a.building_num=b.building_num, a.floor_num=b.floor_num, a.household_num=b.household_num, a.right_term=b.right_term, a.green_rate=b.green_rate, a.volume_rate=b.volume_rate, a.park_num=b.park_num, a.park_rate=b.park_rate, a.decoration=b.decoration, a.sale_status=b.sales_state, a.sale_address=b.sale_address, a.recent_opening_time=b.recent_opening_time, a.recent_delivery_time=b.recent_delivery_time, a.unit_price=b.unit_price 
where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;




select max(land_area),min(land_area) from dwb_db.a_dwb_newest_info adni ;

select t1.*,t2.city_name from 
(select land_area,newest_name,city_id,address,newest_id,issue_number,alias from dwb_db.a_dwb_newest_info adni where land_area > 10000000 ) t1
left join 
(select city_id ,city_name  from dws_db.dim_geography dg group by city_id ,city_name) t2 
on t1.city_id=t2.city_id;


--------------------------------------------- =========================== ---------------------------------------------------





-- 删除housing_id的历史数据
-- 备份
-- 更新url为楼盘id
-- 修改python脚本
--    读取
--    表关联
--    修改楼盘id

delete from odsdb.ori_newest_provide_sche;

update odsdb.ori_newest_pr ovide_sche a ,(select url,uuid,newest_name from odsdb.ori_newest_info_base where main_id is not null union all select url,uuid,newest_name from odsdb.ori_newest_info_base where main_id is null and remark is null group by url,uuid,newest_name having count(1) = 1) b 
  set a.url = b.uuid where a.url = b.url ;

truncate table dwb_db.dwb_newest_provide_sche ;

delete from dws_db.dws_newest_provide_sche where period = '2018Q1';

insert
	into
	dws_db.dws_newest_provide_sche (newest_id,`date`,period,provide_title,provide_sche)
select
	newest_id ,
	`date` ,
	period ,
	provide_title ,
	provide_sche
from
	dwb_db.dwb_newest_provide_sche
where
	dr = 0
	and length(provide_title)>2
	and length(provide_sche)>2
	and period = '2018Q1';






--------------------------------------------- =========================== ---------------------------------------------------





select url,uuid from odsdb.ori_newest_info_base where url in ('//cq.newhouse.fang.com/loupan/3110123590.htm','//cq.newhouse.fang.com/loupan/3110123904.htm','//cq.newhouse.fang.com/loupan/3110123954.htm','//cs.newhouse.fang.com/loupan/2710189578.htm','//cs.newhouse.fang.com/loupan/2710190256.htm','//cs.newhouse.fang.com/loupan/2710190536.htm','//hf.newhouse.fang.com/loupan/2110177976.htm','//hf.newhouse.fang.com/loupan/2111193920.htm','//nn.newhouse.fang.com/loupan/2910196244.htm','//suzhou.newhouse.fang.com/loupan/1822201096.htm','//suzhou.newhouse.fang.com/loupan/1822201868.htm','//xm.newhouse.fang.com/loupan/2212125884.htm','//zz.newhouse.fang.com/loupan/2510148771.htm','http://foshan.jiwu.com/loupan/1292443.htmpan/83897.html','http://km.jiwu.com/loupan/586987.html','https://bj.fang.ke.com/loupan/p_gcbsjafmee/','https://cq.fang.ke.com/loupan/p_bdzyhyfabhan/','https://cq.fang.ke.com/loupan/p_bjcjyxtabikx/','https://cq.fang.ke.com/loupan/p_hrxdcablas/','https://cq.fang.ke.com/loupan/p_ldxlyyabkab/','https://cq.fang.ke.com/loupan/p_zsyjfbjezl/','https://cs.fang.ke.com/loupan/p_aajvp/','https://dg.fang.ke.com/loupan/p_jdcnyjaazst/','https://fs.fang.ke.com/loupan/p_bgyfthwabhbu/','https://fz.fang.ke.com/loupan/p_tjhfbkmnv/','https://hf.fang.ke.com/loupan/p_mmsfabcnp/','https://hk.fang.ke.com/loupan/p_bhbsbcmrw/','https://hz.fang.ke.com/loupan/p_gkdfjcxfbkqva/','https://hz.fang.ke.com/loupan/p_rcwfzcabqyo/','https://km.fang.ke.com/loupan/p_bgykynyxablyp/','https://qd.fang.ke.com/loupan/p_hecccdfxfblode/','https://qd.fang.ke.com/loupan/p_ydxfabbxb/','https://sh.fang.ke.com/loupan/p_rclgyhbjbaw/','https://su.fang.ke.com/loupan/p_hrjywablmv/','https://su.fang.ke.com/loupan/p_szyhggblcqt/','https://su.julive.com/project/20124536.html#around_map','https://sz.fang.ke.com/loupan/p_hryhlsababo/','https://sz.fang.ke.com/loupan/p_xsdggbljhe/','https://sz.julive.com/project/20038194.html#around_map','https://sz.julive.com/project/20039313.html#around_map','https://sz.julive.com/project/20039587.html#around_map','https://sz.julive.com/project/20039666.html#around_map','https://sz.julive.com/project/20040877.html#around_map','https://sz.julive.com/project/20040893.html#around_map','https://sz.julive.com/project/20041308.html#around_map','https://sz.julive.com/project/20041444.html#around_map','https://sz.julive.com/project/20049202.html#around_map','https://sz.julive.com/project/20049244.html#around_map','https://sz.julive.com/project/20078961.html#around_map','https://sz.julive.com/project/20122285.html#around_map','https://tj.fang.ke.com/loupan/p_aaizl/','https://wh.fang.ke.com/loupan/p_wthgjsqblkbe/','https://xianyang.julive.com/project/20048550.html#around_map','https://xianyang.julive.com/project/20048568.html#around_map','https://xianyang.julive.com/project/20048572.html#around_map','https://xianyang.julive.com/project/20053577.html#around_map','https://xianyang.julive.com/project/20053580.html#around_map','https://xm.fang.ke.com/loupan/p_thsmxyzabkvl/','https://zz.fang.ke.com/loupan/p_gljbjuuz/') group by url,uuid;



select * from dwb_db.dwb_newest_customer_info; 

select * from dwb_db.dwb_newest_city_customer_num ;
select * from dwb_db.dwb_newest_county_customer_num dnccn ;
select * from dwb_db.dwb_issue_supply_city disc ;
select * from dwb_db.dwb_issue_supply_county disc ;





-- insert into dws_db.dws_supply 
select newest_id ,`date` ,period ,provide_title ,provide_sche from dwb_db.dwb_newest_provide_sche where dr = 0 and length(provide_title)>2 and length(provide_sche)>2
;


select length('。');


--------------------------------------------- =========================== ---------------------------------------------------


select * from 
	(select *
	from dws_db.dws_customer_visit_reality_info 
	where dr = 0) t1
left join
	(select * from dws_db.dws_customer_visit_reality_info_20210823 
	where dr = 0) t2 
on t1.newest_id=t2.newest_id and t1.city_name=t2.city_name and t1.county_name=t2.county_name  and 
t1.newest_name =t2.newest_name and t1.`data`=t2.`data` and t1.data_sum=t2.data_sum and t1.rate=t2.rate and t1.period=t2.period
where t1.lng_v!=t2.lng_v or t1.lng!=t2.lng or t1.lat!= t2.lat or t1.lat_v != t2.lat_v;


select * from
 (select *
	from dws_db.dws_customer_visit_reality_info_20210823 
	where dr = 0) t1
left join 
	(select *
	from dws_db.dws_customer_visit_reality_info 
	where dr = 0) t2
on t1.newest_id=t2.newest_id and t1.city_name=t2.city_name and t1.county_name=t2.county_name  and 
t1.newest_name =t2.newest_name and t1.`data`=t2.`data` and t1.data_sum=t2.data_sum and t1.rate=t2.rate and t1.period=t2.period
where t2.id is null and t1.newest_name = '高新万达广场';


update dws_db.dws_customer_visit_reality_info set lng_v = 117.12843 where newest_name = '高新万达广场' and city_name = '济南市' and dr = 0;

update dws_db.dws_customer_visit_reality_info set lat_v = 36.686028 where newest_name = '高新万达广场' and city_name = '济南市' and dr = 0;

update dws_db.dws_customer_visit_reality_info set county_name = '历下区' where newest_name = '高新万达广场' and city_name = '济南市' and dr = 0;




--------------------------------------------- =========================== ---------------------------------------------------









update dwb_db.dwb_newest_provide_sche set dr = 1 ;

update dwb_db.dwb_newest_provide_sche set provide_sche = replace(right(provide_sche,1),'：','') where right(provide_sche,1) = '：' and peroiod = '2018Q1';

delete from dwb_db.dwb_newest_provide_sche where period like '%2018%';




--------------------------------------------- =========================== ---------------------------------------------------










truncate table dws_db.dws_customer_month ;


select newest_id from dws_db.dws_newest_provide_sche dnps where period = '2021Q2'  group by newest_id ;

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dws_db'
AND TABLE_NAME LIKE  '%admit%';


select  period,newest_id,max(city_id) ,count(1) from dws_db.dws_newest_popularity_rownumber_quarter dnprq where dr = 0 and city_id not in ('441900','442000')  group by period,newest_id having count(1) = 1;


update dws_db.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2021Q2';
update dws_db.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2021Q1';
update dws_db.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q4';

select  period,newest_id,max(city_id) ,count(1) from dws_db.dws_newest_investment_pop_rownumber_quarter dnprq where dr = 0 and city_id not in ('441900','442000')  group by period,newest_id having count(1) = 1;


update dws_db.dws_newest_investment_pop_rownumber_quarter set dr = 1 where period = '2021Q2';
update dws_db.dws_newest_investment_pop_rownumber_quarter set dr = 1 where period = '2021Q1';
update dws_db.dws_newest_investment_pop_rownumber_quarter set dr = 1 where period = '2020Q4';



update dws_db.dws_newest_popularity_rownumber_quarter set dr = 0 where dr is null ;
update dws_db.dws_newest_popularity_rownumber_quarter set create_time = now() where create_time is null ;
update dws_db.dws_newest_popularity_rownumber_quarter set update_time = now() where update_time is null ;



select newest_id,min(cast(layout_price/layout_area as int(11))*10000) from dws_db.dws_newest_layout dnl group by newest_id ;

select newest_id,cast(layout_price/layout_area as int) from dws_db.dws_newest_layout dnl group by newest_id ;

select newest_id,cast(price as SIGNED) from (select newest_id,min(layout_price/layout_area)*10000 price from dws_db.dws_newest_layout dnl group by newest_id ) t;

select newest_id,cast(min(layout_price/layout_area)*10000 as SIGNED) price from dws_db.dws_newest_layout dnl group by newest_id;



update 
  dws_db.dws_newest_info a ,
  (select newest_id,cast(min(layout_price/layout_area)*10000 as SIGNED) price from dws_db.dws_newest_layout dnl where layout_area is not null and layout_area != 0 group by newest_id) b 
set a.unit_price = b.price 
where a.newest_id = b.newest_id and a.unit_price is null and b.price is not null;

select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' and period = '2021Q2' and marriage = '已婚' and education = '高' and have_child = '有'

--------------------------------------------- =========================== ---------------------------------------------------



SHOW processlist;

kill 2390603;

drop table dwb_db.dwb_issue_supply_county ;


CREATE TABLE dwb_db.dwb_issue_supply_county (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(30) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `county_id` varchar(30) DEFAULT NULL COMMENT '区县id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区县名称',
  `intention` int(6) NOT NULL DEFAULT 0	COMMENT '区县意向数量',
  `period` varchar(30) DEFAULT NULL COMMENT '当前月份时间(格式：%Y%m)',
  `quarter` varchar(30) DEFAULT NULL COMMENT '当前季度时间(举例：2021Q1)',
  `intention_city` int(6) NOT NULL DEFAULT 0 COMMENT '城市意向数量',
  `rate` varchar(30) DEFAULT NULL COMMENT '占比',
  `supply_num` varchar(30) DEFAULT NULL COMMENT '城市供应套数',
  `supply_value` varchar(30) DEFAULT NULL COMMENT '区县供应套数',
  `cric_supply_num` varchar(30) DEFAULT NULL COMMENT 'CRIC套数',
  `num_index` varchar(30) DEFAULT NULL COMMENT '取数标识 （1 从CRIC中取数）',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识(0,有效，1，作废）',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_newest_city_id` (`city_id`) USING BTREE,
  KEY `idx_newest_county_id` (`county_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='区域总供应套数';



select city_id,city_name,quarter period,sum(supply_value) supply_num,dr,create_time,update_time,cric_supply_num,num_index from dwb_db.dwb_issue_supply_county where dr = 0 and quarter = '2021Q1' group by  city_id,city_name,quarter,dr,create_time,update_time,cric_supply_num,num_index;


update dwb_db.dwb_newest_issue_offer set issue_room = 0 
where id in ('40959','43852','40557','40906','40926','44757','42370','43857','44766','44185','43189','44768','40636','43289','44770','40020','43836','44772','40089','43293','42956','43296','41967','43299','43302','43304','43177','43072','40805','40402','43306','43968','43971','43310','40042','40367','43998','43312','41067','44784','39959','43321','42460','41098','40248','40343','41655','40483','41250','41940','44189','41791','41197','42029','41983','42433','41908','41754','42357','40008','44696','41971','41556','40216','44192','43833','44789','42866','43084','43081','43082','40536','42497','43326','41699','43689','43838','41768','42023','44603','44606','44805','43016','41222','41210','41216','44810','44820','44836','44842','40528','41139','41818','44710','43163','44847','44851','40220','43330','43333','44860','40537','43210','41752','41891','44609','40966','40578','44613','43286','41068','43336','43338','41027','42949','44871','42445','44312','43342','40251','40875','40899','44881','44888','43232','40857','40876','40864','44890','44894','44899','44903','40858','40923','40853','40836','40860','40878','42161','43877','43349','40790','41194','43086','43927','41235','43351','44004','44907','44910','43851','43044','43358','42441','43366','41023','41129','43370','44913','44916','44918','40877','43749','44617','44922','40446','40447','40448','40450','40452','44934','44937','44713','43176','43193','41241','40282','42507','43249','44948','44952','40306','44715','44960','44968','44978','41938','42961','43379','41579','41765','43779','40822','43381','40054','41224','44983','44986','41607','43030','44990','44992','43029','45003','45013','45016','45021','41997','43960','43384','42969','45030','42867','43064','41605','43639','43688','43791','43387','40318','43180','41883','41616','43390','45034','40527','41688','42321','40147','40623','40621','41625','42403','41208','41953','43395','45038','44720','44626','42482','43261','42991','42438','45040','40189','42097','40797','40787','40807','40503','41897','43113','43397','45042','41140','44209','43401','41491','42016','45056','45058','45062','45065','41861','43408','45082','45084','40233','40229','45093','42131','43182','41895','40560','43221','43173','42410','43934','41912','41615','40547','43808','42829','65624','44020','45097','41972','41519','45108','43411','43414','41198','41413','44023','42351','43417','41217','41184','42134','44631','40949','40516','40518','40581','44340','43419','45112','41105','43785','43667','45116','41209','41454','41124','41201','42277','41248','43897','44119','45135','44029','40207','43894','42305','42250','42314','42234','42248','40936','40802','42315','42215','42309','42093','42306','42124','42310','42085','42143','42297','42273','42071','42123','40879','40885','40762','40783','40915','40935','42303','42302','45139','40329','40553','40474','43422','40579','43425','40477','43158','43982','42317','44215','42853','40201','40149','39962','42440','43736','43670','42510','40633','40888','41914','43431','41279','42806','43218','45147','44638','40640','43434','44034','44036','44729','40598','44040','44042','41814','44218','43436','41192','43787','41234','43820','41152','45153','41837','45158','41674','45162','41220','41651','41623','41486','42952','43440','42912','41646','43184','43718','41705','39966','40541','45164','44045','43156','43090','44122','41227','45178','43444','41960','41624','41182','41001','41242','45190','41177','45195','42888','45210','41343','44688','40872','40913','43448','41968','39956','41965','44640','44048','44052','44054','41203','44226','41172','41101','43229','40592','41171','45216','41563','40824','40957','40903','40960','40837','40914','40756','40848','40886','40931','40816','41173','45224','45227','45237','45240','40831','41251','42019','41218','43231','41899','42233','43732','39993','43459','43462','43472','41925','43170','41614','42020','44367','41221','41236','43203','43187','41449','45252','41739','41561','43678','41416','40953','40900','42469','45256','41527','42984','40400','41735','42475','41991','40152','43480','43482','41769','44230','42470','43033','40889','40814','40910','42480','42511','41696','42569','42985','43486','41583','42223','40565','40613','40038','43648','42989','43804','41958','65630','42179','42285','42266','40505','40737','45258','45260','40488','43492','40887','40924','40459','40417','40061','40055','40618','40507','40543','40440','40496','40439','40545','40279','40395','40337','43037','41819','41249','41244','41094','41245','42361','42553','42556','42442','43663','40594','45263','41931','41945','41933','41851','41426','65632','40323','44394','40542','42986','43199','43214','41592','41630','40431','43502','44398','44064','45269','45273','45281','45290','41547','43709','44735','41031','43505','40423','42924','41877','44655','43508','41598','65636','40482','40882','42375','40048','40327','40304','40599','42259','45300','41829','42176','41405','43752','43696','41687','41678','44244','43853','44071','42257','40031','40559','40951','42552','43756','41135','41902','41992','41975','43209','41180','41170','41205','41230','44249','41156','41186','41093','41206','41207','41087','43812','40555','40556','44410','41835','43191','40275','65642','65645','40478','43518','40405','40569','40224','45307','40896','40963','44738','41917','44692','40070','40575','43839','40134','40627','40256','40629','43788','40570','45316','45318','43990','43023','43523','43525','43529','43531','42010','43263','43533','43539','43543','43551','43553','40922','41560','41239','44253','41852','42492','43819','43557','43167','44078','44080','41437','40611','43047','44082','43693','41901','40155','41764','45324','45330','40718','41502','44260','41618','41921','44668','43196','65647','44265','40000','44670','42213','43561','41847','40548','40828','43148','41476','41168','41400','44089','40316','43563','42491','41237','42260','42542','41427','41638','40084','40285','40086','40168','43567','42282','41717','45334','42483','40589','41054','42804','41474','42494','44424','44426','41540','43572','44093','45336','44428','45340','41990','43575','43579','45343','41955','45346','45349','44133','41447','43581','43583','42781','44270','44272','40916','42495','40604','41164','43197','42819','44677','40302','40362','43711','43744','43775','40132','42834','44279','41189','39974','43590','44100','45354','43166','45364','44282','41640','43066','43179','43845','40332','45369','44105','40271','40638','42504','44107','43825','40868','43793','45376','42554','42561','42978','42999','45387','45389','45404','45406','45420','45423','45426','45429','45436','44436','42938','40311','40753','45439','44286','41854','40937','45441','43818','41199','41159','41535','42967','42024','44694','41082','42558','43598','41926','41010','42101','40525','40730','40934','41633','43874','43602','43608','43611','43614','43621','41514','40145','65662','41507','65664','42435','41660','45450','42803','40602','43759','40254','40470','40472','41828','41096','43623','39944','41492','43045','39948','42891','41866','41950','41929','43626','41195','41033','43906','40340','40172','40461','44292','41951','43628','40398','40625','44297','44300','41631','41534','41393','45454');

select max(id) from dwb_db.dwb_newest_issue_offer where issue_quarter = '2018Q4' group by newest_name having count(1) > 1;

select * from dwb_db.cust_browse_log_201801_202106 limit 100;

select t1.*,t2.value2 from 
  (select city_name,period,sum(value) value1 from dws_db.dws_supply where dr=0 and period_index=1 and city_county_index=2 group by city_name,period) t1 
left join 
  (select city_name,period,value value2 from dws_db.dws_supply where dr=0 and period_index=1 and city_county_index=1) t2 
on t1.city_name=t2.city_name and t1.period=t2.period where t1.value1!=t2.value2;


update dws_db.dws_supply a ,
  (select city_name,period,sum(value) value from dws_db.dws_supply where dr=0 and period_index=1 and city_county_index=2 and value!='-' group by city_name,period) b 
set a.value=b.value 
where a.city_name=b.city_name and a.period=b.period and a.dr=0 and a.period_index=1 and a.city_county_index=1 and a.value!='-';


update dws_db.dws_newest_offer_rate set dr = 1;

insert into dws_db.dws_newest_offer_rate (city_id,offer,rate,period,create_time,update_time,dr)
select ds.city_id,value,case when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end rate,ds.period,now(),now(),'0' from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null and period='2020Q4' and dr = 0 ) dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1 and period = '2020Q4' and dr = 0 ) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period
union all 
select ds2.city_id,value,case when value='-' then '-' when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds2.period,now(),now(),'0' from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null and period='2020Q4' and dr = 0 ) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1 and period='2020Q4' and dr = 0 ) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period;



--------------------------------------------- =========================== ---------------------------------------------------




--------------------------------------------- =========================== ---------------------------------------------------





select * from 
  (select city_id ,city_name ,newest_id,newest_name from temp_db.tmp_city_newest_customer_imei group by city_id ,city_name ,newest_id,newest_name) t1
left join 
  (select city_id ,city_name ,uuid newest_id from odsdb.ori_newest_info_base group by city_id ,city_name ,newest_id ) t2
on t1.newest_id = t2.newest_id where t2.newest_id is null;



select city_id,city_name,newest_name from temp_db.tmp_city_newest_customer_imei_noid tcncin where period = '2021Q2' and city_id in ('110000', '120000', '130100', '130200', '130600', '210100', '220100', '310000', '320100', '320200', '320300', '320400', '320500', '320600', '321000', '330100', '330200', '330300', '330400', '330500', '330600', '340100', '350100', '350200', '360100', '360400', '360700', '370100', '370200', '370300', '370600', '370800', '410100', '420100', '430100', '440100', '440300', '440400', '440500', '440600', '441200', '441300', '441900', '442000', '450100', '460100', '460200', '500000', '510100', '520100', '530100', '610100', '610300', '610400')
group by city_id,city_name,newest_name;

select issue_code from odsdb.city_newest_deal where city_name = '贵阳' and insert_time is null group by issue_code ;

select issue_date from temp_db.city_newest_deal_data_check where city_name = '沈阳' group by issue_date ;

select open_date from temp_db.city_newest_deal_data_check where city_name = '沈阳' group by open_date ;

select issue_date from odsdb.city_newest_deal cnd where city_name = '沈阳' group by issue_date ;

select open_date from odsdb.city_newest_deal cnd where city_name = '沈阳' group by open_date ;





insert into odsdb.city_newest_deal (url,city_name,gd_city,floor_name,floor_name_new,clean_floor_name,floor_name_clean,address,business,issue_code,issue_date,issue_date_clean,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state,insert_time)
	select 
		url,
		city_name,
		concat(city_name,'市') gd_city,
		floor_name,
		floor_name_new,
		clean_floor_name,
		floor_name floor_name_clean,
		address,
		business,
		issue_code,
		issue_date,
		str_to_date(substr(issue_date,1,10),'%Y-%m-%d') issue_date_clean,
		str_to_date(replace(case when open_date = '' then '9999-09-09' when open_date is null then '9999-09-09' else open_date end,'/','-'),'%Y-%m-%d') open_date,
		issue_area,
		sale_state,
		building_code,
		room_sum,
		area,
		simulation_price,
		sale_telephone,
		sale_address,
		room_code,
		room_sale_area,
		room_sale_state,
		now()
	from temp_db.city_newest_deal_data_check where city_name = '沈阳' and floor_name is not null or floor_name='';

delete from temp_db.city_newest_deal_data_check where city_name = '西安';

delete from odsdb.city_newest_deal where city_name = '合肥' and floor_name is not null or floor_name='';

select city from dwb_db.dwb_newest_issue_offer dnio group by city ;

select city_name from odsdb.city_newest_deal cnd group by city_name ;

select city_name from dwb_db.dwb_issue_supply_city disc where period = '2021Q1' and dr = 0 group by city_name ;

select city_name ,dim_city_id from odsdb.dim_housing dh where city_name = '上海市' group by city_name ,dim_city_id ;

select sales_state from dws_db.dws_newest_info dni where dr = 0 group by sales_state ;


--------------------------------------------- =========================== ---------------------------------------------------




CREATE TABLE temp_db.tmp_city_newest_customer_imei (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `city_id` varchar(30) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名字',
  `newest_id` varchar(30) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(64) DEFAULT NULL COMMENT '楼盘名字',
  `period`varchar(30) DEFAULT NULL COMMENT '周期',
  `dr` varchar(3) DEFAULT NULL COMMENT '有效标识',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `ind_imei_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

truncate table temp_db.tmp_city_newest_customer_imei ;

select customer imei,housing_id from odsdb.79_cust_browse_log_201801_202106 where idate between '20201001' and '20201005' group by customer,housing_id;

select count(1) from odsdb.79_cust_browse_log_201801_202106 where substr(visit_time,1,4) between '2020' and '2021';  -- 64822

select count(1) from odsdb.79_cust_browse_log_201801_202106 where substr(idate ,1,4) between '2020' and '2021';


update temp_db.tmp_city_newest_customer_imei a,dws_db.dws_newest_period_admit b set a.on_index = 1 where a.city_id = b.city_id and a.newest_id=b.newest_id;


select t1.* from 
 (select city_id ,newest_id from temp_db.tmp_city_newest_customer_imei where period = '2020Q4') t1
inner join 
 (select city_id ,newest_id from dws_db.dws_newest_period_admit where dr = 0 and period = '2020Q4') t2
on t1.city_id = t2.city_id and t1.newest_id=t2.newest_id;  -- 5933



select city_id ,city_name ,count(1) from (
	select
		city_id ,
		city_name ,
		newest_name
	from
		temp_db.tmp_city_newest_customer_imei_noid tcnci
	where
		city_id in ('110000', '120000', '130100', '130200', '130600', '210100', '220100', '310000', '320100', '320200', '320300', '320400', '320500', '320600', '321000', '330100', '330200', '330300', '330400', '330500', '330600', '340100', '350100', '350200', '360100', '360400', '360700', '370100', '370200', '370300', '370600', '370800', '410100', '420100', '430100', '440100', '440300', '440400', '440500', '440600', '441200', '441300', '441900', '442000', '450100', '460100', '460200', '500000', '510100', '520100', '530100', '610100', '610300', '610400')
        and imei != '0'
		group by
		city_id ,
		city_name,
		newest_name
) t group by city_id ,city_name;


--- 三季度加总项目上线情况
-- 项目总量

select city_id ,city_name ,count(1),period from (
	select
		city_id ,
		city_name ,
		newest_name,
		period
	from
		temp_db.tmp_city_newest_customer_imei_noid tcnci
	where
		city_id in ('110000', '120000', '130100', '130200', '130600', '210100', '220100', '310000', '320100', '320200', '320300', '320400', '320500', '320600', '321000', '330100', '330200', '330300', '330400', '330500', '330600', '340100', '350100', '350200', '360100', '360400', '360700', '370100', '370200', '370300', '370600', '370800', '410100', '420100', '430100', '440100', '440300', '440400', '440500', '440600', '441200', '441300', '441900', '442000', '450100', '460100', '460200', '500000', '510100', '520100', '530100', '610100', '610300', '610400')
        and imei != '0'
		group by
		city_id ,
		city_name,
		newest_name,
		period
) t group by city_id ,city_name,period;

-- 上线总数量
select city_id ,count(1) from ( 
select city_id ,newest_id from dws_db.dws_newest_period_admit where dr = 0 group by city_id,newest_id ) t 
group by city_id ;


-- 分季度
select city_id ,city_name ,count(1) ,period from (
	select
		city_id ,
		city_name ,
		newest_id ,
		period
	from
		temp_db.tmp_city_newest_customer_imei tcnci
	where
		city_id in ('110000', '120000', '130100', '130200', '130600', '210100', '220100', '310000', '320100', '320200', '320300', '320400', '320500', '320600', '321000', '330100', '330200', '330300', '330400', '330500', '330600', '340100', '350100', '350200', '360100', '360400', '360700', '370100', '370200', '370300', '370600', '370800', '410100', '420100', '430100', '440100', '440300', '440400', '440500', '440600', '441200', '441300', '441900', '442000', '450100', '460100', '460200', '500000', '510100', '520100', '530100', '610100', '610300', '610400')
        and imei != '0'
	group by
		city_id ,
		city_name,
		newest_id,
		period 
) t group by city_id ,city_name,period;

select gd_city ,count(1) from (
select
	gd_city,
	clean_floor_name
from
	odsdb.79_cust_browse_log_201801_202106
where
	housing_id is null
	and gd_city in ('北京市', '天津市', '石家庄市', '唐山市', '保定市', '沈阳市', '长春市', '上海市', '南京市', '无锡市', '徐州市', '常州市', '苏州市', '南通市', '扬州市', '杭州市', '宁波市', '温州市', '嘉兴市', '湖州市', '绍兴市', '合肥市', '福州市', '厦门市', '南昌市', '九江市', '赣州市', '济南市', '青岛市', '淄博市', '烟台市', '济宁市', '郑州市', '武汉市', '长沙市', '广州市', '深圳市', '珠海市', '汕头市', '佛山市', '肇庆市', '惠州市', '东莞市', '中山市', '南宁市', '海口市', '三亚市', '重庆市', '成都市', '贵阳市', '昆明市', '西安市', '宝鸡市', '咸阳市')
group by
	gd_city,
	clean_floor_name) t 
group by gd_city;


select uuid newest_id from odsdb.dim_housing dh where uuid is not null and dim_city_id is null group by uuid;

select newest_id from temp_db.tmp_city_newest_customer_imei tcnci group by newest_id ;

select id dim_city_id ,city_id,city_name from odsdb.dim_city;


select city_id ,count(1) ,period from ( 
  select city_id ,period,newest_id from dws_db.dws_newest_period_admit where dr = 0 group by city_id,newest_id,period ) t 
group by city_id ,period;


select city_id , city_name , newest_id ,newest_name ,imei ,on_index ,period from temp_db.tmp_city_newest_customer_imei tcnci
where city_id in ('110000', '120000', '130100', '130200', '130600', '210100', '220100', '310000', '320100', '320200', '320300', '320400', '320500', '320600', '321000', '330100', '330200', '330300', '330400', '330500', '330600', '340100', '350100', '350200', '360100', '360400', '360700', '370100', '370200', '370300', '370600', '370800', '410100', '420100', '430100', '440100', '440300', '440400', '440500', '440600', '441200', '441300', '441900', '442000', '450100', '460100', '460200', '500000', '510100', '520100', '530100', '610100', '610300', '610400')
and period = '2021Q2';


select city_id,city_name from dwb_db.dwb_dim_geography_55city where city_name != '丽水市'  group by city_id,city_name ;


show create table dw_a.dim_city ;
CREATE TABLE odsdb.dim_city (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `country_id` int(11) DEFAULT NULL COMMENT '国家ID',
  `country_name` varchar(64) DEFAULT NULL COMMENT '国家名称',
  `province_id` int(11) DEFAULT NULL COMMENT '省份ID',
  `province_name` varchar(64) DEFAULT NULL COMMENT '省份名称',
  `city_id` int(11) DEFAULT NULL COMMENT '城市ID',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `city_first_letter` varchar(1) DEFAULT NULL COMMENT '城市首字母',
  `city_zone` varchar(20) DEFAULT NULL COMMENT '城市划分地区',
  `city_group` varchar(20) DEFAULT NULL COMMENT '城市所属都市圈',
  `city_level` tinyint(1) DEFAULT NULL COMMENT '城市等级{1:一线城市, 2:二线城市, 3:三线城市, 4:四线城市, 5:五线城市, ...}',
  `city_level_desc` varchar(20) DEFAULT NULL COMMENT '城市等级描述',
  `region_id` int(11) DEFAULT NULL COMMENT '区县ID',
  `region_name` varchar(64) DEFAULT NULL COMMENT '区县名称',
  `block_name` varchar(128) DEFAULT NULL COMMENT '板块名称',
  `grade` tinyint(1) NOT NULL COMMENT '级别{1:国家, 2:省份, 3:城市, 4:区县, 5:板块}',
  `lat` varchar(15) DEFAULT NULL COMMENT '纬度',
  `lng` varchar(15) DEFAULT NULL COMMENT '经度',
  `boundary` longtext COMMENT '坐标边界',
  `parent_id` int(11) DEFAULT NULL COMMENT '上级记录ID',
  `is_del` tinyint(1) DEFAULT '0' COMMENT '是否逻辑删除',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=37605 DEFAULT CHARSET=utf8mb4 COMMENT='城市表\n省 市 行政区 板块  经纬度';

--------------------------------------------- =========================== ---------------------------------------------------


select * from 
  (select uuid from odsdb.ori_newest_info_base onib where remark is null group by uuid ) t1
left join 
  (select uuid from odsdb.dim_housing dh where uuid is not null group by uuid ) t2
on t1.uuid = t2.uuid where t2.uuid is null ;

select t1.uuid from 
  (select uuid from odsdb.dim_housing dh where uuid is not null and is_del = '0' group by uuid ) t1    -- 58621     -- 59805 - 19266 = 40539
inner 
  (select uuid from odsdb.ori_newest_info_base onib where remark is null group by uuid ) t2  -- 55164
on t1.uuid = t2.uuid where t2.uuid is null ;  -- 19266   -- 18,252


truncate table  odsdb.dim_housing ;


show create table dw_a.cust_browse_log_201801_202106 ;
CREATE TABLE odsdb.79_cust_browse_log_201801_202106 (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '浏览ID',
  `housing_id` int(11) DEFAULT NULL COMMENT '楼盘id',
  `customer` varchar(30) DEFAULT NULL COMMENT '用户号码',
  `plat_name` varchar(50) DEFAULT NULL COMMENT '平台名称',
  `action_name` varchar(50) DEFAULT NULL COMMENT '行为名称',
  `pv` int(10) DEFAULT NULL COMMENT 'PV',
  `visit_time` varchar(30) DEFAULT NULL COMMENT '浏览时间',
  `province_name` varchar(30) DEFAULT NULL COMMENT '省份名称',
  `city_name` varchar(30) DEFAULT NULL COMMENT '城市名称',
  `gd_city` varchar(30) DEFAULT NULL COMMENT '高德城市名称',
  `region_name` varchar(100) DEFAULT NULL,
  `floor_name` varchar(705) DEFAULT NULL,
  `clean_floor_name` varchar(705) DEFAULT NULL COMMENT '清洗过的楼盘名',
  `avg_price` varchar(127) DEFAULT NULL COMMENT '单价',
  `total_price` varchar(127) DEFAULT NULL COMMENT '总价',
  `layout` varchar(200) DEFAULT NULL COMMENT '户型',
  `area` varchar(200) DEFAULT NULL COMMENT '面积',
  `fitment` varchar(50) DEFAULT NULL COMMENT '装修',
  `house_type` varchar(127) DEFAULT NULL COMMENT '住宅类型',
  `prop_type` varchar(200) DEFAULT NULL COMMENT '产权性质',
  `address` varchar(1023) DEFAULT NULL COMMENT '详细地址',
  `lng` varchar(30) DEFAULT NULL COMMENT '经度',
  `lat` varchar(30) DEFAULT NULL COMMENT '纬度',
  `idate` varchar(10) DEFAULT NULL COMMENT '数据接收日期',
  `source` varchar(30) DEFAULT NULL COMMENT '来源手机系统',
  `status` tinyint(2) DEFAULT NULL COMMENT '状态',
  `valid` tinyint(4) DEFAULT NULL COMMENT '是否计费',
  `created_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `remark` varchar(100) DEFAULT NULL COMMENT '备注清洗的过程记录',
  `r_id` int(10) NOT NULL COMMENT '关联的源表id',
  `r_table_name` varchar(32) NOT NULL COMMENT '关联的源表名',
  PRIMARY KEY (`id`) USING BTREE,
  KEY `ind_imei_newest` (`floor_name`,`customer`,`idate`) USING BTREE,
  KEY `idx_query` (`city_name`,`clean_floor_name`) USING BTREE,
  KEY `idx_query2` (`province_name`,`clean_floor_name`) USING BTREE,
  KEY `idx_gd` (`lng`,`lat`) USING BTREE,
  KEY `idx_update` (`gd_city`,`clean_floor_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=89198752 DEFAULT CHARSET=utf8mb4;



CREATE TABLE dwb_db.dwb_issue_supply_county (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(30) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '城市名称',
  `county_id` varchar(30) DEFAULT NULL COMMENT '区县id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区县名称',
  `period_m` varchar(30) DEFAULT NULL COMMENT '当前月份时间(格式：%Y%m)',
  `period_q` varchar(30) DEFAULT NULL COMMENT '当前季度时间(举例：2021Q1)',
  `supply_num` varchar(30) DEFAULT NULL COMMENT '供应套数',
  `cric_supply_num` varchar(30) DEFAULT NULL COMMENT 'CRIC套数',
  `num_index` varchar(30) DEFAULT NULL COMMENT '取数标识 （1 从CRIC中取数）',
  `dr` int(2) NOT NULL DEFAULT '0' COMMENT '有效标识(0,有效，1，作废）',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_newest_city_id` (`city_id`) USING BTREE,
  KEY `idx_newest_county_id` (`county_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='区域总供应套数';


select origin_city city_name,uuid newest_id from odsdb.clean_ori_newest_alias_base where origin_city  = '保定' group by origin_city,uuid;


select city_name ,floor_name from odsdb.cust_browse_log_201801_202106 cbl where city_name = '保定' and idate between '20201001' and '20201231' group by city_name ,floor_name ;


--------------------------------------------- =========================== ---------------------------------------------------





select * from dws_db.dws_newest_period_admit dnpa ;

select city_id ,period ,newest_id from dws_db.dws_newest_period_admit where dr = 0 and period = '2020Q4'

select newest_id ,newest_name  from dws_db.dws_newest_info dni where dr = 0;

update dwb_db.dwb_issue_supply_city set cric_supply_num = supply_num where num_index = '1';


select * from 
  (select newest_id,city_id from dws_db.dws_newest_period_admit dnpa where dr = 0 and period = '2020Q4') t1
left join 
  (select newest_id from dws_db.dws_newest_info dni where dr = 0) t2
on t1.newest_id = t2.newest_id;


select * from 
  (select city_id ,count(1),period from  dws_db.dws_newest_period_admit dnpa where dr = 0 group by city_id,period) t1
left join 
  (select city_id ,city_name from dws_db.dim_geography dg where grade = 3 ) t2
on t1.city_id = t2.city_id;


--------------------------------------------- =========================== ---------------------------------------------------



select city_id,county_id,newest_name from odsdb.ori_newest_info_base where flag is not null limit 1;

select distinct newest_id from dwb_db.dwb_newest_provide_sche dnps ;

select url,main_id,newest_name from odsdb.ori_newest_info_base where main_id is not null 
union all 
select url,uuid,newest_name from odsdb.ori_newest_info_base where main_id is null and flag is not null
union all 
select url,uuid,newest_name from odsdb.ori_newest_info_base where main_id is null and flag is null group by url,uuid,newest_name having count(1) = 1;

truncate table dwb_db.dwb_newest_provide_sche;

show create table dws_db.dws_newest_provide_sche ;
CREATE TABLE dws_db.bak_20210818_dws_newest_provide_sche (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `newest_id` varchar(200) DEFAULT NULL COMMENT '楼盘id', 
  `date` date DEFAULT NULL COMMENT '日期',
  `period` varchar(255) DEFAULT NULL COMMENT '时间周期',
  `provide_title` varchar(500) DEFAULT NULL COMMENT '动态标题',
  `provide_sche` text COMMENT '动态正文',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=195878 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态表';
insert into dws_db.bak_20210818_dws_newest_provide_sche select * from dws_db.dws_newest_provide_sche ;

delete from dws_db.dws_newest_provide_sche where period = '2021Q2';

insert into dws_db.dws_newest_provide_sche (newest_id,`date`,period,provide_title,provide_sche)
select max(newest_id),max(`date`),max(peroiod),max(provide_title),provide_sche from (
select max(newest_id) newest_id,max(`date`) `date`,max(peroiod) peroiod,max(provide_title) provide_title,provide_sche from 
(select newest_id ,`date` ,peroiod ,provide_title ,provide_sche from dwb_db.dwb_newest_provide_sche) t1 group by provide_sche
union all
select newest_id ,`date` ,period peroiod ,provide_title ,provide_sche  from dws_db.bak_20210818_dws_newest_provide_sche dnps where period = '2021Q2') t
group by provide_sche;



SHOW processlist;

kill 2037660;

--------------------------------------------- =========================== ---------------------------------------------------


select t1.newest_name ,count(t1.newest_name) from 
  (select newest_name from odsdb.ori_newest_provide_sche onps group by newest_name) t1
left join 
  (select newest_id ,newest_name ,city_id  from dws_db.dws_newest_info group by newest_id ,newest_name ,city_id) t2
on t1.newest_name = t2.newest_name 
group by t1.newest_name having count(t1.newest_name)>1 ;   --- 787


select newest_name from odsdb.ori_newest_provide_sche onps group by newest_name;  -- 14658


select * from odsdb.ori_newest_provide_sche where provide_sche like '%详询%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%，详询。%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%详询%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%详情%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%微信号%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%免费电话：400-%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%具体售楼热线：400%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%，。%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%，详情请咨询下方经纪人。%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%经纪人%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%搜狐%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%贝壳%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%房天下%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%吉屋%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%，。%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%更多%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%售楼处%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%全文%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%搜狐%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%贝壳%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%房天下%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%吉屋%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%吉屋网%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%贝壳网%';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%,';
select * from odsdb.ori_newest_provide_sche where provide_sche like '%，';
select right(provide_sche,2) c from odsdb.ori_newest_provide_sche group by right(provide_sche,2);
select right(provide_sche,2) from odsdb.ori_newest_provide_sche where right(provide_sche,2) = '!。';
select * from  odsdb.ori_newest_provide_sche dnps where length(provide_sche)<6;


--------------------------------------------- =========================== ---------------------------------------------------


truncate table dwb_db.dwb_newest_issue_offer ;


CREATE TABLE odsdb.ori_newest_provide_sche (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(500) COMMENT '楼盘id',
  `newest_name`varchar(500) COMMENT '数据网址',
  `sche_tag` varchar(500) COMMENT 'tag标签',
  `provide_title` varchar(500) COMMENT 'title标题',
  `provide_date` varchar(500) COMMENT 'date发布时间',
  `provide_sche` varchar(500) COMMENT 'content内容',
  `date_clean` date COMMENT '时间清洗结果',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`)
)  ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态原始信息表';
drop table temp_db.tmp_newest_provide_sche;

select provide_title from temp_db.tmp_newest_provide_sche where length(provide_title)=17 group by provide_title;
-- 13
-- 14
-- 15
-- 16
-- 17



insert into odsdb.ori_newest_provide_sche (url,newest_name,sche_tag,provide_title,provide_date,provide_sche,date_clean,create_time,update_time) 
select newest_id,newest_name,`date`,peroiod,provide_title,provide_sche,
       str_to_date(replace(replace(replace(replace(
         case 
         when length(provide_title)=16 and provide_title not like '%年%' then substr(provide_title,1,10)
         when length(provide_title)=15 and provide_title not like '%年%' then substr(provide_title,1,10)
         when length(provide_title)=14 then substr(provide_title,1,9) 
         when length(provide_title)=13 then substr(provide_title,1,8) 
         when provide_title='' then '9999-09-09' 
         when provide_title='待定' then '9999-09-09' 
         else provide_title end
         ,'年','-'),'月','-'),'日',''),'/','-'),'%Y-%m-%d') date_clean, 
       now() create_time,now()  update_time
from temp_db.tmp_newest_provide_sche; 



--------------------------------------------- =========================== ---------------------------------------------------

select right(issue_area,3) from odsdb.city_newest_deal cnd where city_name = '肇庆' group by right(issue_area,3) ;


select issue_area from odsdb.city_newest_deal cnd where city_name = '肇庆' group by issue_area ;


select substr(issue_area,1,3) from odsdb.city_newest_deal cnd where city_name = '肇庆' group by substr(issue_area,1,3) ;


select room_sum from odsdb.city_newest_deal cnd where city_name = '珠海' group by room_sum;

--------------------------------------------- =========================== ---------------------------------------------------





truncate table odsdb.city_newest_deal ;

drop table odsdb.city_newest_deal ;

CREATE TABLE odsdb.city_newest_deal (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) DEFAULT NULL COMMENT '网址',
  `city_name` varchar(32) NOT NULL COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) DEFAULT NULL COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(320) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格  (元/㎡)',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `insert_time` varchar(200) DEFAULT NULL COMMENT '插入时间',
  PRIMARY KEY (`id`,`city_name`),
  KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
  KEY `idx_newest_city_name` (`city_name`) USING BTREE,
  KEY `idx_newest_url` (`url`) USING BTREE,
  KEY `idx_newest_floor_name` (`floor_name`) USING BTREE,
  KEY `idx_newest_insert_time` (`insert_time`) USING BTREE,
  KEY `idx_issue_date_clean` (`issue_date_clean`) USING BTREE,
  KEY `idx_issue_code` (`issue_code`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 KEY_BLOCK_SIZE=8 COMMENT='新楼盘交易数据表' PARTITION BY LINEAR KEY (city_name) PARTITIONS 128 ;



--------------------------------------------- =========================== ---------------------------------------------------








insert into dws_db.dws_supply select city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,now() ,1,create_time,follow_people_num,cityid,`quarter` 
from dws_db.dws_supply where period_index = 2 and dr = 0;


select city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,now() ,1,create_time,follow_people_num,cityid,`quarter` 
from dws_db.dws_supply where period_index = 2 and dr = 0;


update dws_db.dws_supply a , dwb_db.dwb_newest_city_qua b set a.follow_people_num = b.follow 
  where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.`month` 
    and b.`month` is not null and b.dr = 0 and b.city_id != b.county_id
    and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0;
   
   
select a.cityid ,a.city_id,a.quarter from dws_db.dws_supply a , dwb_db.dwb_newest_city_qua b 
 where a.city_id = b.county_id and a.cityid = b.city_id and a.period = b.`month` 
    and b.`month` is not null and b.dr = 0 and b.city_id != b.county_id
    and a.city_county_index = 2 and a.period_index = 2 and a.dr = 0;
   
    
select cityid,city_id,quarter,follow_people_num from dws_db.dws_supply a where a.city_county_index = 2 and a.period_index = 2 and a.dr = 0 and city_id in ('440301','440101');

update dws_db.dws_supply set dr =1 where city_county_index = 2 and period_index = 2 and dr = 0 and city_id in ('440301','440101');

update dws_db.dws_supply set follow_people_num = '-' where follow_people_num = '0';


show create table dws_db.dws_newest_info ;
CREATE TABLE dws_db.bak_20210811_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT NULL COMMENT '占地面积',
  `building_area` double DEFAULT NULL COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 -- > 2021-08-11备份';
insert into dws_db.bak_20210811_dws_newest_info select * from dws_db.dws_newest_info ;


select park_rate from dws_db.dws_newest_info dni group by park_rate ;

select park_num from dws_db.bak_20210811_dws_newest_info dni group by park_num ;


select volume_rate from dws_db.dws_newest_info dni group by volume_rate ;

select household_num from dws_db.dws_newest_info dni group by household_num ;

    
select land_area from dws_db.dws_newest_info dni group by land_area ;

select building_area from dws_db.dws_newest_info dni group by building_area ;


select * from dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id and a.dr = 0 ;

update dws_db.dws_newest_info a , dws_db.dws_newest_planinfo b set b.park_num = a.park_num where a.newest_id = b.newest_id and a.newest_name = b.newest_name and a.city_id = b.city_id;






--------------------------------------------- =========================== ---------------------------------------------------







SELECT * FROM dws_db.dws_customer_week WHERE newest_id IN (SELECT newest_id FROM dws_db.dws_newest_info WHERE newest_name LIKE '%嘉福•樾府%');

select * from dws_db.dws_customer_week where newest_id = '94817e7f7ccac282df942c77d6ec27aa'

select idate from odsdb.cust_browse_log_202004_202103 cbl where idate between group by idate order by idate desc;

select t.newest_id from 
(select newest_id from dws_db.dws_customer_week dcw where period = '2021q2' group by newest_id,period,exist,week having COUNT(1) = 1 and exist = '增量') t group by t.newest_id;

select city_id ,count(1) from dwb_db.dwb_newest_city_qua dncq where dr=0 and month is not null and city_id!=county_id and quarter = '2021Q2' group by city_id ;

select length(newest_name) from dws_db.dws_newest_info dni group by length(newest_name);





--------------------------------------------- =========================== ---------------------------------------------------






CREATE TABLE dwb_db.dwb_dim_geography_55city (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `province_id` varchar(64) COMMENT '城市id',
  `province_name` varchar(32) COMMENT '城市名称',
  `city_id` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `city_name` varchar(200) COMMENT '原始项目名称',
  `region_id` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `region_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `city_level_desc` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `dr` varchar(200) DEFAULT NULL COMMENT '地址',
  `create_time` varchar(200) DEFAULT NULL COMMENT '公司',
  `update_time` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  PRIMARY KEY (`id`,`city_name`),
KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
KEY `idx_newest_city_name` (`city_name`) USING BTREE,
KEY `idx_newest_url` (`url`) USING BTREE,
KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (id) PARTITIONS 8 ;








CREATE TABLE dwb_db.dwb_dim_geography_55city (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `province_id` varchar(64) COMMENT '城市id',
  `province_name` varchar(32) COMMENT '城市名称',
  `city_id` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `city_name` varchar(200) COMMENT '原始项目名称',
  `region_id` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `region_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `city_level_desc` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `dr` varchar(200) DEFAULT NULL COMMENT '地址',
  `create_time` varchar(200) DEFAULT NULL COMMENT '公司',
  `update_time` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  PRIMARY KEY (`id`,`city_name`),
KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
KEY `idx_newest_city_name` (`city_name`) USING BTREE,
KEY `idx_newest_url` (`url`) USING BTREE,
KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (id) PARTITIONS 8 ;

--------------------------------------------- =========================== ---------------------------------------------------





select * from 
  (select city_name,region_name from dws_db.dim_geography where city_name in ('上海市','广州市','北京市','深证市') and grade = 4) t1
left join 
  (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '') t2
on t1.city_name= t2.city_name and t1.region_name = t2.county_name ;

select a.city_name,a.region_name,a.202102,b.202103,c.202104,d.202105,e.202106 from 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202102` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('北京市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202102') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) a;
 
 
 select a.city_name,a.region_name,a.202007,b.202008,c.202009,d.202010,e.202011,f.202012 from 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202007` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202007') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) a
left join 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202008` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202008') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) b 
on a.city_name = b.city_name and a.region_name = b.region_name
left join 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202009` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202009') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) c 
on a.city_name = c.city_name and a.region_name = c.region_name
left join 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202010` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202010') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) d 
on a.city_name = d.city_name and a.region_name = d.region_name
left join 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202011` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202011') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) e 
on a.city_name = e.city_name and a.region_name = e.region_name
left join 
  (select t1.*,case when t2.city_name is null then '×' else '√' end `202012` from 
    (select city_name,region_name from dws_db.dim_geography where city_name in ('深圳市') and grade = 4) t1
  left join 
    (select city_name,county_name,period from dws_db.dws_supply ds where dr = 0 and period_index = 2 and  city_county_index != 1 and value != '-'  and value != '' and period = '202012') t2
  on t1.city_name= t2.city_name  and t1.region_name = t2.county_name ) f 
on a.city_name = f.city_name and a.region_name = f.region_name;

--------------------------------------------- =========================== ---------------------------------------------------



show create table dws_db.bak_20210727_dws_supply ;
CREATE TABLE dwb_db.dwb_issue_supply (
  `city_name` varchar(64) NOT NULL COMMENT '城市名称',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `period` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `quarter` varchar(40) DEFAULT NULL COMMENT '对应季度',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `follow_people_num` varchar(8) DEFAULT NULL COMMENT '关注人数',
  `cityid` int(11) DEFAULT NULL COMMENT '城市id',
  `local_issue_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前预售证的供应数量的加和',
  `local_room_sum_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前房间数量',
  `cric_value` varchar(20) DEFAULT NULL COMMENT 'CRIC供应数量',
  `value_from_index` varchar(8) DEFAULT NULL COMMENT '供应套数来源标识',
  `county_name_merge` varchar(322) DEFAULT NULL COMMENT '区域名称同步',
  `city_county_index` varchar(8) DEFAULT NULL COMMENT '城市区域维度标识',
  `period_index` varchar(8) DEFAULT NULL COMMENT '周期维度标识',
  `create_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '数据创建时间',
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_city_name` (`city_name`) USING BTREE,
  KEY `idx_supply_county_name` (`county_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市/区域供应套数表';



select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dim_geography where city_name in ('南昌市','重庆市','天津市','常州市','绍兴市','郑州市','咸阳市','成都市','南通市','石家庄市','深圳市','北京市','三亚市','杭州市','贵阳市','西安市','武汉市','济南市','合肥市','肇庆市','青岛市','惠州市','长春市','珠海市','扬州市','广州市','南京市','沈阳市','徐州市','苏州市','上海市','保定市','宁波市','湖州市','赣州市','烟台市','济宁市','汕头市','昆明市','宝鸡市','佛山市','福州市','海口市','嘉兴市','九江市','丽水市','南宁市','厦门市','唐山市','温州市','无锡市','长沙市','淄博市') and grade = 4
union all 
select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dim_geography where city_name in ('东莞市','中山市') and grade = 3;



select * from 
    (select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dws_area_detail) t1 
left join 
    (select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dim_geography 
         where city_name in ('南昌市','重庆市','天津市','常州市','绍兴市','郑州市','咸阳市','成都市','南通市','石家庄市','深圳市','北京市','三亚市','杭州市','贵阳市','西安市','东莞市','武汉市','济南市','合肥市','肇庆市','青岛市','惠州市','长春市','珠海市','扬州市','广州市','南京市','沈阳市','徐州市','苏州市','上海市','保定市','宁波市','湖州市','赣州市','烟台市','济宁市','汕头市','昆明市','宝鸡市','佛山市','福州市','海口市','嘉兴市','九江市','丽水市','南宁市','厦门市','唐山市','温州市','无锡市','长沙市','中山市','淄博市') 
         and grade = 4) t2
on t1.province_id = t2.province_id and t1.province_name = t2.province_name and t1.city_id = t2.city_id and t1.city_name = t2.city_name and t1.region_id = t2.region_id and t1.region_name = t2.region_name and t1.city_level_desc = t2.city_level_desc
where t2.province_id is null ;

select * from 
    (select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dws_area_detail) t1 
right join 
    (select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc from dws_db.dim_geography 
         where city_name in ('南昌市','重庆市','天津市','常州市','绍兴市','郑州市','咸阳市','成都市','南通市','石家庄市','深圳市','北京市','三亚市','杭州市','贵阳市','西安市','东莞市','武汉市','济南市','合肥市','肇庆市','青岛市','惠州市','长春市','珠海市','扬州市','广州市','南京市','沈阳市','徐州市','苏州市','上海市','保定市','宁波市','湖州市','赣州市','烟台市','济宁市','汕头市','昆明市','宝鸡市','佛山市','福州市','海口市','嘉兴市','九江市','丽水市','南宁市','厦门市','唐山市','温州市','无锡市','长沙市','中山市','淄博市') 
         and grade = 4) t2
on t1.province_id = t2.province_id and t1.province_name = t2.province_name and t1.city_id = t2.city_id and t1.city_name = t2.city_name and t1.region_id = t2.region_id and t1.region_name = t2.region_name and t1.city_level_desc = t2.city_level_desc
where t1.province_id is null ;




--------------------------------------------- =========================== ---------------------------------------------------







truncate table temp_db.city_newest_deal_data_check ;

delete from temp_db.tmp_city_newest_deal where city_name = '南昌';

select city_name ,gd_city ,floor_name ,issue_code , issue_date, issue_date_clean ,case when room_sum is null then 0 when room_sum = '' then 0 when room_sum = 'None' then 0 else replace(convert(room_sum using ascii),'?',0) end room_sum,building_code ,room_code ,case when issue_area = 'None' then 0 when issue_area = '' then 0 else substr(issue_area,1,8) end issue_area ,case when area = '' then 0 else area end area ,insert_time  from temp_db.tmp_city_newest_deal where issue_date_clean between '2018-01-01' and '2020-09-30' and city_name = '保定';

select city_name from temp_db.tmp_city_newest_deal group by city_name;

select issue_area from temp_db.tmp_city_newest_deal where city_name = '贵阳' group by issue_area ;

select city_name ,gd_city ,clean_floor_name ,issue_code , issue_date, issue_date_clean ,room_sum ,building_code ,room_code ,insert_time from temp_db.tmp_city_newest_deal where issue_date_clean between '2018-01-01' and '2020-09-30' and city_name = '北京';

select issue_date_clean from temp_db.tmp_city_newest_deal where city_name = '北京' group by issue_date_clean;

select count(1) from (
select url,city_name,gd_city,floor_name,address,business,issue_code,issue_date,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state 
from temp_db.city_newest_deal_data_check 
where city_name = '长春'
group by url,city_name,gd_city,floor_name,address,business,issue_code,issue_date,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state
) t;

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '';

select issue_date from temp_db.city_newest_deal_data_check where city_name = '贵阳' group by issue_date;

select open_date from temp_db.city_newest_deal_data_check where city_name = '贵阳' group by open_date;



select str_to_date(replace(case when issue_date = '' then '9999-09-09' when issue_date is null then '9999-09-09' else issue_date end,'/','-'),'%Y-%m-%d') from temp_db.city_newest_deal_data_check where city_name = '珠海';

delete from temp_db.tmp_city_newest_deal where city_name = '珠海' and substr(insert_time ,1,10) = '2021-08-06';



--------------------------------------------- =========================== ---------------------------------------------------








select a.newest_id,b.newest_name,a.period,b.city_id ,b.county_id,b.s from 
	(select city_id,period,newest_id from dws_db.dws_newest_period_admit dnpa where dr = 0) a
left join     
	(select city_id ,county_id,newest_id, newest_name ,household_num s from dws_db.dws_newest_info dni where dr =0) b  
on a.city_id = b.city_id and a.newest_id = b.newest_id
where b.county_id = '330326' ;

select version();


select t1.*,t2.s,t2.c from 
    (select city_name ,county_name ,period ,cityid ,city_id,sum(value) v from dws_db.dws_supply ds where city_county_index = 2 and dr =0  group by period,cityid ,city_id,city_name ,county_name) t1
inner join 
    (select a.period,b.city_id ,b.county_id,sum(b.s) s,count(b.newest_id) c from 
	    (select city_id,period,newest_id from dws_db.dws_newest_period_admit dnpa where dr = 0) a
	left join     
	    (select city_id ,county_id,newest_id ,household_num s from dws_db.dws_newest_info dni where dr =0) b  
	on a.city_id = b.city_id and a.newest_id = b.newest_id
	group by a.period,b.city_id ,b.county_id) t2
on t1.cityid = t2.city_id and t1.city_id = t2.county_id and t1.period = t2.period where t1.city_id = '130681';



select t1.*,t2.s,t2.c from 
    (select city_name ,county_name ,period ,cityid ,city_id,sum(value) v from dws_db.dws_supply ds where city_county_index = 2 and dr =0  group by period,cityid ,city_id,city_name ,county_name) t1
inner join 
    (select a.period,b.city_id ,b.county_id,sum(b.s) s,count(b.newest_id) c from 
	    (select city_id,period,newest_id from dws_db.dws_newest_period_admit dnpa where dr = 0) a
	left join     
	    (select city_id ,county_id,newest_id ,household_num s from dws_db.dws_newest_info dni where dr =0) b  
	on a.city_id = b.city_id and a.newest_id = b.newest_id
	group by a.period,b.city_id ,b.county_id) t2
on t1.cityid = t2.city_id and t1.city_id = t2.county_id and t1.period = t2.period where t1.v > t2.s;



select t1.*,t2.s from 
    (select city_name ,county_name ,cityid ,city_id,sum(value) v from dws_db.dws_supply ds where city_county_index = 2 and dr =0  group by cityid ,city_id,city_name ,county_name) t1
inner join 
    (select city_id ,county_id,sum(household_num) s from dws_db.dws_newest_info dni where dr =0  group by city_id ,county_id) t2
on t1.cityid = t2.city_id and t1.city_id = t2.county_id;

select t1.*,t2.s from 
    (select city_name ,county_name ,period ,cityid ,city_id,sum(value) v from dws_db.dws_supply ds where city_county_index = 2 and dr =0  group by period,cityid ,city_id,city_name ,county_name) t1
inner join 
    (select a.period,b.city_id ,b.county_id,sum(b.s) s from 
	    (select city_id,period,newest_id from dws_db.dws_newest_period_admit dnpa where dr = 0) a
	left join     
	    (select city_id ,county_id,newest_id ,household_num s from dws_db.dws_newest_info dni where dr =0) b  
	on a.city_id = b.city_id and a.newest_id = b.newest_id
	group by a.period,b.city_id ,b.county_id) t2
on t1.cityid = t2.city_id and t1.city_id = t2.county_id and t1.period = t2.period where t1.v > t2.s;


select household_num,count(1) from dws_db.dws_newest_info dnp group by household_num ;

select household_num,count(1) from dws_db.dws_newest_planinfo dnp group by household_num ;

select sum(household_num) from dws_db.dws_newest_info;

select sum(household_num) from dws_db.dws_newest_planinfo;




--------------------------------------------- =========================== ---------------------------------------------------




select * from dws_db.dws_newest_info where substr(recent_opening_time,1,4) > 2023 or substr(recent_opening_time,1,4) < 2000; 

update dws_db.dws_newest_info set recent_opening_time = null where substr(recent_opening_time,1,4) > 2023 or substr(recent_opening_time,1,4) < 2000;
update dws_db.dws_newest_info set recent_opening_time = replace(recent_opening_time,'09-31','10-01') where recent_opening_time like '%09-31%';
update dws_db.dws_newest_info set opening_date = str_to_date(recent_opening_time ,'%Y-%m-%d') where recent_opening_time != '';
update dws_db.dws_newest_info set recent_opening_time = opening_date;
update dws_db.dws_newest_info set opening_date = null where opening_date is not null ;
update dws_db.dws_newest_info set recent_opening_time = null where recent_opening_time = '';
select recent_opening_time from dws_db.dws_newest_info dni group by recent_opening_time ;
select recent_opening_time from dwb_db.dwb_newest_info dni group by recent_opening_time ;



update dws_db.dws_newest_info a , dwb_db.dwb_newest_info b set a.recent_opening_time = b.recent_opening_time where a.newest_id =b.newest_id and a.recent_opening_time is null and b.recent_opening_time != '';
select a.recent_opening_time,b.recent_opening_time from dws_db.dws_newest_info a , dwb_db.dwb_newest_info b where a.newest_id =b.newest_id and a.recent_opening_time is null and b.recent_opening_time != '';

--------------------------------------------- =========================== ---------------------------------------------------



select t1.*,t2.* from  
	(select
	    county_name ,
		city_id ,
		`quarter`,
		sum(value) v, 
		sum(follow_people_num) n from dws_db.dws_supply 
	where
		quarter != '2020Q3'
		and dr = 0
		and county_name not like '%辖区'
		and city_name in ('上海市', '北京市', '广州市', '深圳市')
		and period_index = 2
	group by
	    county_name ,
		city_id ,
		`quarter` ) t1
inner join 
	(select
		county_name ,
		city_id ,
		`quarter`,
		value v,
		follow_people_num n from dws_db.dws_supply
	where
		quarter != '2020Q3'
		and dr = 0
		and county_name not like '%辖区'
		and city_name in ('上海市', '北京市', '广州市', '深圳市')
		and period_index = 1) t2
on t1.county_name=t2.county_name and t1.city_id=t2.city_id and t1.`quarter`= t2.`quarter`;
		

--------------------------------------------- =========================== ---------------------------------------------------

update dws_db.dws_supply set dr = 1 where dr = 0 and period_index = 1 and follow_people_num = '-';


select * from dws_db.dws_supply ds where  quarter != '2020Q3' and dr = 0 and county_name not like '%辖区' and city_name in ('上海市','北京市','广州市','深圳市') and follow_people_num = '-';

select cityid,city_name,county_name,city_id,period,value,follow_people_num,cric_value,quarter from dws_db.dws_supply where dr = 0 and city_name in ('上海市','北京市','广州市','深圳市') and follow_people_num != '-' and value = '-';

update
	dws_db.dws_newest_popularity_rownumber_quarter a ,
	dws_db.dws_newest_popularity_rownumber_quarter b
set
	a.index_rate = b.index_rate
where
	a.newest_id = b.newest_id
	and a.period = b.period 
	and a.imei_newest = b.imei_newest 
	and a.imei_c_avg = '-999'
	and b.imei_c_avg != '-999'
    and a.dr = 0
    and b.dr = 0;

select * from dws_db.dws_newest_popularity_rownumber_quarter where imei_c_avg = '-999';

select * from dws_db.dws_newest_popularity_rownumber_quarter where imei_c_avg != '-999';

select * from dws_db.dws_newest_popularity_rownumber_quarter where newest_id in ('9a6ab320bf9c11eb86162cea7f6c2bde');

update dws_db.dws_newest_popularity_rownumber_quarter set dr = 1 where city_id in ('441900','442000') and imei_c_avg = '-999';


select * from 




show create table dws_db.dws_supply;
CREATE TABLE dws_db.bak_20210727_dws_supply (
  `city_name` varchar(64) NOT NULL COMMENT '城市名称',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `period` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `local_issue_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前预售证的供应数量的加和',
  `local_room_sum_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前房间数量',
  `cric_value` varchar(20) DEFAULT NULL COMMENT 'CRIC供应数量',
  `value_from_index` varchar(8) DEFAULT NULL COMMENT '供应套数来源标识',
  `county_name_merge` varchar(322) DEFAULT NULL COMMENT '区域名称同步',
  `city_county_index` varchar(8) DEFAULT NULL COMMENT '城市区域维度标识',
  `period_index` varchar(8) DEFAULT NULL COMMENT '周期维度标识',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `follow_people_num` varchar(8) DEFAULT NULL COMMENT '关注人数',
  `cityid` int(11) DEFAULT NULL COMMENT '城市id',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_city_name` (`city_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市/区域供应套数表 -- 20210804 备份';

--------------------------------------------- =========================== ---------------------------------------------------

update dws_db.bak_20210727_dws_supply set period = replace(period,'-','') where not isnull(period) and period  != '';

update
	dws_db.dws_supply a ,
	dws_db.bak_20210727_dws_supply b
set
	a.value = b.value
where
	a.city_id = b.city_id
	and a.period = b.period 
	and b.dr = 0
	and a.dr = 0
    and a.city_name in ('北京市','上海市','广州市','深圳市')
    and b.city_name in ('北京市','上海市','广州市','深圳市') 
    and b.period = '202010'
    and a.period = '202010';

truncate table dws_db.dws_supply ;

update dws_db.dws_supply set `quarter` = period where period_index = 1;
update dws_db.dws_supply set `quarter` = '2020Q3' where period_index = 2 and period between '202007' and '202009';
update dws_db.dws_supply set `quarter` = '2020Q4' where period_index = 2 and period between '202010' and '202012';
update dws_db.dws_supply set `quarter` = '2021Q1' where period_index = 2 and period between '202101' and '202103';
update dws_db.dws_supply set `quarter` = '2021Q2' where period_index = 2 and period between '202104' and '202106';



select t1.*,t2.* from 
    (select city_id,'1' from dws_db.dws_supply ds where  dr = 0  and period_index = 2 and city_id != cityid ) t1 
right join 
    (select county_id,'2' from dwb_db.dwb_newest_city_qua dncq where `month`  is not null and dr = 0 and city_id in ('310000','110000','440300','440100') and city_id != county_id) t2
on t1.city_id=t2.county_id;

select period,sum(follow_people_num),cityid from dws_db.dws_supply ds where period_index = 2 and city_county_index = 2 and dr = 0 group by cityid,period ;


--------------------------------------------- =========================== ---------------------------------------------------

update dwb_db.dwb_newest_city_qua set dr = 1 ;
update dwb_db.dwb_newest_city_qua set `month` = replace(`month`,'-','') where not isnull(`month`) and `month`  != '';
update dwb_db.dwb_newest_city_qua set `month` = null where `month` = '';
update dws_db.dws_supply set follow_people_num = '-' where follow_people_num = '0';

insert
	into
	dwb_db.dwb_newest_city_qua (city_id,
	county_id,
	`quarter`,
	for_sale,
	on_sale,
	sell_out,
	total_count,
	follow,
	intention,
	urgent,
	increase,
	retained,
	unit_price,
	create_time,
	update_time,
	dr,
	`month`)
select cityid,city_id,'2020Q3',0,0,0,0,case when follow_people_num = '-' then 0 else follow_people_num end,0,0,0,0,0,create_time,update_time,dr,period from dws_db.dws_supply where period_index = 2 and  period < '202010';






--------------------------------------------- =========================== ---------------------------------------------------


select 	land_area	 from dws_db.dws_newest_info group by 	land_area	;
select 	building_area	 from dws_db.dws_newest_planinfo dnp group by 	building_area	;
select 	volume_rate	 from dws_db.dws_newest_info group by 	volume_rate	;
select 	household_num	 from dws_db.dws_newest_planinfo group by 	household_num	;
select 	park_rate	 from dws_db.dws_newest_planinfo group by 	park_rate	;
select 	park_num	 from dws_db.dws_newest_planinfo group by 	park_num	;
select 	right_term	 from dws_db.dws_newest_planinfo group by 	right_term	;
select 	green_rate	 from dws_db.dws_newest_planinfo group by 	green_rate	;
select 	decoration	 from dws_db.dws_newest_planinfo group by 	decoration	;
select 	building_type	 from dws_db.dws_newest_planinfo group by 	building_type	;
select 	floor_num	 from dws_db.dws_newest_planinfo group by 	floor_num	;
select 	building_num	 from dws_db.dws_newest_planinfo group by 	building_num	;
select 	volume_rate 	 from dws_db.dws_newest_info group by 	volume_rate	;
select 	land_area 	 from dws_db.dws_newest_planinfo group by 	land_area	;





--------------------------------------------- =========================== ---------------------------------------------------

select distinct imei from dwb_db.dwb_customer_browse_log dcbl where newest_id = '87d98493e706c2b2b243d2b75fff2a33' and visit_date > 20210101 and visit_date < 20210401;


--------------------------------------------- =========================== ---------------------------------------------------


select  * from dws_newest_investment_pop_rownumber_quarter dniprq where newest_id = 'c98c10ab23e4446579e6e848905cc8ee';
select  * from dws_db.dws_newest_popularity_rownumber_quarter dnprq where newest_id = 'c98c10ab23e4446579e6e848905cc8ee';


select newest_name ,land_area ,building_area from dws_db.dws_newest_info dni where city_id = '360400' and newest_name = '山语城';
select newest_name ,land_area ,building_area from dws_db.dws_newest_planinfo dni where city_id = '360400' and newest_name = '山语城';


--------------------------------------------- =========================== ---------------------------------------------------



delete from dws_db.dws_supply where dr = 0 and period_index = 2;


--------------------------------------------- =========================== ---------------------------------------------------







select * from dws_db.dws_newest_city_qua dncq where unit_price < 1000;

select count(1) from 
    (select newest_id ,city_id,sales_state from dws_db.dws_newest_info dni where sales_state = '在售' and county_id = '310115') t1
inner join 
    (select * from dws_db.dws_newest_period_admit dnpa where dr = 0 and period = '2020Q4') t2
on t1.newest_id = t2.newest_id;

update dws_db.dws_newest_city_qua set dr = 1;
update dws_newest_offer_rate set dr = 1 where period = '2021Q2';

insert into dws_db.dws_newest_offer_rate (city_id,offer,rate,period,create_time,update_time,dr)
select ds.city_id,value,case when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end rate,ds.period,now(),now(),'0' from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null and period='2021Q2' and dr = 0 ) dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1 and period = '2021Q2' and dr = 0 ) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period
union all 
select ds2.city_id,value,case when value='-' then '-' when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds2.period,now(),now(),'0' from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null and period='2021Q2' and dr = 0 ) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1 and period='2021Q2' and dr = 0 ) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period;



--------------------------------------------- =========================== ---------------------------------------------------

update dws_db.dws_newest_planinfo set building_area = null  where building_area = '-1' ;
update dws_db.dws_newest_planinfo set land_area = null  where land_area = '-1' ;
update dws_db.dws_newest_planinfo set household_num = null  where household_num = '-1' ;
update dws_db.dws_newest_planinfo set volume_rate = null  where volume_rate = '-1' ;


select	building_area	from dws_db.dws_newest_planinfo dnp where building_area < 20000 group by	building_area	;
select	building_num	from dws_db.dws_newest_planinfo dni group by	building_num	;
select	building_type	from dws_db.dws_newest_planinfo dni group by	building_type	;
select	household_num	from dws_db.dws_newest_planinfo dni group by	household_num	;
select	land_area	from dws_db.dws_newest_info dni group by	land_area	order by land_area;
select	opening_date	from dws_db.dws_newest_planinfo dni group by	opening_date	;
select	park_num	from dws_db.dws_newest_planinfo dni group by	park_num	;
select	park_rate	from dws_db.dws_newest_planinfo dni group by	park_rate	;
select	recent_opening_time	from dws_db.dws_newest_planinfo dni group by	recent_opening_time	;
select	unit_price	from dws_db.dws_newest_planinfo dni group by	unit_price	;
select	volume_rate	from dws_db.dws_newest_planinfo dni where volume_rate <1 group by	volume_rate	;
select newest_id , city_id ,newest_name ,household_num ,building_area ,land_area from dws_db.dws_newest_planinfo dni where household_num  != -1 and building_area/household_num  > 6000 and building_area != -1 or household_num < 10;
select newest_id , city_id ,newest_name ,household_num ,building_area ,land_area from dws_db.dws_newest_planinfo dni where household_num < 10 and household_num != -1;


select * from dws_db.dws_newest_info where newest_name = '汤臣一品';
select * from dws_db.dws_newest_bool_distance where newest_id = '185faedeb773b0f9b69899bb6ddfdb00';
select * from dws_db.dws_newest_mean_distance where newest_id = '185faedeb773b0f9b69899bb6ddfdb00';
select * from dws_db.dws_customer_address where newest_id = '185faedeb773b0f9b69899bb6ddfdb00' and period = '2020Q4' ;

update dws_db.dws_newest_planinfo set household_num = -1 where household_num  != -1 and building_area/household_num  > 6000 and building_area != -1 or household_num < 10;
update dws_db.dws_newest_info set household_num = -1 where household_num  != -1 and building_area/household_num  > 6000 and building_area != -1 or household_num < 10;
update dws_db.dws_newest_info set building_type = '' where building_type = '0';
update dws_db.dws_newest_info set building_type = '板楼' where building_type = ',,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,板楼,,,,,,,,,,,,,,,,,,,,,,,,,,,,';

select t1.*,t2.* from 
   (select county_id,'1' i from dws_db.dws_newest_city_qua dncq where county_id is not null and period = '2021Q2' and dr = 0 and city_id in ('440600','460100','320100','320600','340100','330400','440100','510100','210100','350100','410100')
   ) t1
left join 
   (select city_id,'2' i from dws_db.dws_supply ds where city_id != cityid and period = '2021Q2' and dr = 0 and city_name in ('佛山市','南京市','南通市','合肥市','嘉兴市','广州市','成都市','沈阳市','海口市','郑州市','福州市')
   ) t2
  on t1.county_id = t2.city_id where t2.i is null ;   

--------------------------------------------- =========================== ---------------------------------------------------





update dws_db.dws_newest_city_qua set dr = 1 where period = '2021Q2';
update dws_db.dws_newest_city_qua set dr = 0 where dr is null;
update dws_db.dws_newest_city_qua set create_time = now() where dr is null;
update dws_db.dws_newest_city_qua set update_time = now() where dr is null;

delete from dws_db.dws_newest_city_qua where period = '2021Q2';




--------------------------------------------- =========================== ---------------------------------------------------








show create table dws_db.dws_supply ;
CREATE TABLE bak_20210729_dws_supply (
  `city_name` varchar(64) NOT NULL COMMENT '城市名称',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `period` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `local_issue_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前预售证的供应数量的加和',
  `local_room_sum_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前房间数量',
  `cric_value` varchar(20) DEFAULT NULL COMMENT 'CRIC供应数量',
  `value_from_index` varchar(8) DEFAULT NULL COMMENT '供应套数来源标识',
  `county_name_merge` varchar(322) DEFAULT NULL COMMENT '区域名称同步',
  `city_county_index` varchar(8) DEFAULT NULL COMMENT '城市区域维度标识',
  `period_index` varchar(8) DEFAULT NULL COMMENT '周期维度标识',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `follow_people_num` varchar(8) DEFAULT NULL COMMENT '关注人数',
  `cityid` int(11) DEFAULT NULL COMMENT '城市id',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_city_name` (`city_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市/区域供应套数表 --- > 20210729 备份表';



--------------------------------------------- =========================== ---------------------------------------------------








select str_to_date(recent_opening_time ,'%Y-%m-%d') from dws_db.dws_newest_info where recent_opening_time like '%09-31%';

select * from dws_db.dws_newest_info dni where recent_opening_time like '%09-31%';


update dws_db.dws_newest_info set recent_opening_time = replace(recent_opening_time,'09-31','10-01') where recent_opening_time like '%09-31%';

update dws_db.dws_newest_info set opening_date = str_to_date(recent_opening_time ,'%Y-%m-%d') where recent_opening_time != '';

update dws_db.dws_newest_info set recent_opening_time = opening_date;

update dws_db.dws_newest_info set opening_date = null ;

--------------------------------------------- =========================== ---------------------------------------------------





update dwb_db.dwb_newest_city_qua set county_id = null where county_id = '' or county_id is null;
update dwb_db.dwb_newest_city_qua set unit_price = null where unit_price = 0;
update dwb_db.dwb_newest_city_qua set month = null where month = '' or month is null;
update dws_db.dws_supply set follow_people_num = '-' where follow_people_num = 0 or follow_people_num is null;
update dws_db.dws_supply set value = '-' where value = '0' ;


select count(imei),city_id ,county_id ,substr(idate,1,6) `month` from  
(
	select imei ,city_id ,county_id ,max(idate) idate from 
	  (select customer imei,clean_floor_name,idate from odsdb.cust_browse_log_201801_202106 where idate >= '20200701' and idate < '20201001' and clean_floor_name is not null) tt1
	left join 
	  (
	  select t1.newest_id ,newest_name ,city_id ,county_id from 
	    (select newest_id,newest_name,city_id,county_id from dws_db.dws_newest_info) t1
	  inner join 
	    (select newest_id from dws_db.dws_newest_period_admit where period = '2020Q4') t2
	   on t1.newest_id = t2.newest_id
	  )tt2 
	 on tt1.clean_floor_name = tt2.newest_name group by imei ,city_id ,county_id
 ) a group by city_id ,county_id ,substr(idate,1,6)
union all 
select count(imei),city_id ,city_id county_id ,substr(idate,1,6) `month` from 
(
	select imei ,city_id ,city_id county_id ,max(idate) idate from 
	  (select customer imei,clean_floor_name,idate from odsdb.cust_browse_log_201801_202106 where idate >= '20200701' and idate < '20201001' and clean_floor_name is not null) tt1
	left join 
	  (
	  select t1.newest_id ,newest_name ,city_id from 
	    (select newest_id,newest_name,city_id from dws_db.dws_newest_info) t1
	  inner join 
	    (select newest_id from dws_db.dws_newest_period_admit where period = '2020Q4') t2
	   on t1.newest_id = t2.newest_id
	  )tt2 
	 on tt1.clean_floor_name = tt2.newest_name group by imei ,city_id
) b group by city_id ,substr(idate,1,6);


select count(imei) ,city_name ,substr(idate,1,6) `month` ,'2' from 
(
  select customer imei ,max(idate) idate,city_name,region_name from odsdb.cust_browse_log_201801_202106 where idate >= '20200701' and idate < '20201001' 
  group by customer,city_name,region_name
) t group by city_name ,region_name ,substr(idate,1,6) having city_name in ('北京','上海','深圳','广州')
union all 
select count(imei) ,city_name ,substr(idate,1,6) `month` ,'1' from 
(
  select customer imei ,max(idate) idate,city_name from odsdb.cust_browse_log_201801_202106 where idate >= '20200701' and idate < '20201001' 
  group by customer,city_name
) t group by city_name ,substr(idate,1,6) having city_name in ('北京','上海','深圳','广州')
;


--------------------------------------------- =========================== ---------------------------------------------------




SELECT newest_id,city_id,county_id,imei,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date>='2020-10-01' and visit_date<'2021-01-01';
SELECT newest_id,city_id,county_id,imei,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date>='2020-07-01' and visit_date<'2020-10-01';


select STR_TO_DATE(open_date,'%Y-%m-%d') from temp_db.tmp_city_newest_deal where open_date like '%-%';

select * from dws_db.dws_newest_info;

show create table dws_db.dws_newest_info;
CREATE TABLE dws_db.bak_20210803_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 --> 20210803备份';
insert into dws_db.bak_20210803_dws_newest_info select * from dws_db.dws_newest_info;
update dws_db.bak_20210803_dws_newest_info set volume_rate = null  where volume_rate = -1;
update dws_db.bak_20210803_dws_newest_info set green_rate = null  where green_rate = -1;
update dws_db.bak_20210803_dws_newest_info set building_area = null  where building_area = -1;
update dws_db.bak_20210803_dws_newest_info set land_area = null  where land_area = -1;
update dws_db.bak_20210803_dws_newest_info set household_num = null  where household_num = -1;






select * from dws_newest_info;

drop table dws_db.dws_newest_info;

rename table dws_db.dws_newest_info to dws_db.dws_newest_info_20210803;
rename table dws_db.bak_20210803_dws_newest_info to dws_db.dws_newest_info;

--------------------------------------------- =========================== ---------------------------------------------------





show create table dws_db.dws_customer_cre;
CREATE TABLE dws_db.bak_20210728_dws_customer_cre (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留结果表 -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_customer_cre select * from dws_db.dws_customer_cre;
truncate table dws_db.dws_customer_cre;

show create table dws_db.dws_newest_popularity_rownumber_quarter;
CREATE TABLE  dws_db.bak_20210728_dws_newest_popularity_rownumber_quarter (
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
) ENGINE=InnoDB AUTO_INCREMENT=93534 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单表  -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_newest_popularity_rownumber_quarter select * from dws_db.dws_newest_popularity_rownumber_quarter;
truncate table dws_db.dws_newest_popularity_rownumber_quarter;
update dws_db.dws_newest_popularity_rownumber_quarter set create_time = now(); 
update dws_db.dws_newest_popularity_rownumber_quarter set update_time = now(); 
update dws_db.dws_newest_popularity_rownumber_quarter set dr = 0; 



show create table dws_db.dws_customer_month;
CREATE TABLE dws_db.bak_20210728_dws_customer_month (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `month` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留月度结果表  -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_customer_month select * from dws_db.dws_customer_month;
truncate table dws_db.dws_customer_month;


show create table dws_db.dws_customer_sum;
CREATE TABLE dws_db.bak_20210728_dws_customer_sum (
  `city_id` text COMMENT '城市id',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼盘id',
  `period` text COMMENT '季度周期',
  `cou_imei` bigint(20) DEFAULT NULL COMMENT '当前区域浏览人数',
  `city_avg` double DEFAULT NULL COMMENT '当前城市浏览人数平均值',
  `ratio` double DEFAULT NULL COMMENT '占比',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户总量结果表 -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_customer_sum select * from dws_db.dws_customer_sum;
truncate table dws_db.dws_customer_sum;




show create table dws_db.dws_customer_week;
CREATE TABLE dws_db.bak_20210728_dws_customer_week (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `week` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留周度结果表 -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_customer_week select * from dws_db.dws_customer_week;
truncate table dws_db.dws_customer_week;




show create table dws_db.dws_newest_city_qua;
CREATE TABLE dws_db.bak_20210728_dws_newest_city_qua (
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区域',
  `period` varchar(255) NOT NULL COMMENT '周期',
  `for_sale` int(6) NOT NULL DEFAULT '0' COMMENT '待售数量',
  `on_sale` int(6) NOT NULL DEFAULT '0' COMMENT '在售数量',
  `sell_out` int(6) NOT NULL DEFAULT '0' COMMENT '售罄数量',
  `total_count` int(6) DEFAULT '0' COMMENT '项目总量',
  `follow` int(6) NOT NULL DEFAULT '0' COMMENT '关注楼盘数量',
  `intention` int(6) NOT NULL DEFAULT '0' COMMENT '意向选房数量',
  `urgent` int(6) NOT NULL DEFAULT '0' COMMENT '迫切买房数量',
  `increase` int(6) NOT NULL DEFAULT '0' COMMENT '当季新增',
  `retained` int(6) NOT NULL DEFAULT '0' COMMENT '当季留存',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(11) DEFAULT NULL COMMENT '0有效;1无效'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市项目总量与意向客户统计 -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_newest_city_qua select * from dws_db.dws_newest_city_qua;
truncate table dws_db.dws_newest_city_qua;
update dws_db.dws_newest_city_qua set create_time = now(); 
update dws_db.dws_newest_city_qua set update_time = now(); 
update dws_db.dws_newest_city_qua set dr = 0; 




show create table dws_db.dws_newest_investment_pop_rownumber_quarter;
CREATE TABLE dws_db.bak_20210728_dws_newest_investment_pop_rownumber_quarter (
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
) ENGINE=InnoDB AUTO_INCREMENT=211675 DEFAULT CHARSET=utf8mb4 COMMENT='投资型人气热度排行榜单表 -- > 20210728 备份';
insert into dws_db.bak_20210728_dws_newest_investment_pop_rownumber_quarter select * from dws_db.dws_newest_investment_pop_rownumber_quarter;
truncate table dws_db.dws_newest_investment_pop_rownumber_quarter;
update dws_db.dws_newest_investment_pop_rownumber_quarter set create_time = now(); 
update dws_db.dws_newest_investment_pop_rownumber_quarter set update_time = now(); 
update dws_db.dws_newest_investment_pop_rownumber_quarter set dr = 0; 








truncate table dws_db.dws_newest_investment_pop_rownumber_quarter;
insert into dws_db.dws_newest_investment_pop_rownumber_quarter select * from dws_db.bak_20210728_dws_newest_investment_pop_rownumber_quarter;


update dws_db.dws_supply set period = replace(period,'-','') where period_index = 2;



SELECT newest_id,city_id,county_id,imei,visit_date FROM dwb_db.dwb_customer_browse_log where visit_date>=2020-10-01 and visit_date<2021-01-01
--------------------------------------------- =========================== ---------------------------------------------------








show create table dws_db.dws_newest_city_qua ;
CREATE TABLE dwb_db.dwb_newest_city_qua (
  `id` int(11) NOT NULL auto_increment COMMENT '自增id',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区域',
  `quarter` varchar(255) NOT NULL COMMENT '季度',
  `for_sale` int(6) NOT NULL DEFAULT '0' COMMENT '待售数量',
  `on_sale` int(6) NOT NULL DEFAULT '0' COMMENT '在售数量',
  `sell_out` int(6) NOT NULL DEFAULT '0' COMMENT '售罄数量',
  `total_count` int(6) DEFAULT '0' COMMENT '项目总量',
  `follow` int(6) NOT NULL DEFAULT '0' COMMENT '关注楼盘数量',
  `intention` int(6) NOT NULL DEFAULT '0' COMMENT '意向选房数量',
  `urgent` int(6) NOT NULL DEFAULT '0' COMMENT '迫切买房数量',
  `increase` int(6) NOT NULL DEFAULT '0' COMMENT '当季新增',
  `retained` int(6) NOT NULL DEFAULT '0' COMMENT '当季留存',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(11) DEFAULT NULL COMMENT '0有效;1无效',
  `month` varchar(24) DEFAULT NULL COMMENT '月份',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_county_id` (`county_id`) USING BTREE,
  KEY `idx_supply_quarter` (`quarter`) USING BTREE,
  KEY `idx_supply_month` (`month`) USING BTREE,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市项目总量与意向客户统计';






--------------------------------------------- =========================== ---------------------------------------------------







update dws_db.dws_newest_city_qua set create_time = now(); 
update dws_db.dws_newest_city_qua set update_time = now(); 
update dws_db.dws_newest_city_qua set dr = 0; 






insert into dws_db.dws_supply select * from dws_db.bak_20210727_dws_supply where period_index =2;






--------------------------------------------- =========================== ---------------------------------------------------









select a.*,b.* from dws_db.dws_customer_sum a join (select newest_id,newest_name,city_id from dws_newest_info where (city_id,newest_name) in (
select city_id,newest_name from dws_db.dws_newest_info where newest_id in (select distinct newest_id from dws_newest_period_admit where dr = 0) group by city_id,newest_name having count(*) >1)) b on a.newest_id = b.newest_id ;


select * from dws_db.dws_newest_info where substr(city_id,1,4) != substr(county_id,1,4) and city_id not in ('110000','120000','310000','500000','460400');

select * from dws_db.dws_newest_info where substr(county_id,5,2) = '00' and city_id not in ('441900','442000');



show create table dws_db.dws_newest_info ;
CREATE TABLE dws_db.bak_20210728_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 -- > 20210728备份';

insert into dws_db.bak_20210728_dws_newest_info select * from dws_db.dws_newest_info;



--------------------------------------------- =========================== ---------------------------------------------------







select floor_name from temp_db.tmp_city_newest_deal tcnd where city_name = '扬州' group by floor_name ;

select * from dws_db.dws_newest_city_qua dncq2 where county_id is null ;  -- 152

select * from dws_db.dws_newest_city_qua dncq2 where county_id is not null ; -- 1266



show create table dws_db.dws_newest_offer_rate ;
CREATE TABLE dws_db.bak_20210727_dws_newest_offer_rate (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` text COMMENT '城市id',
  `offer` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `rate` varchar(20) DEFAULT NULL COMMENT '供需比',
  `period` text COMMENT '时间区间',
  `create_time` datetime DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  `dr` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=505 DEFAULT CHARSET=utf8mb4;
insert into dws_db.bak_20210727_dws_newest_offer_rate select * from dws_db.dws_newest_offer_rate;

select now(); 

insert into dws_db.dws_newest_offer_rate select * from  dws_db.bak_20210727_dws_newest_offer_rate;
update  dws_db.dws_newest_offer_rate set dr = '1' ;


truncate table dws_db.dws_newest_offer_rate;

insert
	into
	dws_db.dws_newest_offer_rate (city_id,
	offer,
	rate,
	period,
	create_time,
	update_time,
	dr)
values('110000','10394','7.9394843178756975','2021Q1','2021-07-27 16:56:22.0','2021-07-27 16:56:22.0','0');


delete from dws_db.dws_newest_offer_rate where dr = 0;


insert into dws_db.dws_newest_offer_rate (city_id,offer,rate,period,create_time,update_time,dr)
select ds.city_id,value,case when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds.period,now(),now(),'0' from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null and period='2021Q2') dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period 
union all 
select ds2.city_id,value,case when value='-' then '-' when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds2.period,now(),now(),'0' from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period;




select ds.city_id,value,follow,case when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds.period,now(),now(),'0' from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null) dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period 

union all 

select ds2.city_id,value,follow,case when value='-' then '-' when value=0 then '-' when value != 0 then ifnull(follow/value,'-') end,ds2.period,now(),now(),'0' from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period where follow is not null and value = '-';



select dncq2.city_id,ds2.city_name from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null) dncq2
right join 
  (select city_name,city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period where follow is not null and value = '-' group by dncq2.city_id,ds2.city_name;


select * from 


delete from dws_db.dws_supply where city_name in ('北京市','天津市','石家庄市','唐山市','保定市','沈阳市','长春市','上海市','南京市','无锡市','苏州市','南通市','扬州市','杭州市','宁波市','嘉兴市','湖州市','绍兴市','合肥市','福州市','厦门市','南昌市','赣州市','济南市','淄博市','烟台市','济宁市','郑州市','武汉市','长沙市','广州市','深圳市','南宁市','重庆市','成都市','贵阳市','昆明市','西安市','宝鸡市','咸阳市')





show create table dws_db.dws_supply ;
CREATE TABLE dws_db.bak_20210727_dws_supply (
  `city_name` varchar(64) NOT NULL COMMENT '城市名称',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `period` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `local_issue_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前预售证的供应数量的加和',
  `local_room_sum_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前房间数量',
  `cric_value` varchar(20) DEFAULT NULL COMMENT 'CRIC供应数量',
  `value_from_index` varchar(8) DEFAULT NULL COMMENT '供应套数来源标识',
  `county_name_merge` varchar(322) DEFAULT NULL COMMENT '区域名称同步',
  `city_county_index` varchar(8) DEFAULT NULL COMMENT '城市区域维度标识',
  `period_index` varchar(8) DEFAULT NULL COMMENT '周期维度标识',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_city_name` (`city_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市/区域供应套数表 -- > 2021年7月27号备份';
insert into dws_db.bak_20210727_dws_supply select * from dws_db.dws_supply;

truncate table dws_db.bak_20210727_dws_supply;




select period,count(1) from (
select ds.city_id,value,follow,ds.period from 
  (select city_id,follow,period from dws_db.dws_newest_city_qua where county_id is null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400')) dncq
right join 
  (select city_id,value,period from dws_db.dws_supply where city_county_index = 1 and period_index = 1) ds
on dncq.city_id=ds.city_id and dncq.period=ds.period and ds.period = '2021Q2'
where follow is not null 
union all 
select ds2.city_id,value,follow,ds2.period from 
  (select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where county_id is not null ) dncq2
right join 
  (select city_id,value,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1) ds2
on dncq2.county_id=ds2.city_id and dncq2.period=ds2.period where follow is not null and ds2.period = '2021Q2'
)tttt group by period;







select tt1.city_id,county_id,tt1.period from
(
select city_id,county_id,follow,period from dws_db.dws_newest_city_qua where period = '2021Q2' and county_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400')
) tt1
left join 
(
select city_id,period from dws_db.dws_supply ds where city_county_index = 2 and period_index = 1 and period = '2021Q2'
)tt2
on tt1.county_id = tt2.city_id where tt2.period is null ;


select
	city_id,
	county_id,
	period,
	for_sale,
	on_sale,
	sell_out,
	total_count,
	follow,
	intention,
	urgent,
	increase,
	retained,
	unit_price
from
	dws_db.dws_newest_city_qua dncq where period = '2020Q4'
group by
	city_id,county_id,period,for_sale,on_sale,sell_out,total_count,follow,intention,urgent,increase,retained,unit_price ;



--------------------------------------------- =========================== ---------------------------------------------------





update dws_db.dws_supply set create_time = now() ;

update dws_db.dws_newest_investment_pop_rownumber_quarter set create_time = now(); 
update dws_db.dws_newest_investment_pop_rownumber_quarter set update_time = now();
update dws_db.dws_newest_investment_pop_rownumber_quarter set dr = 0;

update dws_db.dws_newest_popularity_rownumber_quarter set create_time = now(); 
update dws_db.dws_newest_popularity_rownumber_quarter set update_time = now();
update dws_db.dws_newest_popularity_rownumber_quarter set dr = 0;







--------------------------------------------- =========================== ---------------------------------------------------





show create table dws_db.dws_newest_city_qua ;
CREATE TABLE bak_20210727_dws_newest_city_qua (
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区域',
  `period` varchar(255) NOT NULL COMMENT '周期',
  `for_sale` int(6) NOT NULL DEFAULT '0' COMMENT '待售数量',
  `on_sale` int(6) NOT NULL DEFAULT '0' COMMENT '在售数量',
  `sell_out` int(6) NOT NULL DEFAULT '0' COMMENT '售罄数量',
  `total_count` int(6) DEFAULT '0' COMMENT '项目总量',
  `follow` int(6) NOT NULL DEFAULT '0' COMMENT '关注楼盘数量',
  `intention` int(6) NOT NULL DEFAULT '0' COMMENT '意向选房数量',
  `urgent` int(6) NOT NULL DEFAULT '0' COMMENT '迫切买房数量',
  `increase` int(6) NOT NULL DEFAULT '0' COMMENT '当季新增',
  `retained` int(6) NOT NULL DEFAULT '0' COMMENT '当季留存',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(11) DEFAULT NULL COMMENT '0有效;1无效'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市项目总量与意向客户统计 --> 20210727 备份';
insert into dws_db.bak_20210727_dws_newest_city_qua select * from dws_db.dws_newest_city_qua ;
truncate table dws_db.dws_newest_city_qua;





select a111.*,dg.* from (
select * from dws_db.dws_newest_period_admit where newest_id  in (
select a.newest_id from dws_db.bak_20210721_dws_newest_info a 
 LEFT JOIN  dwb_db.dwb_newest_info_20210721 b 
 on a.newest_id = b.newest_id
 where (a.county_id = '' or a.county_id is null or a.county_id = 0)) )a111
left join (select city_id from dws_db.dws_area_detail group by city_id) dg 
on a111.city_id = dg.city_id where dg.city_id is null;


select newest_id,count(1) c from (
select * from dws_db.dws_newest_period_admit where newest_id  in (
select a.newest_id from dws_db.bak_20210721_dws_newest_info a 
 LEFT JOIN  dwb_db.dwb_newest_info_20210721 b 
 on a.newest_id = b.newest_id
 where (a.county_id = '' or a.county_id is null or a.county_id = 0)) )a111
 group by newest_id having c >= 2;





show create table dws_db.dws_customer_sum;
CREATE TABLE temp_db.bak_20210723_dws_customer_sum (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `period` text,
  `cou_imei` bigint(20) DEFAULT NULL,
  `city_avg` double DEFAULT NULL,
  `ratio` double DEFAULT NULL,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户总量结果表 -- 20210723备份';
insert into temp_db.bak_20210723_dws_customer_sum select * from dws_db.dws_customer_sum;
delete from  dws_db.dws_customer_sum where period = '2021Q2';




show create table dws_db.dws_customer_month;
CREATE TABLE temp_db.bak_20210723_dws_customer_month (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `month` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留月度结果表 -- 20210723备份';
insert into temp_db.bak_20210723_dws_customer_month select * from dws_db.dws_customer_month;
delete from  dws_db.dws_customer_month where period = '2021Q2';




show create table dws_db.dws_customer_cre;
CREATE table temp_db.bak_20210723_dws_customer_cre(
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留结果表 -- 20210723备份';
insert into temp_db.bak_20210723_dws_customer_cre select * from dws_db.dws_customer_cre;
delete from  dws_db.dws_customer_cre where period = '2021Q2';



show create table dws_db.dws_customer_week;
CREATE TABLE temp_db.bak_20210723_dws_customer_week (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `week` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留周度结果表  -- 20210723备份';
insert into temp_db.bak_20210723_dws_customer_week select * from dws_db.dws_customer_week;
delete from  dws_db.dws_customer_week where period = '2021Q2';




show create table dws_db.dws_newest_investment_pop_rownumber_quarter;
CREATE TABLE temp_db.bak_20210723_dws_newest_investment_pop_rownumber_quarter (
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
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=110011 DEFAULT CHARSET=utf8mb4 COMMENT='投资型人气热度排行榜单表  -- 20210723备份';
insert into temp_db.bak_20210723_dws_newest_investment_pop_rownumber_quarter select * from dws_db.dws_newest_investment_pop_rownumber_quarter;
delete from  dws_db.dws_newest_investment_pop_rownumber_quarter where period = '2021Q2';


show create table dws_db.dws_newest_investment_pop_rownumber_quarter;
CREATE TABLE temp_db.bak_20210723_dws_newest_investment_pop_rownumber_quarter (
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
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=60420 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单表   -- 20210723备份';
insert into temp_db.bak_20210723_dws_newest_investment_pop_rownumber_quarter select * from dws_db.dws_newest_investment_pop_rownumber_quarter;
delete from  dws_db.dws_newest_investment_pop_rownumber_quarter where period = '2021Q2';





show create table dws_db.dws_newest_property_score;
CREATE TABLE temp_db.bak_20210723_dws_newest_property_score (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `property_id` varchar(255) NOT NULL COMMENT '物业id',
  `city_id` varchar(255) NOT NULL COMMENT '城市id',
  `score` varchar(255) NOT NULL COMMENT '物业得分',
  `quarter` varchar(255) NOT NULL COMMENT '分析周期',
  `mean_score` float DEFAULT NULL COMMENT '城市均值',
  `city_mean_score` float DEFAULT NULL COMMENT '高于城市均值',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=48950 DEFAULT CHARSET=utf8mb4 COMMENT='物业评分表   -- 20210723备份';
insert into temp_db.bak_20210723_dws_newest_property_score select * from dws_db.dws_newest_property_score;
delete from  dws_db.dws_newest_property_score where quarter = '2021Q2';




--------------------------------------------- =========================== ---------------------------------------------------






select city_name,period,sum(value) from dws_db.bak_20210722_dws_newest_supply where period = '2021Q1' group by city_name,period ;

select * from 
 (select city_name,period,sum(value) from dws_db.bak_20210722_dws_newest_supply where city_county_index = 2 and period_index = 2 group by city_name,period) t1
left join 
 (select city_name,period,value,local_issue_value ,local_room_sum_value ,cric_value from dws_db.bak_20210722_dws_newest_supply where city_county_index = 1 and period_index = 2) t2
on t1.city_name = t2.city_name and t1.period = t2.period;

select city_name ,issue_code, room_sum,count(1) from temp_db.tmp_city_newest_deal where city_name = '九江' and issue_date_clean between '2021-04-01' and '2021-06-30' group by city_name ,issue_code ,room_sum ;


select city_name from temp_db.tmp_city_newest_deal tcnd group by city_name ;

--------------------------------------------- =========================== ---------------------------------------------------





show create table dws_db.dws_newest_supply ;
drop table dws_db.dws_newest_supply ;


CREATE TABLE dws_db.bak_20210722_dws_newest_supply (
  `city_name` varchar(64) NOT NULL COMMENT '城市名称',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `period` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数',
  `local_issue_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前预售证的供应数量的加和',
  `local_room_sum_value` varchar(20) DEFAULT NULL COMMENT '本地数据源当前房间数量',
  `cric_value` varchar(20) DEFAULT NULL COMMENT 'CRIC供应数量',
  `value_from_index` varchar(8) DEFAULT NULL COMMENT '供应套数来源标识',
  `county_name_merge` varchar(322) DEFAULT NULL COMMENT '区域名称同步',
  `city_county_index` varchar(8) DEFAULT NULL COMMENT '城市区域维度标识',
  `period_index` varchar(8) DEFAULT NULL COMMENT '周期维度标识',
  KEY `idx_supply_city_id` (`city_id`) USING BTREE,
  KEY `idx_supply_city_name` (`city_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市/区域供应套数表';
insert into dws_db.bak_20210722_dws_newest_supply select * from dws_db.dws_newest_supply;


drop table 


insert into dws_db.dws_newest_supply 
(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index) 
values ('city_name','county_name','city_id','period','value','local_issue_value','local_room_sum_value','cric_value','1','1','1','1');


--------------------------------------------- =========================== ---------------------------------------------------






select
	distinct city_id ,
	county_id ,
	period,
	for_sale ,
	on_sale ,
	sell_out ,
	total_count ,
	follow ,
	intention ,
	urgent ,
	increase ,
	retained ,
	unit_price
from
	dws_db.dws_newest_city_qua dncq;
select
	city_id ,
	county_id ,
	period,
	for_sale ,
	on_sale ,
	sell_out ,
	total_count ,
	follow ,
	intention ,
	urgent ,
	increase ,
	retained ,
	unit_price
from
	dws_db.dws_newest_city_qua dncq
group by 
city_id ,
	county_id ,
	period,
	for_sale ,
	on_sale ,
	sell_out ,
	total_count ,
	follow ,
	intention ,
	urgent ,
	increase ,
	retained ,
	unit_price;



show create table dws_db.dws_newest_city_qua;

CREATE TABLE dws_db.bak_20210721_dws_newest_city_qua (
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区域',
  `period` varchar(255) NOT NULL COMMENT '周期',
  `for_sale` int(6) NOT NULL DEFAULT '0' COMMENT '待售数量',
  `on_sale` int(6) NOT NULL DEFAULT '0' COMMENT '在售数量',
  `sell_out` int(6) NOT NULL DEFAULT '0' COMMENT '售罄数量',
  `total_count` int(6) DEFAULT '0' COMMENT '项目总量',
  `follow` int(6) NOT NULL DEFAULT '0' COMMENT '关注楼盘数量',
  `intention` int(6) NOT NULL DEFAULT '0' COMMENT '意向选房数量',
  `urgent` int(6) NOT NULL DEFAULT '0' COMMENT '迫切买房数量',
  `increase` int(6) NOT NULL DEFAULT '0' COMMENT '当季新增',
  `retained` int(6) NOT NULL DEFAULT '0' COMMENT '当季留存',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市项目总量与意向客户统计 -- > 20210721备份';
insert into dws_db.bak_20210721_dws_newest_city_qua select * from dws_db.dws_newest_city_qua;

truncate table dws_db.dws_newest_city_qua;

insert into dws_db.dws_newest_city_qua select
	distinct city_id ,
	county_id ,
	period,
	for_sale ,
	on_sale ,
	sell_out ,
	total_count ,
	follow ,
	intention ,
	urgent ,
	increase ,
	retained ,
	unit_price
from
	dws_db.bak_20210721_dws_newest_city_qua;
 

--------------------------------------------- =========================== ---------------------------------------------------









select city_name from temp_db.tmp_city_newest_deal tcnd where issue_date_clean != '' group by city_name ;




SELECT
TABLE_NAME,
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'dws_db'
AND TABLE_NAME LIKE  '%dws_newest_supply%';

show create table dws_db.dws_newest_info ;


CREATE TABLE dws_db.bak_20210721_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 -- 20210721';

insert into dws_db.bak_20210721_dws_newest_info select * from dws_db.dws_newest_info ;



--------------------------------------------- =========================== ---------------------------------------------------






show create table dws_db.dws_newest_provide_sche ;

CREATE TABLE dws_db.dws_newest_provide_sche_20210719 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `newest_id` varchar(200) DEFAULT NULL COMMENT '楼盘id',
  `date` date DEFAULT NULL COMMENT '日期',
  `period` varchar(255) DEFAULT NULL COMMENT '时间周期',	
  `provide_title` varchar(500) DEFAULT NULL COMMENT '动态标题',
  `provide_sche` text COMMENT '动态正文',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194047 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态表';
insert into dws_db.dws_newest_provide_sche_20210719 select * from temp_db.bak_20210618_dws_newest_provide_sche ;


--------------------------------------------- =========================== ---------------------------------------------------




show create table dws_db.dws_newest_info ;
CREATE TABLE temp_db.bak_20210719_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表';
insert into temp_db.bak_20210719_dws_newest_info select * from dws_db.dws_newest_info ;

delete from temp_db.bak_20210719_dws_newest_info;

update temp_db.bak_20210719_dws_newest_info set household_num = '-1.000' where household_num = 0;

--------------------------------------------- =========================== ---------------------------------------------------







select park_rate from temp_db.bak_20210709_dws_newest_info bdni where park_rate = '-' or park_rate is null;
select park_num from temp_db.bak_20210709_dws_newest_info bdni where park_num = '' or park_num is null or park_num = 1;
select volume_rate from temp_db.bak_20210709_dws_newest_info bdni where volume_rate < 1 or volume_rate > 5;
select household_num from temp_db.bak_20210709_dws_newest_info bdni where household_num = 0;

update temp_db.bak_20210709_dws_newest_info set park_rate = '-' where park_rate = '' or park_rate is null;

update temp_db.bak_20210709_dws_newest_info set park_num = '-' where park_num = '' or park_num is null or park_num = 1;

update temp_db.bak_20210709_dws_newest_info set volume_rate = '-1.000' where volume_rate < 1 or volume_rate > 5;

update temp_db.bak_20210709_dws_newest_info set household_num = '-1.000' where  household_num = 0;




--------------------------------------------- =========================== ---------------------------------------------------









show create table dws_db.dws_newest_supply ;

CREATE TABLE bak_20210715_dws_newest_supply (
  `city_name` text COMMENT '城市名称',
  `county_name` text COMMENT '区域名称',
  `city_id` varchar(8) DEFAULT NULL COMMENT '城市区划代码',
  `date` varchar(40) DEFAULT NULL COMMENT '对应日期',
  `value` varchar(20) DEFAULT NULL COMMENT '供应套数'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='供应套数表';
insert into dws_db.bak_20210715_dws_newest_supply select * from dws_db.dws_newest_supply;


select city_name from dws_db.bak_20210715_dws_newest_supply bdns group by city_name ;


--------------------------------------------- =========================== ---------------------------------------------------









select newest_name ,city_id ,county_id from dws_db.dws_newest_info group by newest_name ,city_id ,county_id;

select clean_floor_name from odsdb.city_newest_deal cnd where city_name = '宁波' group by clean_floor_name ;



select gd_city , sum(room_sum),sum(c) from 
(
select
	floor_name,
	issue_code,
	gd_city,
	room_sum,
	count(1) c
from
	odsdb.city_newest_deal cnd
where
	issue_date_clean >= '2021-01-01'
	and issue_date_clean < '2021-04-01' 
group by floor_name, issue_code, gd_city, room_sum) t group by gd_city ;



select * from 
(SELECT newest_id,volume_rate FROM dwb_db.dwb_newest_info) 
left join 
(SELECT uuid,volume_rate FROM odsdb.ori_newest_info_base) t2
on t1.newest_id=t2.uuid
where t1.volume_rate != t2.volume_rate;



select * from 
(SELECT newest_id,household_num FROM dwb_db.dwb_newest_info) t1 
left join 
(SELECT uuid,household_num FROM odsdb.ori_newest_info_base) t2
on t1.newest_id=t2.uuid
where t1.household_num != t2.household_num;


select * from 
(SELECT newest_id,land_area FROM dwb_db.dwb_newest_info) t1 
left join 
(SELECT uuid,land_area FROM odsdb.ori_newest_info_base) t2
on t1.newest_id=t2.uuid
where t1.land_area != t2.land_area;

SELECT newest_id,land_area,building_area,volume_rate,household_num,park_rate,park_num FROM dwb_db.dwb_newest_info where building_area/land_area = volume_rate;


select * from 
(SELECT newest_id,land_area,building_area,volume_rate,household_num,park_rate,park_num FROM dwb_db.dwb_newest_info) t1 
left join 
(SELECT uuid,land_area,building_area,volume_rate,household_num,park_rate,park_num FROM odsdb.ori_newest_info_base) t2
on t1.newest_id=t2.uuid
where t1.land_area != t2.land_area;

insert into dwb_db.bak_20210713_dwb_newest_info select * from  dwb_db.dwb_newest_info;
show create table dwb_db.dwb_newest_info;
CREATE TABLE dwb_db.bak_20210713_dwb_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) NOT NULL DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) NOT NULL DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘 ---- >  2021年07月13日备份';


insert into dws_db.bak_20210713_dws_newest_info select * from  dws_db.dws_newest_info;
show create table dws_db.dws_newest_info;
CREATE TABLE dws_db.bak_20210713_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 ---- >  2021年07月13日备份';

--------------------------------------------- =========================== ---------------------------------------------------









show create table temp_db.tmp_city_newest_deal ;

CREATE TABLE odsdb.city_newest_deal (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) DEFAULT NULL COMMENT '网址',
  `city_name` varchar(32) NOT NULL COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) DEFAULT NULL COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格  (元/㎡)',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `insert_time` varchar(200) DEFAULT NULL COMMENT '插入时间',
  PRIMARY KEY (`id`,`city_name`),
  KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
  KEY `idx_newest_city_name` (`city_name`) USING BTREE,
  KEY `idx_newest_url` (`url`) USING BTREE,
  KEY `idx_newest_floor_name` (`floor_name`) USING BTREE,
  KEY `idx_newest_insert_time` (`insert_time`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=25124930 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表'
PARTITION BY LINEAR KEY (city_name) PARTITIONS 128;


insert into odsdb.city_newest_deal select * from temp_db.tmp_city_newest_deal;










--------------------------------------------- =========================== ---------------------------------------------------







CREATE TABLE if not exists dws_db.dws_newest_investment_pop_rownumber_quarter (
  `id` int(11) NOT NULL auto_increment COMMENT '自增id',
  `city_id` int(11) NOT null COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` decimal(10,2) DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period`  varchar(10) COMMENT '时间周期',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=41339 DEFAULT CHARSET=utf8mb4 COMMENT='投资型人气热度排行榜单表';


show create table dws_db.bak_20210622_dws_newest_investment_pop_top30_quarter;
CREATE TABLE dws_db.bak_20210713_dws_newest_investment_pop_top30_quarter (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` text COMMENT '城市/区域代码',
  `newest_id` text COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘排名',
  `period` text COMMENT '时间周期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=34691 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度top30---->2021年7月13日备份'

insert into dws_db.bak_20210713_dws_newest_investment_pop_top30_quarter (city_id,newest_id,imei_newest,imei_city,rate,sort_id,period) select * from dws_db.dws_newest_investment_pop_top30_quarter ;


select index_rate from dws_db.dws_newest_popularity_rownumber_quarter dnprq ;
truncate table  dws_db.dws_newest_popularity_rownumber_quarter;

select * from 
(select city_id ,newest_id ,sort_id ,index_rate from dws_db.dws_newest_popularity_rownumber_quarter) t1 
left join 
(select city_id ,newest_id ,sort_id ,index_rate  from dws_db.dws_newest_popularity_rownumber_quarter) t2
on t1.city_id = t2.city_id and t1.sort_id < t2.sort_id and t1.index_rate < t2.index_rate 
where t2.city_id != null;

drop table dws_db.dws_newest_popularity_rownumber_quarter;
CREATE TABLE if not exists dws_db.dws_newest_popularity_rownumber_quarter (
  `id` int(11) NOT NULL auto_increment COMMENT '自增id',
  `city_id` int(11) NOT null COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` double DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period`  varchar(10) COMMENT '时间周期',
  `imei_c_avg`  double COMMENT '楼盘热度指数占比平均值',
  `index_rate_change`  double COMMENT '楼盘热度指数占比与本市均值对比情况',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=41339 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单表';



show create table temp_db.bak_20210622_dws_newest_popularity_top30_quarter ;
CREATE TABLE `bak_20210622_dws_newest_popularity_top30_quarter` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` text COMMENT '城市/区域代码',
  `newest_id` text COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘排名',
  `period` text COMMENT '时间周期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=41339 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度top30---->2021年6月22日备份'

show create table dws_db.dws_newest_popularity_top30_quarter ;
CREATE TABLE `dws_newest_popularity_top30_quarter` (
  `city_id` text COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘排名',
  `period` text COMMENT '时间周期',
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4

CREATE TABLE dws_db.bak_20210712_dws_newest_popularity_top30_quarter (
  `id` int(11) NOT NULL auto_increment COMMENT '自增id',
  `city_id` int(11) NOT null COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘排名',
  `period`  text COMMENT '时间周期',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=41339 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单';

insert into bak_20210712_dws_newest_popularity_top30_quarter (city_id,newest_id,imei_newest,imei_city,rate,sort_id,period) select * from dws_newest_popularity_top30_quarter;

















--------------------------------------------- =========================== ---------------------------------------------------









select id, gd_city, CONCAT(',',alias,',') alias, newest_name from odsdb.ori_newest_info_main  where remark is null order by FIELD(platform , '贝壳', '房天下', '吉屋', '居里', '搜狐'), id;

select id, CONCAT(',',floor_name_clean,',') floor_name_clean, clean_floor_name from temp_db.tmp_city_newest_deal where floor_name_clean = clean_floor_name and gd_city = '长春市';

select distinct newest_name  from odsdb.ori_newest_info_202104_clean where remark is null and newest_name != '' and newest_name regexp '(·|•|\\.|丨|期)';

select city_name from temp_db.tmp_city_newest_deal where insert_time != '' group by city_name;

update temp_db.tmp_city_newest_deal set insert_time = '20210620' where city_name in ('徐州','深圳');

select city_name,clean_floor_name,floor_name_clean from temp_db.tmp_city_newest_deal group by city_name,clean_floor_name,floor_name_clean;

select city_name,count(1)>20 from temp_db.tmp_city_newest_deal tcnd where clean_floor_name is null group by city_name having count(1)>20;

select city_name from temp_db.tmp_city_newest_deal tcnd where gd_city is null group by city_name ;

update temp_db.tmp_city_newest_deal set gd_city = concat(city_name,'市') where gd_city is null;

select distinct city_id,newest_name from dwb_db.dwb_newest_info;

select city_id,newest_name from dwb_db.dwb_newest_info;





--------------------------------------------- =========================== ---------------------------------------------------







show create table dws_newest_info;
CREATE TABLE dws_newest_info_bak_20210709 (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT '0' COMMENT '占地面积',
  `building_area` double DEFAULT '0' COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT '-1' COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT '-1.000' COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  PRIMARY KEY (`newest_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表';
insert into dws_newest_info_bak_20210709 select * from dws_newest_info;

show create table dws_newest_planinfo;
CREATE TABLE dws_newest_planinfo_bak_20210709 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(255) NOT NULL COMMENT '所在城市',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(255) NOT NULL COMMENT '楼盘名称',
  `period` varchar(255) NOT NULL COMMENT '分析周期',
  `land_area` varchar(255) DEFAULT NULL COMMENT '占地面积',
  `building_area` varchar(255) DEFAULT NULL COMMENT '建筑面积',
  `volume_rate` varchar(255) DEFAULT NULL COMMENT '容积率',
  `household_num` varchar(255) DEFAULT NULL COMMENT '户数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `right_term` varchar(255) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(255) DEFAULT NULL COMMENT '绿化率',
  `decoration` varchar(255) DEFAULT NULL COMMENT '装修情况',
  `building_type` varchar(255) DEFAULT NULL COMMENT '建筑类型',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=48833 DEFAULT CHARSET=utf8mb4 COMMENT='项目详情-规划信息表';
insert into dws_newest_planinfo_bak_20210709 select * from dws_newest_planinfo;


select building_type from  dws_newest_info where building_type like '%^%' group by building_type;

select building_type from  dws_newest_planinfo where building_type like '%^%' group by building_type;

select building_type from  dws_newest_planinfo where building_type = '^' group by building_type;

select building_type from  dws_newest_info where building_type like '^%' group by building_type;
select building_type from  dws_newest_info where building_type like '%^' group by building_type;

select building_type from  dws_newest_planinfo where building_type like '^%' group by building_type;
select building_type from  dws_newest_planinfo where building_type like '%^' group by building_type;


select left(building_type,CHAR_LENGTH(building_type) - 1) from  dws_newest_planinfo where  building_type like '%^';
select left(building_type,CHAR_LENGTH(building_type)) from  dws_newest_planinfo where  building_type like '^%';


update dws_newest_planinfo set building_type = '-' where building_type = '^';

update dws_newest_info set building_type = left(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '%^';
update dws_newest_planinfo set building_type = left(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '%^';


update dws_newest_info set building_type = replace(building_type,'^',',') where building_type like '%^%';
update dws_newest_planinfo set building_type = replace(building_type,'^',',') where building_type like '%^%';


select building_type from  dws_newest_info where building_type like '%、%' group by building_type;
select building_type from  dws_newest_planinfo where building_type like '%、%' group by building_type;

update dws_newest_info set building_type = replace(building_type,'、',',') where building_type like '%、%';
update dws_newest_planinfo set building_type = replace(building_type,'、',',') where building_type like '%、%';


--------------------------------------------- =========================== ---------------------------------------------------


show create table dws_db.dws_newest_provide_sche ;


CREATE TABLE temp_db.dws_newest_provide_sche_1111111111111 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `newest_id` varchar(200) DEFAULT NULL COMMENT '楼盘id',
  `date` date DEFAULT NULL COMMENT '日期',
  `period` varchar(255) DEFAULT NULL COMMENT '时间周期',
  `provide_title` varchar(500) DEFAULT NULL COMMENT '动态标题',
  `provide_sche` text COMMENT '动态正文',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194047 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态表'


show create table dws_db.dws_newest_property_score ;


CREATE TABLE temp_db.dws_newest_property_score_11111111 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `property_id` varchar(255) NOT NULL COMMENT '物业id',
  `city_id` varchar(255) NOT NULL COMMENT '城市id',
  `score` varchar(255) NOT NULL COMMENT '物业得分',
  `quarter` varchar(255) NOT NULL COMMENT '分析周期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=12905 DEFAULT CHARSET=utf8mb4 COMMENT='物业评分表'


--------------------------------------------- ============惠州=============== ---------------------------------------------------
 

select city_id,count(1) from dws_db.dws_newest_issue_code dnic where issue_code is not null group by city_id ;




select
	gd_city,
	floor_name,
	max(business),
	issue_code,
	max(issue_date_clean) issue_date,
	open_date,
	issue_area,
	sale_state,
	building_code,
	room_sum,
	area,
	simulation_price,
	sale_telephone,
	sale_address,
	'tmp_city_newest_deal' from_code
from
	temp_db.tmp_city_newest_deal tcnd 
group by 	
    gd_city,
	floor_name,
	issue_code,
	open_date,
	issue_area,
	sale_state,
	building_code,
	room_sum,
	area,
	simulation_price,
	sale_telephone,
	sale_address;
select gd_city, floor_name, max(business), issue_code, max(issue_date_clean) issue_date, open_date, issue_area, sale_state, building_code, room_sum, area, simulation_price, sale_telephone, sale_address, 'tmp_city_newest_deal' from_code from temp_db.tmp_city_newest_deal where issue_date_clean != '' group by gd_city, floor_name, issue_code, open_date, issue_area, sale_state, building_code, room_sum, area, simulation_price, sale_telephone, sale_address;

select floor_name,gd_city,newest_name,address from temp_db.tmp_city_newest_deal_ls3 tcndl ;




select a0.newest_id,a0.newest_name,a0.city_id,a0.county_id,a0.address,a1.city_name from dws_db.dws_newest_info a0 left join dws_db.dim_geography a1 on a1.city_id=a0.city_id and a1.grade= 3;


select * from dws_db.dws_newest_period_admit dnpa ;


CREATE TABLE dwb_db.dwb_admit_newest_city_deal (
    `id` int(11) NOT NULL AUTO_INCREMENT,
	`city_id` varchar(40) DEFAULT NULL COMMENT '城市id',
	`period` varchar(40) DEFAULT NULL COMMENT '周期',
	`newest_id` varchar(80) DEFAULT NULL COMMENT '准入楼盘id',
	`dr` varchar(2) DEFAULT NULL COMMENT '标识(0,有效，1，作废）',
	`county_id` varchar(40) DEFAULT NULL COMMENT '区域id',
	`address` varchar(255) DEFAULT NULL COMMENT '楼盘地址',
	`city_name` varchar(32) DEFAULT NULL COMMENT '城市名称',
	`floor_name` varchar(255) DEFAULT NULL COMMENT '项目原始名',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194047 DEFAULT CHARSET=utf8mb4 COMMENT='新房交易和基准楼盘表';




CREATE TABLE dws_db.dws_newest_issue_code (
    `id` int(11) NOT NULL AUTO_INCREMENT,
	`city_id` varchar(40) DEFAULT NULL COMMENT '城市id',
	`period` varchar(40) DEFAULT NULL COMMENT '周期',
	`newest_id` varchar(80) DEFAULT NULL COMMENT '准入楼盘id',
	`dr` varchar(2) DEFAULT NULL COMMENT '标识(0,有效，1，作废）',
	`gd_city` varchar(32) DEFAULT NULL COMMENT '城市名称',
	`floor_name` varchar(255) DEFAULT NULL COMMENT '项目原始名',
	`newest_name` varchar(255) DEFAULT NULL COMMENT '楼盘名称',
	`address` varchar(200) DEFAULT NULL COMMENT '楼盘地址',
	`business` varchar(60) DEFAULT NULL COMMENT '开发商',
	`issue_code` varchar(60) DEFAULT NULL COMMENT '预售证号',
	`issue_date` varchar(30) DEFAULT NULL COMMENT '预售证发证时间',
	`open_date` varchar(30) DEFAULT NULL COMMENT '开盘时间',
	`issue_area` varchar(20) DEFAULT NULL COMMENT '预售许可面积',
	`sale_state` varchar(10) DEFAULT NULL COMMENT '销售状态',
	`building_code` varchar(100) DEFAULT NULL COMMENT '建筑编号',
	`room_sum` varchar(5) DEFAULT NULL COMMENT '房间数量',
	`area` varchar(20) DEFAULT NULL COMMENT '建筑面积',
	`simulation_price` varchar(30) DEFAULT NULL COMMENT '拟售价格',
	`sale_telephone` varchar(40) DEFAULT NULL COMMENT '销售电话',
	`sale_address` varchar(200) DEFAULT NULL COMMENT '销售地址',
	`update_time` datetime DEFAULT NULL COMMENT '更新时间',
	`from_code` varchar(20) DEFAULT NULL COMMENT '预售证来源',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194047 DEFAULT CHARSET=utf8mb4 COMMENT='预征信息表'






--------------------------------------------- ============惠州=============== ---------------------------------------------------
 


select * from qyf_tmp.tmptable1 t;

delete from temp_db.city_newest_deal_data_check  where city_name = '扬州';

select city_name from temp_db.city_newest_deal_data_check cnddc group by city_name ;
select city_name from temp_db.tmp_city_newest_deal cnddc group by city_name ;

select insert_time from temp_db.tmp_city_newest_deal cnddc group by insert_time ;

select * from temp_db.tmp_city_newest_deal_ls3;

select floor_name from temp_db.tmp_city_newest_deal tcnd group by floor_name ; -- 54160

select floor_name from temp_db.tmp_city_newest_deal tcnd where city_name = '佛山' and issue_date_clean >= '2020-11-01' and issue_date_clean <= '2020-11-30' group by floor_name ;

select * from dws_db.dws_newest_alias where city_name = '佛山市' 
  and (alias_name in (select floor_name from temp_db.tmp_city_newest_deal tcnd 
                       where city_name = '佛山' and issue_date_clean >= '2020-11-01' and issue_date_clean <= '2020-11-30' group by floor_name) 
  or newest_name in (select floor_name from temp_db.tmp_city_newest_deal tcnd 
                       where city_name = '佛山' and issue_date_clean >= '2020-11-01' and issue_date_clean <= '2020-11-30' group by floor_name)); -- 105 

                       
select count(1) , gd_city from (
select gd_city,floor_name,count(1) c from  temp_db.tmp_city_newest_deal_ls3 group by gd_city,floor_name having count(1) = 2)t group by gd_city ;

select gd_city,floor_name,clean_floor_name,newest_name ,count(1) c from  temp_db.tmp_city_newest_deal_ls3 group by gd_city,floor_name,clean_floor_name,newest_name  having count(1) = 2


select * from temp_db.tmp_city_newest_deal tcnd where city_name in ('九江');
 -- 13,738


select tt.*,dni.* from 
(select b.* from (select gd_city,floor_name from temp_db.tmp_city_newest_deal tcnd where city_name in ('九江') ) a inner join 
    (select gd_city,floor_name,clean_floor_name,newest_name from  temp_db.tmp_city_newest_deal_ls3 where gd_city in ('九江市') 
     group by gd_city,floor_name,clean_floor_name,newest_name ) b on a.floor_name = b.floor_name and a.gd_city=b.gd_city
) tt 
left join dws_db.dws_newest_info dni 
on tt.newest_name = dni.newest_name;


select tt.*,dni.newest_id from 
(select b.* from (select gd_city,floor_name from temp_db.tmp_city_newest_deal tcnd where city_name in ('九江') ) a inner join 
    (select gd_city,floor_name,clean_floor_name,newest_name from  temp_db.tmp_city_newest_deal_ls3 where gd_city in ('九江市') 
     group by gd_city,floor_name,clean_floor_name,newest_name ) b on a.floor_name = b.floor_name and a.gd_city=b.gd_city
) tt 
left join dws_db.dws_newest_info dni 
on tt.newest_name = dni.newest_name where dni.newest_id != '';


select count(1),tttt.gd_city from (
select tt.gd_city,dni.newest_id from 
(select b.* from (select gd_city,floor_name from temp_db.tmp_city_newest_deal tcnd ) a inner join 
    (select gd_city,floor_name,clean_floor_name,newest_name from  temp_db.tmp_city_newest_deal_ls3 
     group by gd_city,floor_name,clean_floor_name,newest_name ) b on a.floor_name = b.floor_name and a.gd_city=b.gd_city
) tt 
left join dws_db.dws_newest_info dni 
on tt.newest_name = dni.newest_name
where dni.newest_id != '' group by tt.gd_city,dni.newest_id) tttt group by tttt.gd_city;


select count(1) ,city_id from (
select city_id,newest_id from dws_db.dws_newest_info dni group by city_id,newest_id) t group by city_id ;

 -- 13,738

select * from dws_db.dim_geography dg where city_name in (温州市 无锡市 长沙市 中山市 淄博市)


select a.*,b.* from
(select distinct floor_name,clean_floor_name,gd_city from temp_db.tmp_city_newest_deal) a
left join
(select distinct a0.newest_name,a1.city_name  from dwb_db.dwb_newest_info a0 left join dws_db.dim_geography a1 on a1.city_id=a0.city_id and a1.grade=3) b
on a.clean_floor_name=b.newest_name and a.gd_city=b.city_name 
having newest_name is null ;  -- 50,329


select a.*,b.* from
(select distinct floor_name,clean_floor_name,gd_city from temp_db.tmp_city_newest_deal) a , 
(select alias_name,city_name,newest_id,newest_name from dws_db.dws_newest_alias where dr = '0') b
where a.gd_city=b.city_name and newest_name is null 
  and (a.clean_floor_name=b.newest_name or a.clean_floor_name=b.alias_name or a.floor_name=b.newest_name or a.floor_name=b.alias_name);







--------------------------------------------- ============惠州=============== ---------------------------------------------------
-- 宁波  
-- 沈阳	
-- 保定 
-- 石家庄

select clean_floor_name,city_name from temp_db.tmp_city_newest_deal group by clean_floor_name,city_name having clean_floor_name is null;
-- 三亚
-- 东莞
-- 南京
-- 哈尔滨
-- 宁波
-- 广州
-- 徐州
-- 惠州
-- 扬州
-- 沈阳
-- 深圳
-- 珠海
-- 长春

ALTER TABLE odsdb.city_newest_deal MODIFY COLUMN issue_code varchar(320) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NULL COMMENT '许可证';
CREATE INDEX idx_issue_code USING BTREE ON temp_db.tmp_city_newest_deal (issue_code);
CREATE INDEX idx_issue_code USING BTREE ON odsdb.city_newest_deal (issue_code);

select max(length(issue_code)) from odsdb.city_newest_deal cnd ;

select city_name from temp_db.city_newest_deal_data_check cnddc group by city_name ; -- 3
select city_name from temp_db.tmp_city_newest_deal cnddc group by city_name ; -- 39
select city_name from temp_db.tmp_city_newest_deal cnddc where insert_time !=''  group by city_name;

select issue_date_clean from temp_db.tmp_city_newest_deal where city_name = '中山';

update odsdb.city_newest_deal set issue_date = '2018-01-09' where issue_date_clean = '0108-01-09';
update odsdb.city_newest_deal set issue_date_clean = '2018-01-09' where issue_date_clean = '0108-01-09';
update odsdb.city_newest_deal set address = business where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set business = issue_code where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set issue_code = issue_date where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set issue_date = '2006-03-06' where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set room_sum = area where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set area = null where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set room_code = room_sale_area where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set room_sale_area = null where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set room_sale_state = '备案' where issue_date_clean in ('2006-0687','2006-0688');
update odsdb.city_newest_deal set issue_date_clean = issue_date where issue_date_clean in ('2006-0687','2006-0688');



update temp_db.tmp_city_newest_deal set issue_date = '2018-01-09' where issue_date_clean = '0108-01-09';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2018-01-09' where issue_date_clean = '0108-01-09';
update temp_db.tmp_city_newest_deal set address = business where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set business = issue_code where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set issue_code = issue_date where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set issue_date = '2006-03-06' where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set room_sum = area where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set area = null where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set room_code = room_sale_area where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set room_sale_area = null where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set room_sale_state = '备案' where issue_date_clean in ('2006-0687','2006-0688');
update temp_db.tmp_city_newest_deal set issue_date_clean = issue_date where issue_date_clean in ('2006-0687','2006-0688');


select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '长沙';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d')  where city_name = '长沙' and issue_date != 'None';

select str_to_date(substr(issue_date,1,10),'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '西安';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(substr(issue_date,1,10),'%Y-%m-%d')  where city_name = '西安' and issue_date != '';

select str_to_date(issue_date_clean ,'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '深圳';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2021-04-01'  where city_name = '深圳' and issue_date_clean = '2021-04-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2021-01-01'  where city_name = '深圳' and issue_date_clean = '2021-01-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2021-12-01'  where city_name = '深圳' and issue_date_clean = '2021-12-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-12-01'  where city_name = '深圳' and issue_date_clean = '2020-12-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-11-01'  where city_name = '深圳' and issue_date_clean = '2020-11-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-10-01'  where city_name = '深圳' and issue_date_clean = '2020-10-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-09-01'  where city_name = '深圳' and issue_date_clean = '2020-09-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-08-01'  where city_name = '深圳' and issue_date_clean = '2020-08-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = '2020-03-01'  where city_name = '深圳' and issue_date_clean = '2020-03-00';
update temp_db.tmp_city_newest_deal set issue_date_clean = ''  where city_name = '深圳' and issue_date_clean  = '-00--00';

select count(1) from temp_db.tmp_city_newest_deal where isnull(issue_code) group by url ;


update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date_clean ,'%Y-%m-%d')  where city_name = '深圳' and issue_date != '' and issue_date_clean > '2020-01-01';


select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '福州';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d')  where city_name = '福州' and issue_date != '';

select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '温州';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d')  where city_name = '温州' and issue_date != '';

select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '淄博';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d')  where city_name = '淄博' and issue_date != '';

select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '海口';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d')  where city_name = '海口' and issue_date != '';

select str_to_date(replace(issue_date,'/','-') ,'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '杭州';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(replace(issue_date,'/','-') ,'%Y-%m-%d')  where city_name = '杭州';

select str_to_date(replace(replace(replace(replace(substring_index(issue_date , ',',1) ,'年','-'),'月','-'),'日','') ,'\.','-'),'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '无锡';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(replace(replace(replace(replace(substring_index(issue_date , ',',1) ,'年','-'),'月','-'),'日','') ,'\.','-'),'%Y-%m-%d') where city_name = '无锡' and issue_date != 'None';

update temp_db.tmp_city_newest_deal set gd_city =concat(city_name,'市')  where city_name = '扬州';

select str_to_date(replace(open_date,' 00:00:00','') ,'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '成都';
update temp_db.tmp_city_newest_deal set issue_date = null where city_name = '成都' ;
update temp_db.tmp_city_newest_deal set issue_date_clean = null where city_name = '成都' ;
update temp_db.tmp_city_newest_deal set open_date = null where city_name = '成都' and open_date = 'None';
update temp_db.tmp_city_newest_deal set issue_date = str_to_date(replace(open_date,' 00:00:00','') ,'%Y-%m-%d') where city_name = '成都' and open_date != '现房';
update temp_db.tmp_city_newest_deal set issue_date_clean = issue_date where city_name = '成都';

select str_to_date(substr(issue_date,1,10),'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '宝鸡';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(substr(issue_date,1,10),'%Y-%m-%d') where city_name = '宝鸡' and issue_date != '';
update temp_db.tmp_city_newest_deal set issue_area = null where city_name = '宝鸡';

select str_to_date(replace(replace(issue_date,' 0:00:00',''),' 0:00',''),'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '唐山';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(replace(replace(issue_date,' 0:00:00',''),' 0:00',''),'%Y/%m/%d') where city_name = '唐山';

select STR_TO_DATE(substring_index(issue_date , 'T',1),'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '厦门';
update temp_db.tmp_city_newest_deal set issue_date_clean = STR_TO_DATE(substring_index(issue_date , 'T',1),'%Y-%m-%d') where city_name = '厦门' and issue_date != 'None';

select str_to_date(substr(issue_date,1,10),'%Y-%m-%d') from temp_db.tmp_city_newest_deal where city_name = '丽水';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(substr(issue_date,1,10),'%Y-%m-%d') where city_name = '丽水' and issue_date != 'None';

select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '佛山';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d') where city_name = '佛山' and issue_date != '';


select str_to_date(replace(issue_date,'0:00:00',''),'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '北京';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(replace(issue_date,'0:00:00',''),'%Y/%m/%d') where city_name = '北京' and issue_date != '';


select str_to_date(issue_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal where city_name = '南宁';
update temp_db.tmp_city_newest_deal set issue_date_clean = str_to_date(issue_date,'%Y/%m/%d') where city_name = '南宁' and issue_date != '';


delete from temp_db.city_newest_deal_data_check where city_name in ('南京','广州') ; 

--------------------------------------------- ============保定=============== ---------------------------------------------------

delete from temp_db.city_newest_deal_data_check where city_name = '沈阳';
delete from temp_db.city_newest_deal_data_check where city_name = '保定';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '保定';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '保定' and issue_code = '';


--------------------------------------------- ============沈阳=============== ---------------------------------------------------
-- 宁波  
-- 沈阳	
-- 保定 
-- 石家庄

delete from temp_db.city_newest_deal_data_check where city_name = '宁波';
delete from temp_db.tmp_city_newest_deal where city_name = '沈阳';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '沈阳';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '沈阳';
update temp_db.city_newest_deal_data_check set gd_city = null  where city_name = '沈阳' and issue_code != '';

select * from temp_db.city_newest_deal_data_check where city_name = '沈阳' and isnull(gd_city) ;

update temp_db.city_newest_deal_data_check set issue_date=STR_TO_DATE(replace(replace(open_date, '\t',''),'/','-'),'%Y-%m-%d') where city_name = '沈阳' and open_date != '';
update temp_db.city_newest_deal_data_check set issue_date_clean=issue_date where city_name = '沈阳' and open_date != '';


update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '沈阳';
select * from temp_db.tmp_city_newest_deal where city_name = '沈阳';
insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '沈阳';
--------------------------------------------- ============宁波=============== ---------------------------------------------------
 
-- 宁波  
-- 沈阳	
-- 保定 

delete from temp_db.city_newest_deal_data_check where city_name = '三亚';
delete from temp_db.tmp_city_newest_deal where city_name = '宁波';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '宁波';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '宁波';

update temp_db.city_newest_deal_data_check set issue_date_clean=STR_TO_DATE(replace(issue_date, '备案日期：',''),'%Y-%m-%d') where city_name = '宁波' and issue_date != '';

update temp_db.city_newest_deal_data_check set simulation_price=replace(simulation_price, '//元/㎡','') where city_name = '宁波';
update temp_db.city_newest_deal_data_check set simulation_price=replace(simulation_price, '元/㎡','') where city_name = '宁波';
update temp_db.city_newest_deal_data_check set simulation_price=replace(simulation_price, '//','') where city_name = '宁波';


update temp_db.city_newest_deal_data_check set open_date =replace(sale_telephone, '售楼地址：','') where city_name = '宁波';
update temp_db.city_newest_deal_data_check set sale_telephone =replace(sale_address, '售楼电话：','') where city_name = '宁波';
update temp_db.city_newest_deal_data_check set sale_address =open_date where city_name = '宁波';
update temp_db.city_newest_deal_data_check set open_date =null where city_name = '宁波';

update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '宁波';
select * from temp_db.tmp_city_newest_deal where city_name = '宁波';
insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '宁波';


--------------------------------------------- ============三亚=============== ---------------------------------------------------

-- 三亚


delete from temp_db.city_newest_deal_data_check where city_name = '东莞';
delete from temp_db.tmp_city_newest_deal where city_name = '三亚';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '三亚';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '三亚';

update temp_db.city_newest_deal_data_check set issue_date=STR_TO_DATE(substring_index(open_date, 'T',1),'%Y-%m-%d') where city_name = '三亚' and open_date != '';
update temp_db.city_newest_deal_data_check set issue_date_clean=issue_date where city_name = '三亚';

update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '三亚';

select * from temp_db.tmp_city_newest_deal where city_name = '三亚';

insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '三亚';





--------------------------------------------- ============东莞=============== ---------------------------------------------------

delete from temp_db.city_newest_deal_data_check where city_name = '南京';

-- 扬州 
-- 惠州 
-- 东莞

-- gd_city
delete from temp_db.city_newest_deal_data_check where city_name = '惠州';
delete from temp_db.tmp_city_newest_deal where city_name = '东莞';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '东莞';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '东莞';

select floor_name       from temp_db.city_newest_deal_data_check cnddc where city_name = '东莞' group by floor_name; 
select issue_date       from temp_db.city_newest_deal_data_check cnddc where city_name = '东莞' group by issue_date; 

update temp_db.city_newest_deal_data_check set issue_date=STR_TO_DATE(open_date,'%Y/%m/%d') where city_name = '东莞';
update temp_db.city_newest_deal_data_check set issue_date_clean=issue_date where city_name = '东莞';

update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '东莞';

select * from temp_db.tmp_city_newest_deal where city_name = '东莞';

insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '东莞';



--------------------------------------------- ============惠州=============== ---------------------------------------------------











-- gd_city
delete from temp_db.city_newest_deal_data_check where city_name = '扬州';
select * from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '惠州';

-- 肉眼筛查脏数据
select city_name        from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by city_name;    
select gd_city          from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by gd_city;        
select floor_name       from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by floor_name; 
select floor_name_new   from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by floor_name_new;     
select clean_floor_name from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by clean_floor_name;       
select floor_name_clean from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by floor_name_clean;       
select address          from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by address;   
select business         from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by business;      
select issue_code       from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by issue_code; 
select issue_date       from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by issue_date; 
select issue_date_clean from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by issue_date_clean;       
select open_date        from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州'  and open_date != '' group by open_date;
select issue_area       from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by issue_area; 
select sale_state       from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' group by sale_state; 
select building_code    from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020' group by building_code;    
select room_sum         from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by room_sum;
select area             from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by area;
select simulation_price from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by simulation_price;       
select sale_telephone   from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by sale_telephone;     
select sale_address     from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by sale_address;   
select room_code        from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by room_code;
select room_sale_area   from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by room_sale_area;     
select room_sale_state  from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by room_sale_state;      
select create_time      from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and issue_date_clean >'2020'  group by create_time;  

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '惠州' and simulation_price like '%元/㎡%';

update temp_db.city_newest_deal_data_check set issue_date_clean = STR_TO_DATE(replace(replace(replace(issue_date,'年','-'),'月','-'),'日',''),'%Y-%m-%d') 
  where city_name = '惠州' and issue_date != ''; 

update temp_db.city_newest_deal_data_check set issue_area = replace(issue_area,'，','') where city_name = '惠州' and issue_area like '%，%'; 

update temp_db.city_newest_deal_data_check set simulation_price = replace(simulation_price,'元/㎡','') where city_name = '惠州' and simulation_price = '元/㎡'; 

update temp_db.city_newest_deal_data_check set simulation_price = replace(simulation_price,'(元/㎡)','') where city_name = '惠州' and simulation_price like '%元/㎡%';

update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '惠州';

select * from temp_db.tmp_city_newest_deal where city_name = '惠州';

insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '惠州';

--------------------------------------------- ============扬州=============== ---------------------------------------------------








select * from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州';
update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市')  where city_name = '扬州';


select city_name        from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by city_name;    
select gd_city          from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by gd_city;        
select floor_name       from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by floor_name; 
select floor_name_new   from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by floor_name_new;     
select clean_floor_name from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by clean_floor_name;       
select floor_name_clean from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by floor_name_clean;       
select address          from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by address;   
select business         from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by business;      
select issue_code       from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by issue_code; 
select issue_date       from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by issue_date; 
select issue_date_clean from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by issue_date_clean;       
select open_date        from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州'  and open_date != '' group by open_date;
select issue_area       from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by issue_area; 
select sale_state       from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by sale_state; 
select building_code    from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by building_code;    
select room_sum         from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by room_sum;
select area             from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by area;
select simulation_price from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by simulation_price;       
select sale_telephone   from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by sale_telephone;     
select sale_address     from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by sale_address;   
select room_code        from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by room_code;
select room_sale_area   from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by room_sale_area;     
select room_sale_state  from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by room_sale_state;      
select create_time      from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' group by create_time;  


select * from temp_db.city_newest_deal_data_check cnddc where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
-- 
-- update temp_db.city_newest_deal_data_check set open_date = issue_area where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set issue_area = '' where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- 
-- update temp_db.city_newest_deal_data_check set sale_state = building_code where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set building_code = room_sum where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set room_sum = area where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set area = '' where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- 
-- update temp_db.city_newest_deal_data_check set simulation_price = sale_telephone where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set sale_telephone = sale_address where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set sale_address = '' where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- 
-- update temp_db.city_newest_deal_data_check set room_code = room_sale_area where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set room_sale_area = room_sale_state where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- update temp_db.city_newest_deal_data_check set room_sale_state = '' where  city_name = '扬州' and issue_code  = '' and issue_date != '';
-- 
update temp_db.city_newest_deal_data_check set business = issue_code where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set issue_date = open_date where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set open_date = sale_state where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set sale_state = room_sum where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set room_sum = simulation_price where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';


update temp_db.city_newest_deal_data_check set building_code = area where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set area = '' where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';

update temp_db.city_newest_deal_data_check set simulation_price = sale_address where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set sale_address = '' where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';


update temp_db.city_newest_deal_data_check set sale_telephone = room_code where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set room_code = '' where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';


update temp_db.city_newest_deal_data_check set issue_code = issue_date where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';
update temp_db.city_newest_deal_data_check set issue_date = '' where  city_name = '扬州' and issue_code = '扬州北湖房地产开发有限公司';



select STR_TO_DATE(replace(replace(open_date,'预计',''),'即将上市',''),'%Y-%m-%d'),open_date from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州' and open_date != '';

update temp_db.city_newest_deal_data_check set issue_date = STR_TO_DATE(replace(open_date,'预计',''),'%Y-%m-%d') where city_name = '扬州' and open_date != '' and open_date != '即将上市';

update temp_db.city_newest_deal_data_check set issue_date_clean = issue_date where city_name = '扬州';

update temp_db.city_newest_deal_data_check set create_time = '20210702' where city_name = '扬州';

select * from temp_db.tmp_city_newest_deal where city_name = '扬州';

insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '扬州';


--------------------------------------------- =========================== ---------------------------------------------------







truncate table  temp_db.tmp_city_newest_deal_shenzhen;

insert into temp_db.tmp_city_newest_deal_huizhou select * from temp_db.tmp_city_newest_deal where city_name = '惠州';

delete from temp_db.tmp_city_newest_deal where city_name = '惠州';









select city_name from temp_db.city_newest_deal_data_check cnddc group by city_name ;
select city_name from temp_db.tmp_city_newest_deal cnddc group by city_name ;
-- 南京
-- 广州
-- 扬州
-- 沈阳
-- 珠海
-- 石家庄
-- 长春

select * from temp_db.tmp_city_newest_deal cnddc where city_name = '南京';  -- 无

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '南京';   -- issue_date 时间没有

update temp_db.city_newest_deal_data_check set issue_date=STR_TO_DATE(open_date,'%Y-%m-%d') where city_name = '南京';

update temp_db.city_newest_deal_data_check set issue_date_clean=issue_date where city_name = '南京';

insert into temp_db.tmp_city_newest_deal select * from temp_db.city_newest_deal_data_check where city_name = '南京';

update temp_db.tmp_city_newest_deal set insert_time='20210701' where city_name = '南京';

delete from temp_db.city_newest_deal_data_check where city_name in ('南京','广州') ; 





select * from temp_db.tmp_city_newest_deal cnddc where city_name = '广州';  

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '广州';  

delete from temp_db.city_newest_deal_data_check where city_name = '广州'; 






select * from temp_db.tmp_city_newest_deal cnddc where city_name = '扬州';    -- 无

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '扬州';   -- 无

delete from temp_db.tmp_city_newest_deal where city_name = '扬州'; 
delete from temp_db.city_newest_deal_data_check where city_name = '扬州'; 



select * from temp_db.tmp_city_newest_deal cnddc where city_name = '沈阳';    -- 无

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '沈阳';   -- 需要清洗

delete from temp_db.tmp_city_newest_deal where city_name = '沈阳'; 




select * from temp_db.tmp_city_newest_deal cnddc where city_name = '珠海'; 

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '珠海'; 

delete from temp_db.city_newest_deal_data_check where city_name = '珠海'; 





select * from temp_db.tmp_city_newest_deal cnddc where city_name = '石家庄';  -- 无

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '石家庄';  -- 没有房屋面积

delete from temp_db.tmp_city_newest_deal where city_name = '石家庄'; 

delete from temp_db.city_newest_deal_data_check where city_name = '石家庄'; 



select * from temp_db.tmp_city_newest_deal cnddc where city_name = '长春'; 

select * from temp_db.city_newest_deal_data_check cnddc where city_name = '长春'; 

delete from temp_db.city_newest_deal_data_check where city_name = '长春'; 


--------------------------------------------- =========================== ---------------------------------------------------






insert into temp_db.tmp_city_newest_deal_shenzhen select * from temp_db.tmp_city_newest_deal where city_name = '南通';

delete from temp_db.tmp_city_newest_deal_shenzhen where city_name = '深圳';

select * from temp_db.tmp_city_newest_deal_shenzhen where city_name = '南通';

update temp_db.tmp_city_newest_deal_shenzhen 
set issue_date=STR_TO_DATE(replace(replace(replace(open_date,'年','/'),'月','/'),'日',''),'%Y/%m/%d');

update temp_db.tmp_city_newest_deal_shenzhen 
set issue_date_clean = issue_date;

delete from temp_db.tmp_city_newest_deal where city_name = '南通';

insert into temp_db.tmp_city_newest_deal select * from temp_db.tmp_city_newest_deal_shenzhen;



--------------------------------------------- =========================== ---------------------------------------------------


delete from temp_db.tmp_city_newest_deal_zhaoqing where city_name = '肇庆';


insert into temp_db.tmp_city_newest_deal_zhaoqing select * from temp_db.tmp_city_newest_deal where city_name = '肇庆';

update temp_db.tmp_city_newest_deal_zhaoqing set issue_date_clean = null ;

select
	STR_TO_DATE(replace(replace(replace(substring_index(substring_index(issue_date, '自',-1), '至', 1),'年','/'),'月','/'),'日',''),'%Y/%m/%d')
from
	temp_db.tmp_city_newest_deal_zhaoqing where issue_date != 'None';
-- 469,698


update temp_db.tmp_city_newest_deal_zhaoqing 
set issue_date_clean=STR_TO_DATE(replace(replace(replace(substring_index(substring_index(issue_date,'自',-1),'至',1),'年','/'),'月','/'),'日',''),'%Y/%m/%d') 
where issue_date != 'None';

delete from temp_db.tmp_city_newest_deal where city_name in ('肇庆');

insert into temp_db.tmp_city_newest_deal select * from temp_db.tmp_city_newest_deal_zhaoqing;





--------------------------------------------- =========================== ---------------------------------------------------



insert into temp_db.tmp_city_newest_deal_opendate select * from temp_db.tmp_city_newest_deal where city_name in ('合肥','成都','青岛');

delete from temp_db.tmp_city_newest_deal_opendate where city_name in ('成都');

delete from temp_db.tmp_city_newest_deal where city_name in ('合肥','青岛');

truncate table temp_db.tmp_city_newest_deal_opendate;


update temp_db.tmp_city_newest_deal_opendate set issue_date = STR_TO_DATE(open_date,'%Y/%m/%d') where open_date like '%/%';
update temp_db.tmp_city_newest_deal_opendate set issue_date = substr(open_date,1,10) where length(open_date)=19;
update temp_db.tmp_city_newest_deal_opendate set issue_date_clean = issue_date;

insert into temp_db.tmp_city_newest_deal select * from temp_db.tmp_city_newest_deal_opendate where issue_date_clean != '0001-01-01';


-- 1,367,016

select STR_TO_DATE(open_date,'%Y/%m/%d') from temp_db.tmp_city_newest_deal_opendate where open_date like '%/%';   -- 774,698

select substr(open_date,1,10) from temp_db.tmp_city_newest_deal_opendate where length(open_date)=19;   -- 589,696



select length(open_date) from temp_db.tmp_city_newest_deal_opendate group by length(open_date);
-- 2
-- 8
-- 9
-- 10
-- 19

select * from temp_db.tmp_city_newest_deal_opendate where  length(open_date) = 9;



show create table temp_db.tmp_city_newest_deal ;


CREATE TABLE temp_db.tmp_city_newest_deal_opendate (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) DEFAULT NULL COMMENT '网址',
  `city_name` varchar(32) NOT NULL COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) DEFAULT NULL COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `insert_time` text COMMENT '插入时间',
  PRIMARY KEY (`id`,`city_name`),
  KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
  KEY `idx_newest_city_name` (`city_name`) USING BTREE,
  KEY `idx_newest_url` (`url`) USING BTREE,
  KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=25124930 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表'
PARTITION BY LINEAR KEY (city_name) PARTITIONS 128;

--------------------------------------------- =========================== ---------------------------------------------------







select open_date from temp_db.tmp_city_newest_deal tcnd where city_name in ('') group by open_date ;

select issue_date from temp_db.tmp_city_newest_deal tcnd where city_name in ('西安') group by issue_date ;

select floor_name from temp_db.tmp_city_newest_deal tcnd where city_name in ('丽水') group by floor_name ;

select * from  temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') and issue_code like '%20180042%';

select * from temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') group by issue_code in ('20150055');

select t1.s1, from 
(select issue_code,count(1) s1 from temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') and issue_date >= '2020/10/1' group by issue_code) t1   -- 251
left join 
(select issue_code,count(1) s2,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') and issue_date >= '2020/10/1' group by issue_code,room_sum having s2 > replace(room_sum,'户','')) t2  -- 10 
left join 
(select issue_code,count(1) s3,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('海口') and issue_date >= '2020/10/1' group by issue_code,room_sum having s3 < replace(room_sum,'户','')) t3 ; -- 17


select issue_code,count(1) s3,room_sum from temp_db.tmp_city_newest_deal tcnd where city_name in ('深圳') and issue_date >= '2020' group by issue_code,room_sum having s3 != room_sum;



select count(1) from (
select floor_name from  temp_db.tmp_city_newest_deal tcnd where city_name in ('哈尔滨') group by floor_name)t ;


select count(1) from temp_db.tmp_city_newest_deal tcnd where city_name in ('青岛');





select open_date from temp_db.city_newest_deal_data_check tcnd where city_name in ('扬州') group by open_date ;

select count(1) from (
select floor_name from  temp_db.city_newest_deal_data_check tcnd where city_name in ('扬州') group by floor_name)t ;


select count(1) from temp_db.city_newest_deal_data_check tcnd where city_name in ('扬州');





--------------------------------------------- =========================== ---------------------------------------------------





select city_name from dws_db.dim_geography dg where city_name not in ('宝鸡市','北京市','佛山市','福州市','哈尔滨市','海口市','杭州市','惠州市','嘉兴市','九江市','丽水市','南昌市','南宁市','厦门市','深圳市','唐山市','温州市','无锡市','西安市','徐州市','长沙市','中山市','淄博市') group by city_name ;



select gd_city,max(url) from temp_db.tmp_city_newest_deal tcnd group by gd_city ;


SELECT DISTINCT city_id,city_name FROM dws_area_detail;







--------------------------------------------- =========================== ---------------------------------------------------

select dnptq.*,dni.newest_name from  dws_db.dws_customer_week dnptq , dws_db.dws_newest_info dni where dnptq.newest_id = dni.newest_id and dnptq.city_id in ('370800');


select dnptq.*,dni.newest_name from  dws_db.dws_customer_month dnptq , dws_db.dws_newest_info dni where dnptq.newest_id = dni.newest_id and dnptq.city_id in ('370800');







select dnptq.*,dni.newest_name from  dws_db.dws_newest_popularity_top30_quarter dnptq , dws_db.dws_newest_info dni where dnptq.newest_id = dni.newest_id and dnptq.city_id in ('370800');

select newest_id,imei from dwb_db.dwb_customer_browse_log;


truncate table dws_db.dws_newest_popularity_top30_quarter ;


show create table dws_db.dws_newest_popularity_top30_quarter_20210623 ;

CREATE TABLE temp_db.dws_newest_popularity_top30_quarter_bak_20210628 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` text COMMENT '城市/区域代码',
  `newest_id` text COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘排名',
  `period` text COMMENT '时间周期',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=41339 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度top30';


delete from dws_db.dws_customer_cre where period = '2021Q1';
delete from dws_db.dws_newest_city_qua where period = '2020Q4';

select * from dws_db.dws_customer_cre where period = '2021Q1';

select * from dws_db.dws_newest_city_qua where period = '2021Q1';


select period,count(1) from dws_db.dws_newest_city_qua_20210623 dncq group by period ;


select
	distinct imei,
	visit_date date
from
	dwb_db.dwb_customer_browse_log
where
	visit_date >= '2021-01-01'
	and visit_date<'2021-02-01'
	and newest_id in ('000d60f3627442c5a7c1665a4323e258');


select * from dwb_db.dwb_customer_browse_log dcbl left join (
select newest_id from dws_db.dws_newest_period_admit a inner join (select * from dws_db.dws_newest_info dni where county_id in ('110101')) b on a.newest_id = b.newest_id;
)t where dcbl.newest_id = t.newest_id;













select * from dws_customer_cre where newest_id = '242006bd75d8721ab9d75203ad2f8f28';

select * from dws_customer_sum where newest_id = '242006bd75d8721ab9d75203ad2f8f28';

select
	a.*,
	b.*
from
	(
	select
		newest_id,
		period,
		sum(imei_num) cou_imei
	from
		dws_customer_cre
	group by
		newest_id,
		period) a
left join dws_customer_sum b on
	a.period = b.period
	and a.newest_id = b.newest_id
where
	a.cou_imei <> b.cou_imei;



select
	a.*,
	b.*
from
	(
	select
		newest_id,
		period,
		sum(imei_num) cou_imei
	from
		dws_customer_month
	group by
		newest_id,
		period) a
left join dws_customer_sum b on
	a.period = b.period
	and a.newest_id = b.newest_id
where
	a.cou_imei <> b.cou_imei;










select * from dws_db.dws_customer_month dcm where 



select
	a.*,
	b.*
from
	(
	select
		newest_id,
		period,
		sum(imei_num) cou_imei,
		exist 
	from
		dws_customer_month
	group by
		newest_id,
		period) a
left join 
(
	select
		newest_id,
		period,
		sum(imei_num) cou_imei,
		exist
	from
		dws_customer_cre
	group by
		newest_id,
		period)  b on
	a.period = b.period
	and a.newest_id = b.newest_id
	and a.exist = b.exist 
where
	a.cou_imei <> b.cou_imei;




select * from temp_db.dwd_customer_exsits_test dcet where browser_previod_sign = '3' and browser_previod = '2021Q1';



SELECT
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'dwb_db'
AND TABLE_NAME = 'dwb_customer_browse_log';



show create table dws_db.dws_newest_provide_sche ;

CREATE TABLE dws_db.dws_newest_provide_sche_20210719 (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `newest_id` varchar(200) DEFAULT NULL COMMENT '楼盘id',
  `date` date DEFAULT NULL COMMENT '日期',
  `period` varchar(255) DEFAULT NULL COMMENT '时间周期',	
  `provide_title` varchar(500) DEFAULT NULL COMMENT '动态标题',
  `provide_sche` text COMMENT '动态正文',
  PRIMARY KEY (`id`),
  KEY ```newest_id``` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=194047 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘动态表';






UPDATE dws_db.dws_newest_city_qua SET county_id = NULL WHERE county_id = 'NULL' OR county_id = '';

select * from dws_db.dws_newest_city_qua dncq where county_id = 'NULL' OR county_id = '';

select '旧表',period,count(1) from dws_db.dws_customer_cre group by period 
union all
select '新表',period,count(1) from dws_db.dws_customer_cre_20210622 group by period ;



select '旧表',period,count(1) from dws_db.dws_customer_month group by period 
union all
select '新表',period,count(1) from dws_db.dws_customer_month_20210622 group by period ;



select '旧表',period,count(1) from dws_db.dws_customer_sum group by period 
union all
select '新表',period,count(1) from dws_db.dws_customer_sum_20210622 group by period ;


odsdb.cust_browse_log_202004_202103

select week from dws_db.dws_customer_week_20210623 dcw2 group by week ;




delete from dws_db.dws_customer_week_20210622 where week = '2021-14';


insert into dws_db.dws_customer_week_20210622 select city_id,newest_id,week,exist,imei_num,period from dws_db.dws_customer_week where week = '2020-40';

--------------------------------------------- =========================== ---------------------------------------------------



select distinct url from  temp_db.city_newest_deal_data_check cnddc where city_name in ('石家庄') and substr(issue_date_clean,1,4)>=2020; 






select floor_name from temp_db.city_newest_deal_data_check cnddc where floor_name like '%	%';
 
update temp_db.city_newest_deal_data_check set address =replace(address,'	','') where address like '%	%';
 
update temp_db.city_newest_deal_data_check set address = '' where address in ('0');
update temp_db.city_newest_deal_data_check set address = '' where address in ('1');

update temp_db.city_newest_deal_data_check set business =replace(business,'	','') where business like '%	%';


update temp_db.city_newest_deal_data_check set issue_code =replace(issue_code,'	','') where issue_code like '%	%';

select issue_code from temp_db.city_newest_deal_data_check cnddc where issue_code like '%	%';

select * from temp_db.city_newest_deal_data_check cnddc where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set business =issue_code  where issue_code in ('扬州华侨城实业发展有限公司');

select * from temp_db.city_newest_deal_data_check cnddc where business in ('扬州华侨城实业发展有限公司');

update temp_db.city_newest_deal_data_check set issue_code =issue_date where issue_code in ('扬州华侨城实业发展有限公司');




 update temp_db.city_newest_deal_data_check set issue_date = ''  where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set open_date= issue_area where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');
update temp_db.city_newest_deal_data_check set issue_area= '' where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set sale_state= building_code where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');


select count(1) from dws_db.dws_newest_city_qua dncq group by period ;



update temp_db.city_newest_deal_data_check set building_code= room_sum where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set room_sum= area where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set area= '' where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

update temp_db.city_newest_deal_data_check set simulation_price = sale_telephone  where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

 update temp_db.city_newest_deal_data_check set sale_telephone = sale_address  where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');

 update temp_db.city_newest_deal_data_check set sale_address = ''  where business in ('扬州华侨城实业发展有限公司') and floor_name in ('华侨城·侨城里');










--------------------------------------------- =========================== ---------------------------------------------------










create table dws_db.dws_customer_cre_20210622 like dws_db.dws_customer_cre;
create table dws_db.dws_customer_month_20210622 like dws_db.dws_customer_month;
create table dws_db.dws_customer_sum_20210622 like dws_db.dws_customer_sum;
create table dws_db.dws_customer_week_20210622 like dws_db.dws_customer_week;
create table dws_db.dws_newest_city_qua_20210622 like dws_db.dws_newest_city_qua;
create table dws_db.dws_newest_popularity_top30_quarter_20210622 like dws_db.dws_newest_popularity_top30_quarter;
create table dws_db.dws_newest_investment_pop_top30_quarter_20210622 like dws_db.dws_newest_investment_pop_top30_quarter;


select max(substring_index(week,'-',-1)) from dws_db.dws_customer_week_20210622 where substr(week,1,4)=2020 ;

select max(substring_index(week,'-',-1)) from dws_db.dws_customer_week where substr(week,1,4)=2020 ;

select week,concat(substring_index(week,'-',1),'-',substring_index(week,'-',-1)+1) from dws_db.dws_customer_week;



update dws_db.dws_customer_week_20210622 set week = concat(substring_index(week,'-',1),'-',substring_index(week,'-',-1)+1);


truncate table dws_db.dws_customer_week_20210622;

truncate table dws_db.dws_newest_popularity_top30_quarter_20210622;

truncate table dws_db.dws_newest_investment_pop_top30_quarter_20210622;

truncate table dws_db.dws_newest_city_qua_20210622 ;




SELECT t.period FROM (
select a.*
from dws_db.dws_newest_city_qua_20210622 a left join dws_db.dws_newest_city_qua_bak_2021_06_11 b on a.county_id = b.county_id 
where b.county_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据

union all

select b.*,a.*
from dws_db.dws_newest_city_qua_bak_2021_06_11 b left join dws_db.dws_newest_city_qua_20210622 a ON a.county_id = b.county_id
where a.county_id is null

) t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据


select
	*
from
	dws_newest_period_admit
where
	newest_id in (
	select
		newest_id
	from
		dws_newest_period_admit
	where
		dr = 0
		and newest_id in (
		select
			newest_id
		from
			dws_newest_period_admit
		where
			update_time in ( '2021-06-22 13:59:57','2021-06-22 13:57:37.0'))  -- '2021-06-23 10:14:39.0',
	group by
		newest_id
	having
		count(period)= 1 );

 COMMENT '2021年6月22日备份'
 
 
 
 
 
 
--------------------------------------------- =========================== ---------------------------------------------------







SELECT
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'temp_db'
AND TABLE_NAME = 'tmp_city_newest_deal';


select imei,concern,intention,urgent,cre from dws_db.dws_imei_browse_tag where period = '2020Q4';




select city_name ,count(1) from (
select city_name ,issue_date ,open_date from  temp_db.city_newest_deal_data_check cnddc where issue_date in ('',null,'		') and open_date not in ('') ) t group by city_name ;

update temp_db.city_newest_deal_data_check t set issue_date = open_date where issue_date in ('',null,'		') and open_date not in (''); -- and city_name like ('%沈阳%');

update temp_db.city_newest_deal_data_check t set city_name = '沈阳' where city_name like ('%沈阳%');

select * from  temp_db.city_newest_deal_data_check cnddc where 

show variables like 'innodb_lock_wait_timeout';   -- 5

SHOW GLOBAL VARIABLES LIKE 'innodb_lock_wait_timeout';

SET innodb_lock_wait_timeout=5;

SET GLOBAL innodb_lock_wait_timeout=5;

unlock tables;
--------------------------------------------- =========================== ---------------------------------------------------


select count(1) from (
select city_name from temp_db.tmp_city_newest_deal_3 tcnd group by city_name) t;









--------------------------------------------- =========================== ---------------------------------------------------




select * from  temp_db.city_newest_deal_data_check cnddc where city_name in ('石家庄')

select gd_city ,city_name,count(1) from temp_db.city_newest_deal_data_check cnddc group by city_name,gd_city ;
-- 南京市	南京	519712
-- 	广州	3009625
-- 扬州市	扬州	38019
-- 	沈阳	2232321
-- 珠海市	珠海	113331
-- 石家庄市	石家庄	990081
-- 长春市	长春	1483948

select count(1) from temp_db.city_newest_deal_data_check cnddc where issue_date like '%/%';
-- 9588
select count(1) from temp_db.city_newest_deal_data_check cnddc where issue_date_clean is not null;
-- 9588


select count(1) from temp_db.city_newest_deal_data_check cnddc where issue_date not like '%-%' and issue_date not like '%/%' and issue_date != '';

select max(substr(issue_date,5,2)),min(substr(issue_date,5,2)) ,max(substr(issue_date,7,2)), min(substr(issue_date,7,2))
from temp_db.city_newest_deal_data_check cnddc 
where issue_date not like '%-%' 
and issue_date not like '%/%' 
and issue_date != ''
and substr(issue_date,5,2)>'00'
and substr(issue_date,7,2)<'32';


select count(1) 
from temp_db.city_newest_deal_data_check cnddc 
where issue_date not like '%-%' 
and issue_date not like '%/%' 
and issue_date != ''
and substr(issue_date,5,2)>'00'
and substr(issue_date,7,2)<'32';
-- 990452
select count(1) from temp_db.city_newest_deal_data_check cnddc where issue_date_clean is not null;
-- 1000040


select max(substr(issue_date,5,2)),min(substr(issue_date,5,2)) ,max(substr(issue_date,7,2)), min(substr(issue_date,7,2))
from temp_db.city_newest_deal_data_check cnddc 
where issue_date not like '%-%' 
and issue_date not like '%/%' 
and issue_date != ''
and substr(issue_date,5,2)>'00'
and substr(issue_date,7,2)<'32';

select LPAD(max(substring_index(substring_index(issue_date,'-',-2),'-',1)),2,0), 
       LPAD(min(substring_index(substring_index(issue_date,'-',-2),'-',1)),2,0),
       LPAD(max(substring_index(issue_date,'-',-1)),2,0),
       LPAD(min(substring_index(issue_date,'-',-1)),2,0)
from temp_db.city_newest_deal_data_check cnddc 
where issue_date like '%-%' 
and issue_date != ''
and substring_index(substring_index(issue_date,'-',-2),'-',1)>'00'
and substring_index(issue_date,'-',-1)<'32';


select concat(substring_index(issue_date,'-',1),
              '-',
              LPAD(substring_index(substring_index(issue_date,'-',-2),'-',1),2,0),
              '-',
              LPAD(substring_index(issue_date,'-',-1),2,0)) 
from temp_db.city_newest_deal_data_check cnddc where length(substring_index(substring_index(issue_date,'-',-2),'-',1))=1;


select issue_date ,substr(issue_date,1,4) 
from temp_db.city_newest_deal_data_check cnddc 
where substr(issue_date,1,4)>=2020 and issue_date like '%/%'; 

select issue_date ,substr(issue_date,1,4) 
from temp_db.city_newest_deal_data_check cnddc 
where substr(issue_date,1,4)>=2020 and issue_date like '%-%'; 

select issue_date ,substr(issue_date,1,4) 
from temp_db.city_newest_deal_data_check cnddc 
where substr(issue_date,1,4)>=2020 and issue_date not like '%-%' and issue_date not like '%/%'; 


select count(1) from temp_db.city_newest_deal_data_check cnddc where substr(issue_date,1,4)>=2020;
-- 688894

create table temp_db.tmp_city_newest_deal_test like temp_db.tmp_city_newest_deal;


update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('珠海'); 

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('长春');  

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('南京');  

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('扬州');  

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('石家庄'); 

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('沈阳'); 

update temp_db.city_newest_deal_data_check set gd_city = concat(city_name,'市') where city_name in ('广州'); 



update temp_db.city_newest_deal_data_check set issue_date_clean = STR_TO_DATE(issue_date,'%Y/%m/%d') where issue_date like '%/%';

update temp_db.city_newest_deal_data_check set issue_date_clean = STR_TO_DATE(issue_date,'%Y%m%d') 
where issue_date not like '%-%' 
and issue_date not like '%/%' 
and issue_date != ''
and substr(issue_date,5,2)>'00'
and substr(issue_date,7,2)<'32';


select city_name        from temp_db.city_newest_deal_data_check cnddc group by city_name;    
select gd_city          from temp_db.city_newest_deal_data_check cnddc group by gd_city;        
select floor_name       from temp_db.city_newest_deal_data_check cnddc group by floor_name; 
select floor_name_new   from temp_db.city_newest_deal_data_check cnddc group by floor_name_new;     
select clean_floor_name from temp_db.city_newest_deal_data_check cnddc group by clean_floor_name;       
select floor_name_clean from temp_db.city_newest_deal_data_check cnddc group by floor_name_clean;       
select address          from temp_db.city_newest_deal_data_check cnddc group by address;   
select business         from temp_db.city_newest_deal_data_check cnddc group by business;      
select issue_code       from temp_db.city_newest_deal_data_check cnddc group by issue_code; 
select issue_date       from temp_db.city_newest_deal_data_check cnddc group by issue_date; 
select issue_date_clean from temp_db.city_newest_deal_data_check cnddc group by issue_date_clean;       
select open_date        from temp_db.city_newest_deal_data_check cnddc group by open_date;
select issue_area       from temp_db.city_newest_deal_data_check cnddc group by issue_area; 
select sale_state       from temp_db.city_newest_deal_data_check cnddc group by sale_state; 
select building_code    from temp_db.city_newest_deal_data_check cnddc group by building_code;    
select room_sum         from temp_db.city_newest_deal_data_check cnddc group by room_sum;
select area             from temp_db.city_newest_deal_data_check cnddc group by area;
select simulation_price from temp_db.city_newest_deal_data_check cnddc group by simulation_price;       
select sale_telephone   from temp_db.city_newest_deal_data_check cnddc group by sale_telephone;     
select sale_address     from temp_db.city_newest_deal_data_check cnddc group by sale_address;   
select room_code        from temp_db.city_newest_deal_data_check cnddc group by room_code;
select room_sale_area   from temp_db.city_newest_deal_data_check cnddc group by room_sale_area;     
select room_sale_state  from temp_db.city_newest_deal_data_check cnddc group by room_sale_state;      
select create_time      from temp_db.city_newest_deal_data_check cnddc group by create_time;  





update temp_db.city_newest_deal_data_check set issue_date_clean 
= STR_TO_DATE(concat(substring_index(issue_date,'-',1),
              '-',
              LPAD(substring_index(substring_index(issue_date,'-',-2),'-',1),2,0),
              '-',
              LPAD(substring_index(issue_date,'-',-1),2,0))
,'%Y-%m-%d') 
where issue_date like '%-%' 
and issue_date != ''
and substring_index(substring_index(issue_date,'-',-2),'-',1)>'00'
and substring_index(issue_date,'-',-1)<'32';




-- 徐州数据查验
--  1. 新建表
--  2. 将数据加载进去后，检验数据合理性
--     1> 按照月份统计数据查看是否合理
--     2> 查看空值是否存在太多
--     3> 手动查看多个url验证套数，是否合理。ps:尤其是5000+的
--     4> 通过许可证日期、项目名称、城市名称、建筑号码查看是否存在太多重复值
--     5> 检查url中不包含http://www.的
-- 网址不能为空
-- 
-- 城市
-- 
-- 项目名字
-- 
-- 许可证编号和日期
-- 
-- 房间销售状态
-- 
-- 房间编号
-- 
-- 房间面积

use temp_db;

-- 检验数据
--   1>时间最大值和最小值
select
    city_name,
	max(issue_date),
	min(issue_date),count(1)
FROM
	temp_db.city_newest_deal_data_check
WHERE
	NOT isnull(issue_date)
	AND issue_date NOT IN ('')
	AND issue_date LIKE '%0%'
	AND LENGTH(issue_date)<18 
	AND LENGTH(issue_date) > 7 
group by city_name ;

-- 广州	2021-6-15	2005-1-11	2191314
-- 沈阳	2021-05-27	1990-01-01	10093


--   2>获取所有时间的值
SELECT issue_date FROM temp_db.city_newest_deal_data_check GROUP BY issue_date;

--   4>每年的套数
SELECT
	t.city_name,
	sum(t.room_sum) s,
	substr(t.issue_date, 1, 4)
FROM
	(SELECT DISTINCT issue_code, issue_date, room_sum, city_name FROM temp_db.city_newest_deal_data_check tcnd) t
GROUP BY
	t.city_name,
	substr(t.issue_date, 1, 4);
-- 广州	347513.0	
-- 广州	43469.0	2005
-- 广州	71314.0	2006
-- 广州	59145.0	2007
-- 广州	117276.0	2008
-- 广州	94842.0	2009
-- 广州	112102.0	2010
-- 广州	141847.0	2011
-- 广州	121917.0	2012
-- 广州	148125.0	2013
-- 广州	180692.0	2014
-- 广州	193811.0	2015
-- 广州	174122.0	2016
-- 广州	142090.0	2017
-- 广州	191412.0	2018
-- 广州	112944.0	2019
-- 广州	166373.0	2020
-- 广州	68591.0	2021
-- 沈阳	243313.0	
-- 沈阳	61603.0	1990
-- 沈阳	28485.0	2005
-- 沈阳	96358.0	2006
-- 沈阳	98793.0	2007
-- 沈阳	111457.0	2008
-- 沈阳	79682.0	2009
-- 沈阳	128460.0	2010
-- 沈阳	174734.0	2011
-- 沈阳	174466.0	2012
-- 沈阳	228949.0	2013
-- 沈阳	180478.0	2014
-- 沈阳	160180.0	2015
-- 沈阳	147123.0	2016
-- 沈阳	116914.0	2017
-- 沈阳	143328.0	2018
-- 沈阳	154426.0	2019
-- 沈阳	138955.0	2020
-- 沈阳	35845.0	2021
-- 石家庄	594889.0	

--   8>重复数据条数
SELECT 
     tt.c AS `去重后条数`,tcnd.c AS `总条数` ,tcnd.c-tt.c AS `重复数据`  
FROM 
  (SELECT count(1) c FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.city_newest_deal_data_check tcnd) t)tt
,
  (SELECT count(1) c FROM temp_db.city_newest_deal_data_check) tcnd;
--  33706	6231730	6198024

 
--  9>每月预证数量
select
    city_name,
	substr(issue_date, 1, 8),
	count(issue_code),
	count(DISTINCT issue_code)
FROM
	temp_db.city_newest_deal_data_check
WHERE
	issue_date != ''
GROUP BY
	substr(issue_date, 1, 8),city_name ;
-- 广州	2020/1/1	6460	12		
-- 广州	2020/1/2	1039	2		
-- 广州	2020/1/3	66	4		
-- 广州	2020/1/6	609	1		
-- 广州	2020/1/8	610	1		20
-- 广州	2020/2/1	945	1		
-- 广州	2020/2/2	1071	5		6
-- 广州	2020/3/1	822	3		
-- 广州	2020/3/2	9051	28		
-- 广州	2020/3/3	580	3		
-- 广州	2020/3/4	578	2		
-- 广州	2020/3/5	174	1		
-- 广州	2020/3/6	299	2		39
-- 广州	2020/4/1	7215	10		
-- 广州	2020/4/2	9009	34		
-- 广州	2020/4/3	2734	13		57
-- 广州	2020/5/1	2348	12		
-- 广州	2020/5/2	8234	34		
-- 广州	2020/5/4	82	1		
-- 广州	2020/5/6	442	3		
-- 广州	2020/5/9	82	1		51
-- 广州	2020/6/1	4242	23		
-- 广州	2020/6/2	4891	25		
-- 广州	2020/6/3	2430	15		
-- 广州	2020/6/4	838	2		
-- 广州	2020/6/8	1656	6		71
-- 广州	2020/7/1	2249	10		
-- 广州	2020/7/2	5748	27		
-- 广州	2020/7/3	3450	15		
-- 广州	2020/7/6	62	1		
-- 广州	2020/7/8	508	4		57
-- 广州	2020/8/1	3888	13		
-- 广州	2020/8/2	6113	33		
-- 广州	2020/8/3	481	2		
-- 广州	2020/8/4	537	3		
-- 广州	2020/8/5	1016	5		
-- 广州	2020/8/6	510	2		
-- 广州	2020/8/7	191	1		59
-- 广州	2020/9/1	11629	40		
-- 广州	2020/9/2	19126	72		
-- 广州	2020/9/3	5300	21		
-- 广州	2020/9/4	59	1		
-- 广州	2020/9/7	650	1		
-- 广州	2020/9/8	320	2		
-- 广州	2020/9/9	659	4		141
-- 广州	2020-10-	10156	36		36
-- 广州	2020-11-	19553	76		76
-- 广州	2020-12-	25414	106		106
-- 广州	2021/1/1	4608	20		
-- 广州	2021/1/2	5847	25		
-- 广州	2021/1/4	187	3		
-- 广州	2021/1/5	274	1		
-- 广州	2021/1/8	250	3		52
-- 广州	2021/2/1	1436	1		
-- 广州	2021/2/2	385	1		
-- 广州	2021/2/3	456	1		
-- 广州	2021/2/4	854	5		
-- 广州	2021/2/5	1567	2		
-- 广州	2021/2/7	595	5		
-- 广州	2021/2/8	194	2		
-- 广州	2021/2/9	1025	3		20
-- 广州	2021/3/1	3979	22		
-- 广州	2021/3/2	3831	13		
-- 广州	2021/3/3	2808	15		
-- 广州	2021/3/5	172	1		
-- 广州	2021/3/9	44	1		51
-- 广州	2021/4/1	4098	17		
-- 广州	2021/4/2	20233	70		
-- 广州	2021/4/3	2821	10		
-- 广州	2021/4/6	1920	3		
-- 广州	2021/4/7	745	4		
-- 广州	2021/4/8	636	3		
-- 广州	2021/4/9	2502	5		
-- 广州	2021/5/1	2934	11		
-- 广州	2021/5/2	2792	15		
-- 广州	2021/5/6	50	1		
-- 广州	2021/5/7	192	2		
-- 广州	2021/5/8	165	1		
-- 广州	2021/6/1	6894	16		
-- 广州	2021/6/2	28	1		
-- 广州	2021/6/4	603	2		
-- 广州	2021/6/9	3674	3		
-- 沈阳	2020-01-	23	23		
-- 沈阳	2020-02-	14	14		
-- 沈阳	2020-03-	56	56		
-- 沈阳	2020-04-	117	117		
-- 沈阳	2020-05-	96	96		
-- 沈阳	2020-06-	116	116		
-- 沈阳	2020-07-	118	118		
-- 沈阳	2020-08-	120	120		
-- 沈阳	2020-09-	131	131		
-- 沈阳	2020-10-	60	60		
-- 沈阳	2020-11-	98	98		
-- 沈阳	2020-12-	60	60		
-- 沈阳	2021-01-	33	33		
-- 沈阳	2021-02-	10	10		
-- 沈阳	2021-03-	53	53		
-- 沈阳	2021-04-	71	71		
-- 沈阳	2021-05-	80	80		


--   11>检查字段长度
SELECT * FROM temp_db.city_newest_deal_data_check  WHERE length(url)<56 ;
SELECT * FROM temp_db.city_newest_deal_data_check  WHERE length(city_name)>9 or length(city_name)<3;

-- 5> 检查url中不包含http://www.的
select  * from  temp_db.city_newest_deal_data_check cnddc where url not like '%http://%';

-- 网址不能为空
-- 城市
select '许可日期为空的',count(1) from (
select url from temp_db.city_newest_deal_data_check cndg where url in ('',null,'0')) a

-- 城市
select '许可日期为空的',count(1) from (
select url from temp_db.city_newest_deal_data_check cndg where city_name in ('',null,'0')) a

-- 许可证编号和日期
select a.city_name ,'许可日期为空的',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg where issue_date in ('',null,'0') and open_date in ('',null,'0') and city_name in ('广州') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 广州	许可日期为空的	1688
-- 扬州	许可日期为空的	112
-- 沈阳	许可日期为空的	32
-- 长春	许可日期为空的	4

-- 南京	项目总量	1866
-- 广州	项目总量	8049
-- 扬州	项目总量	187
-- 沈阳	项目总量	14357
-- 珠海	项目总量	192
-- 石家庄	项目总量	2029
-- 长春	项目总量	5240

select a.city_name ,'许可证编号为空的',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg where issue_code in ('',null,'0') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 广州	许可证编号为空的	1674
-- 扬州	许可证编号为空的	111
-- 沈阳	许可证编号为空的	3981
-- 南京	项目总量	1866
-- 广州	项目总量	8049
-- 扬州	项目总量	187
-- 沈阳	项目总量	14357
-- 珠海	项目总量	192
-- 石家庄	项目总量	2029
-- 长春	项目总量	5238


select a.city_name ,'房间销售状态为空的',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg where room_sale_state in ('',null,'0') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 扬州	房间销售状态为空的	187
-- 沈阳	房间销售状态为空的	13937
-- 珠海	房间销售状态为空的	8
-- 石家庄	房间销售状态为空的	28
-- 长春	房间销售状态为空的	82


-- 南京	项目总量	1866
-- 广州	项目总量	8049
-- 扬州	项目总量	187
-- 沈阳	项目总量	14357
-- 珠海	项目总量	192
-- 石家庄	项目总量	2029
-- 长春	项目总量	5240

select a.city_name ,'房间编号为空的',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg where room_code in ('',null,'0') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 南京	房间编号为空的	53
-- 扬州	房间编号为空的	185
-- 沈阳	房间编号为空的	13937
-- 石家庄	房间编号为空的	28
-- 长春	房间编号为空的	734


-- 南京	项目总量	1866
-- 广州	项目总量	8049
-- 扬州	项目总量	187
-- 沈阳	项目总量	14357
-- 珠海	项目总量	192
-- 石家庄	项目总量	2029
-- 长春	项目总量	5238

select a.city_name ,'房间面积为空的',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg where room_sale_area in ('',null,'0') and city_name in ('沈阳') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 广州	房间面积为空的	12
-- 沈阳	房间面积为空的	14102
-- 石家庄	房间面积为空的	2029

-- 广州	项目总量	8049
-- 沈阳	项目总量	14106
-- 石家庄	项目总量	2029


select a.city_name ,'项目名字为空的',count(1) from (
select city_name ,url  from temp_db.city_newest_deal_data_check cndg where floor_name in ('',null,'0') group by city_name,url) a group by a.city_name 
union all 
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 沈阳	项目名字为空的	1291
-- 石家庄	项目名字为空的	2

-- 南京	项目总量	1866
-- 广州	项目总量	8049
-- 扬州	项目总量	187
-- 沈阳	项目总量	14357
-- 珠海	项目总量	192
-- 石家庄	项目总量	2029
-- 长春	项目总量	5240






--------------------------------------------- =========================== ---------------------------------------------------

CREATE TABLE temp_db.city_newest_deal_data_check_1 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) COMMENT '网址',
  `city_name` varchar(32) COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `create_time` text COMMENT '插入时间',
  PRIMARY KEY (`id`),
KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
KEY `idx_newest_city_name` (`city_name`) USING BTREE,
KEY `idx_newest_url` (`url`) USING BTREE,
KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (id) PARTITIONS 8 ;




select b.city_name ,'项目总量',count(1) from (
select city_name ,floor_name from temp_db.city_newest_deal_data_check cndg group by city_name ,floor_name) b group by b.city_name;
-- 广州	项目总量	7818
-- 沈阳	项目总量	4365
-- 石家庄	项目总量	664
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url) b group by b.city_name;
-- 广州	项目总量	8049
-- 沈阳	项目总量	14106
-- 石家庄	项目总量	2029
select b.city_name ,'项目总量',count(1) from (
select city_name ,url from temp_db.city_newest_deal_data_check cndg group by city_name,url,floor_name,address,business) b group by b.city_name;
-- 广州	项目总量	8049
-- 沈阳	项目总量	14106
-- 石家庄	项目总量	2029






--------------------------------------------- =========================== ---------------------------------------------------




33706	6231730	6198024








select count(1),city_name from temp_db.tmp_city_newest_deal tcnd group by city_name;


select count(1),city_name from temp_db.tmp_city_newest_deal_1 tcnd group by city_name;


select count(1),city_name from temp_db.tmp_city_newest_deal_3 tcnd group by city_name;


select * from temp_db.tmp_city_newest_deal_2 tcnd;


--------------------------------------------- =========================== ---------------------------------------------------











SELECT 
url,
city_name,
gd_city,
floor_name,
floor_name_new,
clean_floor_name,
floor_name_clean,
address,
business,
issue_code,
issue_date,
issue_date_clean,
open_date,
issue_area,
sale_state,
building_code,
room_sum,
area,
simulation_price,
sale_telephone,
sale_address,
room_code,
room_sale_area,
room_sale_state,
create_time FROM temp_db.tmp_city_newest_deal where city_name in ("贵阳","武汉","佛山");







--------------------------------------------- =========================== ---------------------------------------------------











show create table temp_db.city_newest_deal_guangzhou ;

CREATE TABLE `city_newest_deal_data_check` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` text COMMENT '网址',
  `city_name` varchar(100) NOT NULL COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` text COMMENT '原始项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` text COMMENT '地址',
  `business` text COMMENT '公司',
  `issue_code` text COMMENT '许可证',
  `issue_date` text COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` text COMMENT '开盘日期',
  `issue_area` text COMMENT '许可证面积',
  `sale_state` text COMMENT '销售状态',
  `building_code` text COMMENT '建筑编号',
  `room_sum` text COMMENT '房间个数',
  `area` text COMMENT '房间面积',
  `simulation_price` text COMMENT '拟售价格',
  `sale_telephone` text COMMENT '销售电话',
  `sale_address` text COMMENT '销售地址',
  `room_code` text COMMENT '房间编号',
  `room_sale_area` text COMMENT '房间销售面积',
  `room_sale_state` text COMMENT '房间销售状态',
  PRIMARY KEY (`id`,`city_name`)
) ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (city_name) PARTITIONS 1000 ;

create table temp_db.city_newest_deal_data_check like odsdb.city_newest_deal_data_check;


insert into temp_db.city_newest_deal_data_check select * from temp_db.city_newest_deal_guangzhou;


insert into temp_db.city_newest_deal_data_check (url,city_name,floor_name,address,business,issue_code,issue_date,open_date,issue_area,sale_state,building_code,room_sum,area,simulation_price,sale_telephone,sale_address,room_code,room_sale_area,room_sale_state) values('	http://124.95.133.164/work/xjlp/build_list.jsp?xmmcid=340&xmmc=皇朝万鑫大厦	','	沈阳	','	皇朝万鑫大厦	','	和平区青年大街390号	','	沈阳皇朝万鑫房屋开发有限公司	','		','		','	2006-08-23	','		','		','	和平区青年大街392号	','	193	','		','		','		','		','	4101	','		','	已发证	');



select city_name from temp_db.city_newest_deal_data_check  group by city_name ;


select city_name from temp_db.city_newest_deal_guangzhou group by city_name ;   -- 12



show create table temp_db.tmp_city_newest_deal ;

CREATE TABLE temp_db.tmp_city_newest_deal_2 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) COMMENT '网址',
  `city_name` varchar(32) COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `create_time` text COMMENT '插入时间',
  PRIMARY KEY (`id`),
KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
KEY `idx_newest_city_name` (`city_name`) USING BTREE,
KEY `idx_newest_url` (`url`) USING BTREE,
KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (id) PARTITIONS 64 ;


CREATE TABLE temp_db.city_newest_deal_data_check_3 (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `url` varchar(765) COMMENT '网址',
  `city_name` varchar(32) COMMENT '城市名称',
  `gd_city` varchar(32) DEFAULT NULL COMMENT '高德城市名称',
  `floor_name` varchar(200) COMMENT '原始项目名称',
  `floor_name_new` varchar(200) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `clean_floor_name` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `floor_name_clean` varchar(255) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `address` varchar(200) DEFAULT NULL COMMENT '地址',
  `business` varchar(200) DEFAULT NULL COMMENT '公司',
  `issue_code` varchar(2000) DEFAULT NULL COMMENT '许可证',
  `issue_date` varchar(200) DEFAULT NULL COMMENT '发证日期',
  `issue_date_clean` varchar(200) DEFAULT NULL COMMENT '发证日期清洗结果',
  `open_date` varchar(200) DEFAULT NULL COMMENT '开盘日期',
  `issue_area` varchar(200) DEFAULT NULL COMMENT '许可证面积',
  `sale_state` varchar(200) DEFAULT NULL COMMENT '销售状态',
  `building_code` varchar(200) DEFAULT NULL COMMENT '建筑编号',
  `room_sum` varchar(200) DEFAULT NULL COMMENT '房间个数',
  `area` varchar(200) DEFAULT NULL COMMENT '房间面积',
  `simulation_price` varchar(200) DEFAULT NULL COMMENT '拟售价格',
  `sale_telephone` varchar(200) DEFAULT NULL COMMENT '销售电话',
  `sale_address` varchar(2000) DEFAULT NULL COMMENT '销售地址',
  `room_code` varchar(200) DEFAULT NULL COMMENT '房间编号',
  `room_sale_area` varchar(200) DEFAULT NULL COMMENT '房间销售面积',
  `room_sale_state` varchar(200) DEFAULT NULL COMMENT '房间销售状态',
  `create_time` text COMMENT '插入时间',
  PRIMARY KEY (`id`,`city_name`),
KEY `idx_newest_gd_city` (`gd_city`) USING BTREE,
KEY `idx_newest_city_name` (`city_name`) USING BTREE,
KEY `idx_newest_url` (`url`) USING BTREE,
KEY `idx_newest_floor_name` (`floor_name`) USING BTREE
)  ENGINE=InnoDB AUTO_INCREMENT=3243767 DEFAULT CHARSET=utf8mb4 COMMENT='新楼盘交易数据检验表' 
PARTITION BY LINEAR KEY (city_name) PARTITIONS 8 ;




insert into temp_db.tmp_city_newest_deal_2 select * from  temp_db.tmp_city_newest_deal_1;


select max(length(url)) from temp_db.tmp_city_newest_deal tcnd ;

--------------------------------------------- =========================== ---------------------------------------------------






select '面积为空的',count(1) from (
select floor_name from temp_db.city_newest_deal_guangzhou cndg where issue_area in ('') group by floor_name) t 
union all 
select '项目总量',count(1) from (
select floor_name from temp_db.city_newest_deal_guangzhou cndg group by floor_name) t 
;




select version() from dual;


show plugins;
--------------------------------------------- =========================== ---------------------------------------------------













create table temp_db.bak_20210617_dws_newest_investment_top30_quarter_bak_20210617 like dws_db.dws_newest_investment_pop_top30_quarter;


create table temp_db.city_newest_deal_guangzhou like temp_db.city_newest_deal_xuzhou;


truncate TABLE dws_db.dws_newest_popularity_top30_quarter ;



truncate TABLE dws_db.dws_newest_investment_pop_top30_quarter ;


SELECT city_id FROM dws_db.dim_geography WHERE city_name LIKE '%保定%'  group by city_id;

select * from dws_db.dws_newest_city_qua dncq where for_sale > 100;




-- 国贸佘山别墅
select a.*,b.*,c.* from
(select newest_id,alias_name,newest_name from dws_db.dws_newest_info dni where alias_name like '%国贸%' or newest_name like '%国贸%') a
left join (select newest_id,`date`,period,provide_title,provide_sche from dws_db.dws_newest_provide_sche dnps) b
  on a.newest_id=b.newest_id
left join (select uuid,url from odsdb.ori_newest_info_base) c
  on a.newest_id = c.uuid;

 
 
select uuid,url,provide_sche,block_name,city_name,newest_name 
from odsdb.ori_newest_info_base 
where uuid in ('591000a8a6092392f9992410d7364944') or newest_name like '%国贸佘山原墅%';



update table from dws_db.dws_newest_provide_sche set provicde_sche


--------------------------------------------- =========================== ---------------------------------------------------











select browser_previod,exist,count(1) from temp_db.dwd_customer_exsits_test dcet  where browser_previod_sign='2' group by browser_previod,exist ;

select period,cre,count(1) from dws_db.dws_imei_browse_tag group by period,cre ;


select browser_previod_sign from temp_db.dwd_customer_exsits_test dcet group by browser_previod_sign;





create table temp_db.dws_newest_city_qua_bak_2021_06_11 like dws_db.dws_newest_city_qua;



select * from 
(select t1.city_id,t1.county_id,t1.period,t1.unit_price from dws_db.dws_newest_city_qua t1 where unit_price < 5000) a
left  join 
(select city_id,region_id,city_name,region_name from dws_db.dim_geography) b
on a.city_id = b.city_id and a.county_id=b.region_id
;


truncate TABLE dws_db.dws_newest_city_qua ;





--------------------------------------------- =========================== ---------------------------------------------------








select distinct 
city_id,
newest_id,
imei_newest,
imei_city,
rate,
sort_id,
period
from dws_db.dws_newest_investment_pop_top30_quarter;





truncate TABLE dws_db.dws_newest_popularity_top30_quarter ;


truncate TABLE dws_db.dws_newest_investment_pop_top30_quarter ;


DELETE FROM dws_db.dws_tag_basic where period = '2020Q1';


select distinct city_id, newest_id,tag_value,value_num,total_num,ratio,tag_name,period from dws_db.dws_tag_basic;

select
	distinct city_id,
	newest_id,
	mobile_brand,
	mobile_model,
	model_num,
	brand_num,
	total_num,
	brand_ratio,
	model_ratio,
	tag_name,
	period
from
	dws_db.dws_tag_purchase_mobile;
-- 2116858      2119987

select count(1) from (
select
	city_id,
	newest_id,
	mobile_brand,
	mobile_model,
	model_num,
	brand_num,
	total_num,
	brand_ratio,
	model_ratio,
	tag_name,
	period
from
	dws_db.dws_tag_purchase_mobile
group by city_id,
	newest_id,
	mobile_brand,
	mobile_model,
	model_num,
	brand_num,
	total_num,
	brand_ratio,
	model_ratio,
	tag_name,
	period)t;


select distinct city_id,newest_id,tag_value,value_num,total_num,ratio,tag_name,period from dws_db.dws_tag_purchase;



select
	distinct city_id,
	newest_id,
	tag_value,
	workday_value_num,
	workday_total_num,
	holiday_value_num,
	holiday_total_num,
	workday_ratio,
	holiday_ratio,
	period
from
	dws_db.dws_tag_lifestyle_traffic;





select
	city_id,
	newest_id,
	tag_value,
	workday_value_num,
	workday_total_num,
	holiday_value_num,
	holiday_total_num,
	workday_ratio,
	holiday_ratio,
	period
from
	dws_db.dws_tag_lifestyle_traffic
group by city_id,
	newest_id,
	tag_value,
	workday_value_num,
	workday_total_num,
	holiday_value_num,
	holiday_total_num,
	workday_ratio,
	holiday_ratio,
	period;
-- 26531 42756




SELECT * FROM dws_db.dws_newest_info WHERE alias_name LIKE '%涿%' OR newest_name LIKE  '%涿%';

SELECT city_id FROM dws_db.dim_geography WHERE city_name LIKE '%保定%'  group by city_id;





--------------------------------------------- =========================== ---------------------------------------------------






CREATE TABLE `temp_db.dwd_customer_exsits` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `imei` varchar(500) NOT NULL COMMENT '客户号',
  `browser_previod` varchar(255) DEFAULT NULL COMMENT '浏览周期',
  `browser_previod_sign` int(11) DEFAULT NULL COMMENT '浏览周期区分标识',
  `exist`  varchar(255)  DEFAULT NULL COMMENT '增存量标识',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1116680 DEFAULT CHARSET=utf8mb4 COMMENT='客户增存量标识表的临时表';

SHOW CREATE TABLE temp_db.dws_tag_purchase_mobile_bak_2021_06_11;

--------------------------------------------- =========================== ---------------------------------------------------






CREATE TABLE temp_db.dws_tag_purchase_mobile_bak_2021_06_11 LIKE dws_db.dws_tag_purchase_mobile;

CREATE TABLE temp_db.dws_tag_lifestyle_traffic_bak_2021_06_11 LIKE dws_db.dws_tag_lifestyle_traffic;

select distinct newest_id,date,period,provide_title,provide_sche from dws_db.dws_newest_provide_sche;

truncate TABLE dws_db.dws_tag_purchase ;

-- UPDATE dws_db.dws_newest_city_qua SET county_id = NULL WHERE county_id = 'NULL' OR county_id = '';

SELECT
TABLE_NAME,
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'dws_db'
AND TABLE_NAME LIKE  '%dws_imei_browse_tag%';




--------------------------------------------- =========================== ---------------------------------------------------







SELECT DISTINCT county_id,newest_id,imei
FROM dwb_db.dwb_customer_browse_log  
WHERE visit_date >= '20201001' AND visit_date<'20210101' AND city_id = '330100';
-- 
-- 35192210121956  330105 1   330110 4
-- 
-- 35192210174952   330110 2   330112  2
-- 
-- 35192210231419     330108  2   330109   2    330112 1






SELECT * FROM dws_db.dws_newest_info WHERE alias_name LIKE '%隆基泰%' OR newest_name LIKE  '%隆基泰%';


SELECT * FROM dws_db.dim_geography WHERE city_name LIKE '%杭州%'; -- AND region_name LIKE '%高%'; -- 130600  130684


SELECT
	city_id,
	newest_id,
	exist,
	imei_num
FROM
	dws_db.dws_customer_cre
WHERE
	period = '2020Q4'
	AND newest_id IN (
	SELECT
		DISTINCT newest_id
	FROM
		dws_db.dws_newest_period_admit
	WHERE
		dr = 0);


CREATE TABLE temp_db.dws_newest_info_test LIKE dws_db.dws_newest_info;



truncate TABLE dws_db.dws_newest_city_qua;



--------------------------------------------- =========================== ---------------------------------------------------





UPDATE dwb_db.dwb_newest_info t1
SET t1.property_type = CASE 
                         WHEN t1.property_type IN ('普通住宅','住宅','普通住宅,商业','普通住宅,洋房,住宅底商','住宅,商业','普通住宅,住宅底商','普通住宅,写字楼商铺','住宅底商,商用公寓,LOFT','普通住宅,洋房,合院','普通住宅,公寓','普通住宅,大平层,酒店写字楼,购物中心,商业,商住公寓','普通住宅,商住公寓','普通住宅,联排','普通住宅,洋房,联排','商业,住宅','普通住宅,洋房','普通住宅,写字楼商铺,商用公寓','普通住宅,大平层,住宅底商','普通住宅,综合体','普通住宅,住宅底商,酒店式公寓','普通住宅,酒店式公寓','普通住宅,商业,综合体,商用公寓','普通住宅,联排,合院,商业') THEN '住宅类' 
                         WHEN t1.property_type IN ('酒店式公寓') THEN '公寓' 
                         WHEN t1.property_type IN ('商业','商住','商业,酒店式公寓,LOFT','别墅','商业,商住公寓,LOFT','综合体,商用公寓,酒店式公寓,LOFT,SOHO','商住公寓','别墅,商办楼','商住公寓,LOFT','酒店写字楼,企业独栋,商住别墅,商住公寓','商业,综合体,商住公寓,LOFT,SOHO','商业,LOFT','大平层','公寓,酒店式公寓,写字楼') THEN '商住' 
                         ELSE t1.property_type
                      END;
                      
                      
SELECT
	newest_id,
	newest_sn,
	newest_name,
	alias_name,
	city_id,
	county_id,
	address,
	lng,
	lat,
	property_type,
	property_fee,
	property_id,
	building_type,
	land_area,
	building_area,
	building_num,
	floor_num,
	household_num,
	right_term,
	green_rate,
	volume_rate,
	park_num,
	park_rate,
	decoration,
	sales_state,
	sale_address,
	opening_date,
	recent_opening_time,
	recent_delivery_time,
	unit_price,
	dr,
	create_date,
	create_user,
	update_date,
	update_user,
	`index`,
	jpg
FROM
	dws_db.dws_newest_info;
                      
-- 物业类型清洗规则：
-- 一、替换住宅类型关键字，生成property_sub住宅小类，替换对应关系详见《property_type归类》，对应类型为空即置空
-- 二、标准化住宅类型，生成property_cate住宅大类：
-- （1）property_sub包含住宅，property_cate为【住宅类】
-- （2）property_sub包含公寓且仅包含公寓、住宅配套、社会保障房、小产权房的组合，property_cate为【公寓】
-- （3）property_sub包含商业且仅包含商业、住宅配套的组合，property_cate为【商业】
-- （4）property_type仅包含底商、商铺、社区底商、住宅底商、临街商铺的组合，property_cate为【商业]
-- （5）其他property_sub包含商业、商住(公寓+其他已包含），property_cate为【商住】
-- （6）剩余property_cate为空
-- 三、只清洗【住宅类】【商住】【公寓】
                      
                      
                      
                      

SELECT t1.* FROM dws_db.dws_newest_info t1,temp_db.ori_newest_info_base_new_20210609_test t2 
WHERE t1.newest_id = t2.uuid;




--------------------------------------------- =========================== ---------------------------------------------------

SELECT layout_id,newest_id,room,hall,bathroom,layout_area,layout_area_str,layout_price,layout_price_str,dr,create_date,create_user,update_date,update_user  FROM dwb_db.dwb_newest_layout;




--------------------------------------------- =========================== ---------------------------------------------------



 
truncate TABLE dws_db.dws_newest_city_qua;



where newest_id in (select distinct newest_id from dws_newest_period_admit where dr = 0)




CREATE TABLE temp_db.dwd_customer_exsits_test  (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `imei` varchar(500) NOT NULL COMMENT '客户号',
  `browser_previod` varchar(255) NOT NULL COMMENT '浏览周期',
  `browser_previod_sign` int(10) NOT NULL COMMENT '浏览周期区分标识',
  `exist` varchar(255) NOT NULL COMMENT '增存量标识',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=99331 DEFAULT CHARSET=utf8mb4 COMMENT='客户增存量标识表';



CREATE TABLE `tmp_dws_customer_month_test` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(255) NOT NULL COMMENT '城市名称',
  `newest_id` varchar(255) NOT NULL COMMENT '楼盘id',
  `month` varchar(255) NOT NULL COMMENT '月份',
  `exist` varchar(255) NOT NULL COMMENT '增量/存量',
  `imei_num` varchar(255) NOT NULL COMMENT 'imei数量',
  `period` varchar(255) NOT NULL COMMENT '分析周期',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=99331 DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留月度结果表'








SHOW CREATE TABLE temp_db.tmp_dws_customer_month_test;


--------------------------------------------- =========================== ---------------------------------------------------







CREATE TABLE temp_db.tmp_dws_customer_month_test LIKE dws_db.dws_customer_month;

truncate TABLE temp_db.tmp_dws_customer_month_test;

CREATE TABLE temp_db.tmp_dws_customer_week_test LIKE dws_db.dws_customer_week;


CREATE TABLE temp_db.dws_tag_basic_bak_2021_06_11 LIKE dws_db.dws_tag_basic	;



--------------------------------------------- =========================== ---------------------------------------------------









-- SELECT count(1) FROM (
SELECT floor_name FROM 
(SELECT floor_name FROM odsdb.cust_browse_log_202004_202103 GROUP BY floor_name) t  -- 56984
INNER JOIN 
-- (SELECT newest_name FROM dws_db.dws_newest_alias GROUP BY newest_name) t2            -- 17666
-- (SELECT alias_name FROM dws_db.dws_newest_alias GROUP BY alias_name) t2                  -- 27156
-- --(SELECT alias_name,newest_name FROM dw_db.dws_newest_alias GROUP BY alias_name,newest_name) t2   -- 27158
-- (SELECT newest_name FROM dwb_db.dwb_newest_info GROUP BY newest_name) t2              -- 24092
-- (SELECT alias_name FROM dwb_db.dwb_newest_info GROUP BY alias_name) t2              -- 7064
(SELECT alias_name,newest_name FROM dwb_db.dwb_newest_info GROUP BY alias_name,newest_name) t2   
-- (SELECT newest_name FROM dwb_db.dwb_customer_browse_log GROUP BY newest_name) t2              -- 27817
ON t.floor_name = t2.newest_name OR t.floor_name = t2.alias_name                       -- 27158
GROUP BY floor_name;-- ) t3;




SELECT t.* FROM dws_db.dws_newest_city_qua t INNER JOIN 
(SELECT city_id FROM dws_db.dim_geography WHERE grade = '3' AND city_name = '杭州市') t1 ON  t.city_id=t1.city_id;
 








--------------------------------------------- =========================== ---------------------------------------------------









CREATE TABLE temp_db.ori_newest_info_base_new_20210609_test LIKE dwd_db.ori_newest_info_base_new_20210609;


CREATE TABLE temp_db.ori_newest_info_base_new_20210609_test LIKE dwd_db.ori_newest_info_base_new_20210609;

truncate TABLE  temp_db.ori_newest_info_base_new_20210609_test;



SELECT recent_opening_time,recent_delivery_time,issue_date FROM dwd_db.ori_newest_info_base_new_20210609 WHERE LENGTH(recent_opening_time)<10;

SELECT recent_opening_time,recent_delivery_time,issue_date FROM dwd_db.ori_newest_info_base_new_20210609 WHERE LENGTH(recent_delivery_time)<10;

SELECT recent_opening_time,recent_delivery_time,issue_date FROM dwd_db.ori_newest_info_base_new_20210609 WHERE LENGTH(issue_date)<10;



SELECT city_name,city_id FROM dws_db.dim_geography WHERE grade=3;








--------------------------------------------- =========================== ---------------------------------------------------








SELECT 
    t1.newest_id,
    ifnull(sum(one),0),
    ifnull(sum(two),0),
    ifnull(sum(three),0),
    ifnull(sum(four),0),
    ifnull(sum(four_out),0)
FROM 
    (SELECT
	    t.newest_id,
	    CASE WHEN t.room=1 THEN t.num END AS 'one',
	    CASE WHEN t.room=2 THEN t.num END AS 'two',
	    CASE WHEN t.room=3 THEN t.num END AS 'three',
	    CASE WHEN t.room=4 THEN t.num END AS 'four',
	    CASE WHEN t.room>4 THEN t.num END AS 'four_out'
	FROM 
	    (SELECT newest_id,room,count(1) num FROM dws_db.dws_newest_layout GROUP BY newest_id,room,hall,bathroom) t ) t1 
GROUP BY t1.newest_id;



SELECT
    t.newest_id,
	CASE WHEN t.room=1 THEN ifnull(sum(t.num),0) END AS 'one',
	CASE WHEN t.room=2 THEN ifnull(sum(t.num),0) END AS 'two',
	CASE WHEN t.room=3 THEN ifnull(sum(t.num),0) END AS 'three',
    CASE WHEN t.room=4 THEN ifnull(sum(t.num),0) END AS 'four',
    CASE WHEN t.room>4 THEN ifnull(sum(t.num),0) END AS 'four_out'
FROM 
    (SELECT newest_id,room,count(1) num FROM dws_db.dws_newest_layout GROUP BY newest_id,room,hall,bathroom) t
GROUP BY t.newest_id,room;









--------------------------------------------- =========================== ---------------------------------------------------












SELECT period,count(1) FROM dws_db.dws_newest_investment_pop_top30_quarter GROUP BY period;


truncate TABLE dws_db.dws_newest_investment_pop_top30_quarter;




SELECT * FROM dws_db.dws_newest_provide_sche WHERE LENGTH(provide_title)>80 AND LENGTH(provide_sche)<300;

SELECT * FROM dws_db.dws_newest_provide_sche WHERE provide_title LIKE '%许可证%';

SELECT uuid,city_id,provide_sche FROM odsdb.ori_newest_info_base WHERE provide_sche LIKE '%润扬观澜鹭岛已取证\|【润扬%';

truncate TABLE dws_db.dws_newest_provide_sche;

SELECT
TABLE_NAME,
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'dws_db'
AND TABLE_NAME LIKE  '%dws_newest_period_admit%';


--------------------------------------------- =========================== ---------------------------------------------------










SELECT * FROM dws_db.dws_newest_alias WHERE newest_name LIKE '%国贸佘山原墅%';


truncate TABLE dws_issue_info;

truncate TABLE dws_newest_alias;

truncate TABLE dws_newest_developer_info;




truncate TABLE dws_newest_developer_rs;

truncate TABLE dws_newest_layout;

truncate TABLE dws_property_info;





truncate TABLE dws_newest_planinfo;

truncate TABLE dws_newest_property_rs;





DELETE FROM dws_newest_planinfo WHERE period = 'NULL';









--------------------------------------------- =========================== ---------------------------------------------------











SELECT a0.newest_id,a0.property_id,a1.land_area,a2.property_fee,a1.city_id
FROM dws_db.dws_newest_property_rs a0
LEFT JOIN dwb_db.dwb_newest_info a1 ON a1.newest_id=a0.newest_id
LEFT JOIN odsdb.ori_newest_info_main a2 ON a2.uuid=a0.newest_id
WHERE a2.property_fee>0 AND a1.land_area>1000





--------------------------------------------- =========================== ---------------------------------------------------









CREATE TABLE temp_db.dws_newest_provide_sche_test LIKE dws_db.dws_newest_provide_sche;

truncate TABLE dws_db.dws_newest_provide_sche;


UPDATE dws_db.dws_newest_city_qua SET county_id = NULL WHERE county_id = 'NULL';

SELECT county_id,county_id+NULL FROM dws_db.dws_newest_city_qua WHERE county_id = 'NULL'

SELECT * FROM temp_db.tmp_city_newest_deal WHERE floor_name IN ('三亚');



show full processlist;

select * from information_schema.innodb_trx;








--------------------------------------------- =========================== ---------------------------------------------------








CREATE TABLE temp_db.tmp_newest_info_sche (`newest_id` text,`date` text,`period` text,`provide_title` text,`provide_sche` text);

CREATE TABLE temp_db.tmp_newest_info_sche (`uuid` text,`city_id` text,`platform` text,`provide_sche` text);

SELECT uuid,city_id,platform,provide_sche FROM odsdb.ori_newest_info_base LIMIT 100;

SELECT uuid,url,city_id,newest_name,platform,provide_sche FROM odsdb.ori_newest_info_base WHERE newest_name LIKE '%中天诚品%' AND provide_sche LIKE '%宇宏健康%';


SELECT count(1) FROM odsdb.ori_newest_info_base WHERE provide_sche NOT IN ('') OR provide_sche IS NULL;


SELECT * FROM  dwb_db.dwb_customer_browse_log LIMIT 10;
SELECT * FROM  dwb_db.dwb_customer_browse_log_copy LIMIT 10;

SELECT
CREATE_TIME,
UPDATE_TIME
FROM
INFORMATION_SCHEMA.TABLES
WHERE
TABLE_SCHEMA = 'odsdb'
AND TABLE_NAME = 'ori_newest_info_base';

show OPEN TABLES where In_use > 0;

SELECT * FROM INFORMATION_SCHEMA.INNODB_LOCKS;
SELECT * FROM INFORMATION_SCHEMA.INNODB_LOCK_WAITS;

show processlist;

CREATE TABLE temp_db.bak_dws_newest_provide_sche LIKE dws_db.dws_newest_provide_sche;








--------------------------------------------- =========================== ---------------------------------------------------











SELECT province_id,city_id FROM dws_db.dim_geography WHERE province_name = '上海';

SELECT * FROM dws_newest_alias WHERE newest_id = 'dd9fbafb9bbc459ce555768d6e88c9e5';



SELECT * FROM dws_newest_alias WHERE newest_name = '映虹桥'; -- OR newest_name = '国贸佘山原墅';


SELECT
	*
FROM
	temp_db.dws_newest_investment_pop_top30_quarter_test
WHERE
	city_id = '130600';



SELECT * FROM temp_db.dws_newest_investment_pop_top30_quarter_test WHERE newest_id = '591000a8a6092392f9992410d7364944' OR newest_id = '7661c4fe4aa15ee1140c7acee8b0bfb5';

SELECT t.newest_id,count(1) FROM (SELECT
    DISTINCT
	newest_id,
	imei
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= 20201001
	AND visit_date<20210101
	AND newest_id IN ('7661c4fe4aa15ee1140c7acee8b0bfb5','591000a8a6092392f9992410d7364944')) t
GROUP BY
	t.newest_id ;

SELECT
    DISTINCT
	newest_id,
	count(1)
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= 20201001
	AND visit_date<20210101
	AND newest_id IN ('7661c4fe4aa15ee1140c7acee8b0bfb5','591000a8a6092392f9992410d7364944')
GROUP BY
	newest_id ;


SELECT t.newest_id,count(1) FROM (SELECT
    DISTINCT
	newest_id,
	imei
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= 20210101
	AND visit_date<20210401
	AND newest_id IN ('7661c4fe4aa15ee1140c7acee8b0bfb5','591000a8a6092392f9992410d7364944')) t
GROUP BY
	t.newest_id ;

SELECT
    DISTINCT
	newest_id,
	count(1)
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= 20210101
	AND visit_date<20210401
	AND newest_id IN ('7661c4fe4aa15ee1140c7acee8b0bfb5','591000a8a6092392f9992410d7364944')
GROUP BY
	newest_id ;


SELECT
	*
FROM
	dws_db.dws_newest_popularity_top30_quarter
WHERE
	period = '2020Q4'
	AND city_id = '130600'
	AND newest_id = '7661c4fe4aa15ee1140c7acee8b0bfb5';

SELECT
	newest_id,
	count(imei) imei
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= '20201001'
	AND visit_date<'20210101'
	AND newest_id LIKE 'c0f52d5ad6450153aa3ffd5868ede2bb'
GROUP BY
	newest_id
ORDER BY
	count(imei) DESC;
	

SELECT t.newest_id,count(1) FROM (SELECT
    DISTINCT
	newest_id,
	imei
FROM
	dwb_db.dwb_customer_browse_log
WHERE
	visit_date >= 20201001
	AND visit_date<20210101
	AND newest_id IN ('7661c4fe4aa15ee1140c7acee8b0bfb5','591000a8a6092392f9992410d7364944')) t
GROUP BY
	t.newest_id ;
	







--------------------------------------------- =========================== ---------------------------------------------------








SELECT count(1) FROM (
	SELECT
		DISTINCT city_id,
		county_id,
		imei
	FROM
		dwb_db.dwb_customer_browse_log t
	WHERE
		visit_date >= 20210101
		AND visit_date <20210401
	    AND city_id IN ('120000')
	    AND county_id IN ('120103')
)t1;

SELECT count(1) FROM (
SELECT city_id,county_id,imei,count(newest_id) FROM (
	SELECT
		DISTINCT city_id,
		county_id,
		newest_id,
		imei
	FROM
		dwb_db.dwb_customer_browse_log t
	WHERE
		visit_date >= 20210101
		AND visit_date <20210401
	    AND city_id IN ('120000')
	    AND county_id IN ('120103')
)t1 GROUP BY city_id,county_id,imei HAVING count(newest_id)>10 )t2;


--------------------------------------------- =========================== ---------------------------------------------------

SELECT * FROM dws_newest_alias WHERE city_id = '130600';

select distinct newest_id,city_id,county_id from dwb_db.dwb_newest_info WHERE city_id = '130600';


SELECT * FROM dws_db.dws_newest_popularity_top30_quarter where city_id IS NOT NULL AND period IN ('2020Q4')  LIMIT 10;  


CREATE TABLE temp_db.dws_newest_popularity_top30_quarter_test LIKE dws_db.dws_newest_popularity_top30_quarter;


SELECT * FROM temp_db.dws_newest_popularity_top30_quarter_test;

SELECT t.period FROM (
select a.*
from temp_db.dws_newest_popularity_top30_quarter_test a left join dws_db.dws_newest_popularity_top30_quarter b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.*
from dws_db.dws_newest_popularity_top30_quarter b left join temp_db.dws_newest_popularity_top30_quarter_test a ON a.city_id = b.city_id
where a.city_id is NULL
)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据







--------------------------------------------- =========================== ---------------------------------------------------



SELECT * FROM dws_db.dws_customer_month where city_id IS NOT NULL AND period IN ('2020Q4')  LIMIT 10;  


CREATE TABLE temp_db.dws_customer_month_test LIKE dws_db.dws_customer_month;

SELECT * FROM temp_db.dws_customer_month_test;

SELECT t.period FROM (
select a.*
from temp_db.dws_customer_month_test a left join dws_db.dws_customer_month b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.*
from dws_db.dws_customer_month b left join temp_db.dws_customer_month_test a ON a.city_id = b.city_id
where a.city_id is NULL
)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据

--------------------------------------------- =========================== ---------------------------------------------------


truncate table temp_db.dws_customer_cre_test;


SELECT
	b.*
FROM
	dws_db.dws_customer_cre b
LEFT JOIN temp_db.dws_customer_cre_test a ON
	a.city_id = b.city_id AND a.newest_id = b.newest_id AND a.exist = b.exist 
WHERE a.city_id IS NULL OR a.newest_id IS NULL ;




SELECT * FROM dws_db.dws_customer_cre where city_id IS NOT NULL AND period IN ('2020Q4')  LIMIT 10;  
SELECT * FROM dws_db.dws_customer_cre where city_id IS NOT NULL AND period IN ('2021Q1')  LIMIT 10;  

CREATE TABLE temp_db.dws_customer_cre_test LIKE dws_db.dws_customer_cre;

SELECT * FROM temp_db.dws_customer_cre_test;

SELECT t.period FROM (
select a.*
from temp_db.dws_customer_cre_test a left join dws_db.dws_customer_cre b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.*
from dws_db.dws_customer_cre b left join temp_db.dws_customer_cre_test a ON a.city_id = b.city_id
where a.city_id is NULL
)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据



--------------------------------------------- =========================== ---------------------------------------------------



SELECT * FROM dws_db.dws_customer_week where city_id IS NOT NULL AND period IN ('2020Q4')  LIMIT 10;  

CREATE TABLE temp_db.dws_customer_week_test LIKE dws_db.dws_customer_week;

SELECT * FROM dws_db.dws_customer_week LIMIT 10;
SELECT * FROM temp_db.dws_customer_week_test;

SELECT t.period FROM (
select a.*
from temp_db.dws_customer_week_test a left join dws_db.dws_customer_week b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.*
from dws_db.dws_customer_week b left join temp_db.dws_customer_week_test a ON a.city_id = b.city_id
where a.city_id is NULL
)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据







--------------------------------------------- =========================== ---------------------------------------------------






SELECT * FROM dws_customer_week LIMIT 10;

SELECT * FROM dws_db.dws_customer_sum where city_id IS NOT  NULL AND period IN ('2020Q4')  LIMIT 10; 

CREATE TABLE temp_db.dws_customer_sum_test LIKE dws_db.dws_customer_sum;

SELECT * FROM temp_db.dws_customer_sum_test;

SELECT * FROM dws_db.dws_customer_sum WHERE period  IN ('2020Q4');

SELECT t.period FROM (
select a.*
from temp_db.dws_customer_sum_test a left join dws_db.dws_customer_sum b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.*
from dws_db.dws_customer_sum b left join temp_db.dws_customer_sum_test a ON a.city_id = b.city_id
where a.city_id is NULL
)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据






--------------------------------------------- =========================== ---------------------------------------------------







SELECT count(a.city_id) FROM (
SELECT
	t1.*
FROM
	dws_newest_popularity_top30_quarter t1
INNER JOIN dws_newest_investment_pop_top30_quarter t2 ON
	t1.newest_id = t2.newest_id) a
WHERE a.period IN ('2020Q4') GROUP BY a.city_id;


SELECT * FROM  dws_db.dim_geography WHERE city_name IN ('保定市') AND region_name IN  ('高碑店市');

SELECT * FROM  temp_db.dws_newest_popularity_top30_quarter_test WHERE  city_id IN ('130684') OR city_id IN ('130600');

SELECT * FROM  temp_db.dws_newest_investment_pop_top30_quarter_test WHERE period IN ('2021Q1') AND (city_id IN ('130684') OR city_id IN ('130600'));

SELECT t.period FROM (
select a.city_id,a.newest_id,a.period
from temp_db.dws_newest_popularity_top30_quarter_test a left join dws_db.dws_newest_investment_pop_top30_quarter b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.city_id,b.newest_id,a.period
from dws_db.dws_newest_investment_pop_top30_quarter b left join temp_db.dws_newest_popularity_top30_quarter_test a ON a.city_id = b.city_id
where a.city_id is NULL)t GROUP BY t.period;   -- 这样查询的结果是B表中有而A表中没有的数据








SELECT * FROM temp_db.dws_newest_popularity_top30_quarter_test LIMIT 10;

SELECT * FROM temp_db.dws_newest_investment_pop_top30_quarter_test LIMIT 10;

DESC temp_db.dws_newest_popularity_top30_quarter_test;

DESC temp_db.dws_newest_investment_pop_top30_quarter_test;

CREATE TABLE temp_db.dws_newest_popularity_top30_quarter_test LIKE dws_db.dws_newest_popularity_top30_quarter;

CREATE TABLE temp_db.dws_newest_investment_pop_top30_quarter_test LIKE dws_db.dws_newest_investment_pop_top30_quarter;

select a.city_id,a.county_id,a.period,a.for_sale,a.on_sale,a.sell_out
from dws_newest_county_qua a left join dws_newest_city_qua b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.city_id,b.county_id,b.period,b.for_sale,b.on_sale,b.sell_out
from dws_newest_city_qua b left join dws_newest_county_qua a ON a.city_id = b.city_id
where a.city_id is NULL;   -- 这样查询的结果是B表中有而A表中没有的数据

 select distinct imei,'投资型' type  from dwb_db.dwb_customer_imei_tag where is_college_stu = '否' -- and period = "'''+date_quarter+'''"and marriage = '已婚' and education = '高' and have_child = '有'



SELECT count(1) FROM dws_db.dim_geography;

SELECT count(1) FROM dwb_db.dwb_customer_imei_tag;


SELECT resi_province FROM dwb_db.dwb_customer_imei_tag GROUP BY resi_province;

SELECT * FROM dwb_db.dwb_customer_imei_tag WHERE imei='86928704101558';

select 
    distinct imei,substring_index(substring_index(a.resi_province, ',', b.help_topic_id + 1), ',',- 1) NAME 
from (SELECT imei,resi_province from dwb_db.dwb_customer_imei_tag where resi_province is not null) a
  JOIN mysql.help_topic b 
    ON b.help_topic_id < ( LENGTH (a.resi_province) - LENGTH(REPLACE(a.resi_province, ',',''))+1);
    
UPDATE temp_db.city_newest_deal_huizhou SET url='1';

SELECT count(1) from dwb_db.dwb_customer_imei_tag where resi_province is not NULL;  ---2395940


SELECT count(1) from dwb_db.dwb_customer_imei_tag WHERE resi_city is not null;


SELECT resi_province,resi_city FROM dwb_db.dwb_customer_imei_tag GROUP BY resi_city,resi_province;

SELECT resi_city FROM dwb_db.dwb_customer_imei_tag GROUP BY resi_city ORDER BY resi_city DESC;

SELECT max(issue_code),min(issue_code) FROM temp_db.city_newest_deal_shenyang WHERE issue_date_clean IS NOT NULL ;

SELECT * FROM  temp_db.city_newest_deal_shenyang LIMIT 1;

SELECT resi_county FROM dwb_db.dwb_customer_imei_tag GROUP BY resi_county;

SELECT imei_x,imei_y FROM  dws_db.dws_tag_resident_area GROUP BY imei_x,imei_y ORDER BY imei_x DESC ;


select 
    distinct imei,substring_index(substring_index(a.resi_province, ',', b.help_topic_id + 1), ',',- 1) NAME 
from (SELECT imei,resi_province from dwb_db.dwb_customer_imei_tag where resi_province is not null) a
  JOIN mysql.help_topic b 
    ON b.help_topic_id < ( LENGTH (a.resi_province) - LENGTH(REPLACE(a.resi_province, ',',''))+1);

   
SELECT 
     a2.qqq,a2.nnn,aa.newest_id,aa.visit_date
FROM (
select 
 a1.qq qqq,substring_index(substring_index(a1.nn, '-', b1.help_topic_id + 1), ',',- 1) nnn
from
(select 
    imei qq,substring_index(substring_index(a.resi_county, ',', b.help_topic_id + 1), ',',- 1) nn 
from (SELECT imei,resi_county from dwb_db.dwb_customer_imei_tag where resi_county is not null) a
  JOIN mysql.help_topic b 
    ON b.help_topic_id < ( LENGTH (a.resi_county) - LENGTH(REPLACE(a.resi_county, ',',''))+1) 
) a1
JOIN mysql.help_topic b1 
    ON b1.help_topic_id < ( LENGTH (a1.nn) - LENGTH(REPLACE(a1.nn, ',',''))+1)
) a2
INNER JOIN 
(SELECT imei,newest_id,visit_date FROM dwb_db.dwb_customer_browse_log) aa ON a2.qqq=aa.imei ;

    
   
   
   
   
   
   
   
   
   
   