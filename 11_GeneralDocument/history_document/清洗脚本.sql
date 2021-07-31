-- 楼盘表 dwb_db.dwb_newest_info（全量）
INSERT ignore INTO dwb_db.dwb_newest_info (
	newest_id,
	newest_sn,
	newest_name,
	alias_name,
	address,
	city_id,
  county_id,  
	lng,
	lat,
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
	recent_opening_time,
	recent_delivery_time,
  
	unit_price,
	property_fee,
	property_type
) 
SELECT
	c.uuid,
	c.id,
	c.newest_name,
	c.alias,
	c.address,
  c.city_id,
  c.county_id,
	c.gd_lng, -- CAST(gd_lng AS DECIMAL(10,7)),
	c.gd_lat, -- CAST(gd_lat AS DECIMAL(10,7)),
	c.building_type,
	REPLACE (
		REPLACE (
			REPLACE (      
        IF(c.land_area="",0,CAST(c.land_area AS DECIMAL(12,2))),
				'暂无信息',
				''
			),
			'㎡',
			''
		),
		',',
		''
	),
	REPLACE (
		REPLACE (
			REPLACE (
       IF(c.building_area="",0,CAST(c.building_area AS DECIMAL(12,2))),
				'暂无信息',
				''
			),
			'㎡',
			''
		),
		',',
		''
	),
	c.building_num,
	c.floor_num,
  
  IF(c.household_num="",0,CAST(c.household_num AS SIGNED)),
	c.right_term,
	c.green_rate,
	IF(c.volume_rate="",0,CAST(c.volume_rate AS DECIMAL(10,3))),
	c.park_num,
	c.park_rate,
	c.decoration,
	c.sale_status, 
	c.sale_address,
	c.recent_opening_time,
	c.recent_delivery_time,
  IF(c.unit_price="",0,CAST(c.unit_price AS SIGNED)),
	property_fee,
	property_sub
FROM
	odsdb.ori_newest_info_main c where remark is null;

-- 更新property_id
update dwb_db.dwb_newest_info t,dwb_db.dwb_newest_property_rs r set t.property_id = r.property_id where t.newest_id=r.newest_id;

-- 更新park_rate
update dwb_db.dwb_newest_info set park_rate = SUBSTR(park_rate,2) where park_rate REGEXP '^0[0-9]';
update dwb_db.dwb_newest_info set park_rate = CONCAT(SUBSTR(park_rate,1,POSITION(":" in park_rate)),SUBSTR(SUBSTR(park_rate,POSITION(":" in park_rate)+1),2)) where park_rate REGEXP ':0[0-9]';

-- 更新sales_state
update  dwb_db.dwb_newest_info set sales_state='待售' where sales_state in('未开盘','即将开盘','下期待开','待租');
update  dwb_db.dwb_newest_info set sales_state='售罄' where sales_state in('二期售罄','四期售罄');
update  dwb_db.dwb_newest_info set sales_state='在售' where sales_state in('老盘加推','出租',''); 

-- 意向客户统计 dws_newest_city_qua 
-- 以2021第一季度为例

-- 基础数据
insert into dws_db.dws_newest_city_qua(city_id,county_id,period,unit_price) select city_id,county_id,'2021Q1',AVG(unit_price) from dwb_db.dwb_newest_info g GROUP BY city_id,county_id;

--  待售 在售 售罄  total
update dws_db.dws_newest_city_qua t set t.for_sale=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='待售' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.on_sale=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where  g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='在售' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.sell_out=IFNULL((select g.sales from(select city_id,county_id,sales_state,count(sales_state) as sales from dwb_db.dwb_newest_info  GROUP BY city_id,county_id,sales_state) g where  g.city_id=t.city_id and g.county_id=t.county_id and  g.sales_state='售罄' ),0) where t.period='2021Q1';
update dws_db.dws_newest_city_qua t set t.total_count=t.for_sale+t.on_sale+t.sell_out  where t.period='2021Q1';;

-- 关注 意向 紧急 
update dws_db.dws_newest_city_qua t set t.follow=(select count(DISTINCT imei) from dwb_db.dwb_customer_browse_log  where DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27' and city_id=t.city_id and county_id=t.county_id) where t.period='2021Q1';;

UPDATE dws_db.dws_newest_city_qua t
SET t.intention = IFNULL((
	SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				city_id,county_id,g.imei
			FROM
				 dwb_db.dwb_customer_browse_log g	
			WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27'	
			GROUP BY
				g.city_id,county_id,g.imei
			HAVING
				count(g.newest_id) > 3
		) a
		where a.city_id=t.city_id and a.county_id=t.county_id
),0) where t.period='2021Q1';


UPDATE dws_db.dws_newest_city_qua t
SET t.urgent = IFNULL((
	SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				city_id,county_id,g.imei
			FROM
				 dwb_db.dwb_customer_browse_log g	
			WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27'	
			GROUP BY
				g.city_id,county_id,g.imei
			HAVING
				count(g.newest_id) > 10
		) a
		where a.city_id=t.city_id and a.county_id=t.county_id
),0) where t.period='2021Q1';

-- 当季新增 当季留存
-- 将当季和上季的数据分别插入两张临时表
insert into temp_db.dwb_customer_browse_log select * from dwb_db.dwb_customer_browse_log g		WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-04-16';
insert into temp_db.dwb_customer_browse_log2 select * from dwb_db.dwb_customer_browse_log g		WHERE DATE_FORMAT(create_date,'%Y-%m-%d') = '2021-05-27';	
	
UPDATE dws_db.dws_newest_city_qua t set t.retained = IFNULL((
SELECT
		count(*)
	FROM
		(
			SELECT DISTINCT
				a.imei,
				a.city_id,
				a.county_id
			FROM
				temp_db.dwb_customer_browse_log a
			JOIN temp_db.dwb_customer_browse_log2 b ON a.city_id = b.city_id
			AND a.county_id = b.county_id
			AND a.imei = b.imei
		) c
    where c.city_id=t.city_id and c.county_id=t.county_id
),0) where t.period='2021Q1';

UPDATE dws_db.dws_newest_city_qua t set t.increase = t.follow-t.retained where t.period='2021Q1';



-- 配套信息dws_db.dws_tag_purchase_poi
-- 首先建立一张临时表 然后把poi信息合并洗到临时表里
drop table if EXISTS temp_db.newest_poi;
CREATE TABLE temp_db.newest_poi (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(20) DEFAULT NULL COMMENT '城市id',
  `poi_type` varchar(50) DEFAULT NULL COMMENT '类型',
  `poi_name` varchar(50) DEFAULT NULL COMMENT '名字',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  PRIMARY KEY (`id`),
  KEY `index_lng_lat` (`city_id`,`poi_name`)
) ENGINE=InnoDB AUTO_INCREMENT=560402 DEFAULT CHARSET=utf8mb4;
insert into temp_db.newest_poi(city_id,poi_type,poi_name,lng,lat)
select g.city_id,t.poi_type,t.poi_name,substring_index(t.lnglat,',',1) as lng,substring_index(t.lnglat,',',-1) as lat from dwb_db.dwb_newest_poi_gd_info t LEFT JOIN dw_v2.dim_geography g on g.grade=3 and t.city_name=g.city_short; 
insert into temp_db.newest_poi(city_id,poi_type,poi_name,lng,lat)
select g.city_id,t.business_type,t.mall_name,substring_index(t.gd_lnglat,',',1) as lng,substring_index(t.gd_lnglat,',',-1) as lat from dwb_db.dwb_newest_poi_mall_info t LEFT JOIN dw_v2.dim_geography g on g.grade=3 and t.ori_city=g.city_short; 
--  然后执行java代码清洗 bigdata-clean-task服务下 com.eju.bigdata.task.PoiTask的main 方法
--  ps 如果执行报错,可以按城市分组


-- 竞品动态dws_db.dws_newest_provide_sche
-- 第一步需要乔永峰先从楼盘表将provide_sche字段拆分到临时表 qyf_tmp.provideScheSplit2中 然后根据provideScheSplit2中数据清洗
insert into dws_db.dws_newest_provide_sche(newest_id,date,provide_title,provide_sche) 
select uuid, if(length(substring_index(substring_index(provide_sche,'|',2),'|',-1))<>9,null,STR_TO_DATE(substring_index(substring_index(provide_sche,'|',2),'|',-1),'%m/%d%Y')) AS date,
concat(substring_index(provide_sche,'|',2),'|',substring_index(provide_sche,'|',-1)) AS title,substring_index(substring_index(provide_sche,'|',-2),'|',1) as sche from qyf_tmp.provideScheSplit2 where substring_index(provide_sche,'|',1) = '楼盘动态';


insert into dws_db.dws_newest_provide_sche(newest_id,date,provide_title,provide_sche) 
select uuid, if(length(substring_index(substring_index(provide_sche,'|',2),'|',-1))<>9,null,STR_TO_DATE(substring_index(substring_index(provide_sche,'|',2),'|',-1),'%m/%d%Y')) AS date,
concat(substring_index(provide_sche,'|',2),'|',substring_index(provide_sche,'|',-1)) AS title,substring_index(substring_index(provide_sche,'|',-2),'|',1) as sche from qyf_tmp.provideScheSplit2 where substring_index(provide_sche,'|',1) = '证件信息';


insert into dws_db.dws_newest_provide_sche(newest_id,date,provide_title,provide_sche) 
select uuid, if(length(substring_index(provide_sche,'|',-1))<>17,null,STR_TO_DATE(substring_index(provide_sche,'|',-1),'%Y年%m月%d日')) AS date,substring_index(provide_sche,'|',2)  AS title,
substring_index(substring_index(provide_sche,'|',3),'|',-1) as sche from qyf_tmp.provideScheSplit2 where substring_index(provide_sche,'|',1) = '楼盘资讯';

insert into dws_db.dws_newest_provide_sche(newest_id,date,provide_title,provide_sche) 
select uuid,if(length(substring_index(provide_sche,'|',-1))<>10,null,substring_index(provide_sche,'|',-1)) AS date,substring_index(provide_sche,'|',1)  AS title,substring_index(substring_index(provide_sche,'|',2),'|',-1) as sche from qyf_tmp.provideScheSplit2 where provide_sche regexp '^\\[';

insert into dws_db.dws_newest_provide_sche(newest_id,date,provide_title,provide_sche) 
select uuid,if(length(substring_index(provide_sche,'|',-1))<>10,null,substring_index(provide_sche,'|',-1)) AS date,substring_index(provide_sche,'|',1)  AS title,substring_index(substring_index(provide_sche,'|',2),'|',-1) as sche from qyf_tmp.provideScheSplit2 where provide_sche regexp '^【';