--清洗惠州数据
-- 1. 时间存在年月日汉字的问题
-- 2. 城市名字结尾少了‘市’
-- 3. 原先表中存在惠州错误数据

-- 抽样查看脏数据
SELECT * FROM temp_db.city_newest_deal_huizhou WHERE isnull(url) OR isnull(city_name) OR isnull(floor_name) LIMIT 10;

SELECT concat_ws('-',substr(issue_date,1,4),substr(issue_date,6,2),substr(issue_date,8,2)) FROM temp_db.city_newest_deal_huizhou WHERE issue_date LIKE '%年%'  ORDER BY issue_date;


-- 删除表中惠州数据
select count(1) from temp_db.tmp_city_newest_deal where city_name in ('惠州'); --100W+

SELECT count(1) FROM temp_db.city_newest_deal_huizhou; --100W+

delete from temp_db.tmp_city_newest_deal where city_name in ('惠州');


-- 将数据按照规则清洗加载到表中
INSERT
	INTO
	temp_db.tmp_city_newest_deal (url,
	city_name,
	gd_city,
	floor_name,
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
	room_sale_state)
SELECT
	url,
	city_name,
	concat(city_name, '市'),
	floor_name,
	clean_floor_name,
	floor_name_clean,
	address,
	business,
	issue_code,
	issue_date r1,
	issue_date r2,
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
	room_sale_state 
FROM
	temp_db.city_newest_deal_huizhou 
WHERE 
	city_name IN ('惠州') 
AND 
	issue_date NOT LIKE '%年%' 

UNION ALL 

SELECT 
	url,
	city_name,
	concat(city_name, '市'),
	floor_name,
	clean_floor_name,
	floor_name_clean,
	address,
	business,
	issue_code,
	issue_date r1,
	concat_ws('-', substr(issue_date, 1, 4), LPAD(substr(issue_date, 6, 1), 2, '0'), substr(issue_date, 8, 2)) r2,
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
	room_sale_state 
FROM
	temp_db.city_newest_deal_huizhou 
WHERE
	city_name IN ('惠州') 
AND 
	issue_date LIKE '%年%' 
AND 
    concat_ws('-', substr(issue_date, 1, 4), LPAD(substr(issue_date, 6, 1), 2, '0'), substr(issue_date, 8, 2)) NOT LIKE '%日%'
    
UNION ALL 

SELECT
    t.url,
	t.city_name,
	concat(t.city_name, '市'),
	t.floor_name,
	t.clean_floor_name,
	t.floor_name_clean,
	t.address,
	t.business,
	t.issue_code,
	t.issue_date r1,
	concat_ws('-', substr(t.t, 1, 7), LPAD(substr(t.t, 9, 1), 2, '0')) r2,
	t.open_date,
	t.issue_area,
	t.sale_state,
	t.building_code,
	t.room_sum,
	t.area,
	t.simulation_price,
	t.sale_telephone,
	t.sale_address,
	t.room_code,
	t.room_sale_area,
	t.room_sale_state 
FROM
	(
	SELECT
		*,
		concat_ws('-', substr(issue_date, 1, 4), LPAD(substr(issue_date, 6, 1), 2, '0'), substr(issue_date, 8, 2)) t
	FROM
		temp_db.city_newest_deal_huizhou
	WHERE
		city_name IN ('惠州')
    AND
		issue_date LIKE '%年%') t 
WHERE
	t.t LIKE '%日%';