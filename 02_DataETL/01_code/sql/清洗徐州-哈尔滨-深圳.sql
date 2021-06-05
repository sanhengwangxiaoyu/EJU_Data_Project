SELECT issue_date FROM temp_db.city_newest_deal_haerbin GROUP  BY issue_date;

SELECT issue_date FROM temp_db.city_newest_deal_xuzhou GROUP  BY issue_date;

SELECT issue_date FROM temp_db.city_newest_deal_shenzhen GROUP  BY issue_date;

SELECT city_names FROM temp_db.tmp_city_newest_deal WHERE city_name IN ('徐州','深圳','哈尔滨');

select count(1) from temp_db.tmp_city_newest_deal where city_name in ('深圳'); 

select count(1) from temp_db.city_newest_deal_shenzhen; 

delete from temp_db.tmp_city_newest_deal where city_name in ('深圳');


SELECT
	id,
	url,
	city_name,
	gd_city,
	floor_name,
	issue_code,
	issue_date,
	issue_date_clean,
	room_sum
FROM
	temp_db.tmp_city_newest_deal
WHERE
	city_name IN ('徐州', '深圳', '哈尔滨')
	AND NOT isnull(issue_date)
	AND issue_date NOT IN ('')
LIMIT 10;


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
	temp_db.city_newest_deal_haerbin;
	
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
	temp_db.city_newest_deal_xuzhou;


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
		concat_ws('-', substr(issue_date, 1, 4), LPAD(substr(issue_date, 6, 2), 2, '0'), substr(issue_date, 9, 2)) t
	FROM
		temp_db.city_newest_deal_shenzhen)t;
		
	
	
	