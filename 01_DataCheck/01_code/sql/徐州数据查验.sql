-- 徐州数据查验
--  1. 新建表
--  2. 将数据加载进去后，检验数据合理性
--     1> 按照月份统计数据查看是否合理
--     2> 查看空值是否存在太多
--     3> 手动查看多个url验证套数，是否合理。ps:尤其是5000+的
--     4> 通过许可证日期、项目名称、城市名称、建筑号码查看是否存在太多重复值
use temp_db;

-- 检验数据
--   1>时间最大值和最小值
SELECT
	max(issue_date),
	min(issue_date),count(1)
FROM
	temp_db.city_newest_deal_xuzhou
WHERE
	NOT isnull(issue_date)
	AND issue_date NOT IN ('')
	AND issue_date LIKE '%0%'
	AND LENGTH(issue_date)<18 AND LENGTH(issue_date) > 8;


--   2>获取所有时间的值
SELECT issue_date FROM temp_db.city_newest_deal_xuzhou GROUP BY issue_date;


--   3>每年的数据条数
SELECT
	count(1),
	substr(issue_date, 1, 4)
FROM
	temp_db.city_newest_deal_xuzhou
GROUP BY
	substr(issue_date, 1, 4) ;-- 12416 2021 ，98628 2020，57998 2019


--   4>每年的套数
SELECT
	t.city_name,
	sum(t.room_sum) s,
	substr(t.issue_date, 1, 4)
FROM
	(SELECT DISTINCT issue_code, issue_date, room_sum, city_name FROM temp_db.city_newest_deal_xuzhou tcnd) t
GROUP BY
	t.city_name,
	substr(t.issue_date, 1, 4);  --   13237.0   2021，119426.0  2020 ，98689.0    2019                                                                        


--   5>空值数据
SELECT
	url,
	city_name,
	floor_name,
	issue_date
FROM
	temp_db.city_newest_deal_xuzhou
WHERE
	isnull(url)
	OR url IN ('')
	OR isnull(city_name)
	OR isnull(floor_name);


--   6>空值数量
SELECT
	count(1)
FROM
	temp_db.city_newest_deal_xuzhou
WHERE
	isnull(url)
	OR url IN ('')
	OR isnull(city_name)
	OR isnull(floor_name);


--   7>总套数
SELECT t.city_name, sum(t.room_sum) s FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.city_newest_deal_xuzhou tcnd) t GROUP BY t.city_name;


--   8>重复数据条数
SELECT 
     tt.c AS `去重后条数`,tcnd.c AS `总条数` ,tcnd.c-tt.c AS `重复数据`  
FROM 
  (SELECT count(1) c FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.city_newest_deal_xuzhou tcnd) t)tt
,
  (SELECT count(1) c FROM temp_db.city_newest_deal_xuzhou) tcnd;
 
--  9>每月预证数量
SELECT
	substr(issue_date, 1, 8),
	count(issue_code),
	count(DISTINCT issue_code)
FROM
	city_newest_deal_xuzhou
WHERE
	issue_date != ''
GROUP BY
	substr(issue_date, 1, 8);

--   10>每月的套数
SELECT
	t.city_name,
	sum(t.room_sum) s,
	substr(t.issue_date, 1, 8)
FROM
	(SELECT DISTINCT issue_code, issue_date, room_sum, city_name FROM temp_db.city_newest_deal_xuzhou tcnd) t
GROUP BY
	t.city_name,
	substr(t.issue_date, 1, 8);

--   11>检查字段长度
SELECT * FROM temp_db.city_newest_deal_xuzhou  WHERE room_code LIKE '%div%';







