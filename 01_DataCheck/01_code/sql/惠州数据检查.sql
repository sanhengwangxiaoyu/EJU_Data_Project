--惠州数据查验
--  1. 新建表
--  2. 将数据加载进去后，检验数据合理性
--     1> 按照月份统计数据查看是否合理
--     2> 查看空值是否存在太多
--     3> 手动查看多个url验证套数，是否合理。ps:尤其是5000+的
--     4> 通过许可证日期、项目名称、城市名称、建筑号码查看是否存在太多重复值
use temp_db;

-- 新建表  temp_db.city_newest_deal_huizhou
CREATE TABLE IF NOT EXISTS  `city_newest_deal_huizhou` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `url` text,
  `city_name` text,
  `floor_name` text,
  `address` text,
  `business` text,
  `issue_code` text,
  `issue_date` text,
  `open_date` text,
  `issue_area` text,
  `sale_state` text,
  `building_code` text,
  `room_sum` text,
  `area` text,
  `simulation_price` text,
  `sale_telephone` text,
  `sale_address` text,
  `room_code` text,
  `room_sale_area` text,
  `room_sale_state` text
  PRIMARY KEY (`id`)
)DEFAULT CHARSET=utf8mb4;


-- 检验数据
--   1>获取最大值和最小值的时间
SELECT max(issue_date),min(issue_date) FROM temp_db.city_newest_deal_huizhou WHERE issue_date_clean NOT IN ('');

--   2>每年的数据数量
SELECT count(1),substr(issue_date,1,4) FROM  temp_db.city_newest_deal_huizhou GROUP BY substr(issue_date,1,4) ;

--   3>每年，每个城市的套数
SELECT
	t.city_name,
	sum(t.room_sum) s,
	substr(t.issue_date, 1, 4)
FROM
	(
	SELECT
		DISTINCT issue_code,
		issue_date,
		room_sum,
		city_name
	FROM
		temp_db.city_newest_deal_huizhou tcnd) t
GROUP BY
	t.city_name,
	substr(t.issue_date, 1, 4);

--   4>空值数据
SELECT * FROM temp_db.city_newest_deal_huizhou WHERE isnull(url) OR isnull(city_name) OR isnull(floor_name) LIMIT 10;

--   5>空值数量
SELECT count(1) FROM temp_db.city_newest_deal_huizhou WHERE isnull(url) OR isnull(city_name) OR isnull(floor_name) LIMIT 10;

--   6>每个城市的总套数
SELECT t.city_name, sum(t.room_sum) s FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.city_newest_deal_huizhou tcnd) t GROUP BY t.city_name;

--    7>重复数据条数
SELECT count(1) FROM ( SELECT DISTINCT issue_code, room_sum, city_name FROM temp_db.city_newest_deal_huizhou tcnd) t

SELECT count(1) FROM temp_db.city_newest_deal_huizhou tcnd;