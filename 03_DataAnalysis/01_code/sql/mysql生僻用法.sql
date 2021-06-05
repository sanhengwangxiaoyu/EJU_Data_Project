--mysql生僻用法
-- 1.修改时间格式函数：date_format
-- 2.清空表 truncate
-- 3.解锁表 unlock tables
-- 4.mysql版本 show version()
-- 5.窗口函数 

--1
insert
	into
	city_newest_deal (url,issue_date)
select
    id,
	DATE_FORMAT(issue_date , '%Y-%m-%d %H:%i:%s')
from
	tmp_city_newest_deal tcnd where city_name in ('杭州');


--2
truncate table city_newest_deal ;


--3
UNLOCK TABLES;



--4
select version();



--5
SET @row_number=0, @customer_no=0;

SELECT
    @row_number:=CASE
        WHEN @customer_no = s.floor_name THEN @row_number + 1 
        ELSE 1
    END AS num,
    @customer_no:=s.floor_name AS floor_name,
    s.city_name ,  
    s.room_sum ,
    s.issue_date_clean 
FROM
    temp_db.tmp_city_newest_deal s
where city_name in ('杭州')
ORDER BY
    s.floor_name_clean asc;