select a.city_id,a.county_id,a.period,a.for_sale,a.on_sale,a.sell_out
from dws_newest_county_qua a left join dws_newest_city_qua b on a.city_id = b.city_id
where b.city_id is NULL    -- 这样查询的结果是A表中有而B表中没有的数据
union all
select b.city_id,b.county_id,b.period,b.for_sale,b.on_sale,b.sell_out
from dws_newest_city_qua b left join dws_newest_county_qua a ON a.city_id = b.city_id
where a.city_id is NULL;   -- 这样查询的结果是B表中有而A表中没有的数据