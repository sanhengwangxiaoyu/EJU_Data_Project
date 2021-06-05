--一行拆多行
-- 拆分原始数据表ori_jiguang_personal_tag_i中 jdata字段，用来统计标签数量，但是后来发现有更好的方法
select 
    distinct a.*,substring_index(substring_index(a.jdata, "'}, {'", b.help_topic_id + 1), ',',- 1) NAME 
from origin_estate.ori_jiguang_personal_tag_i a 
  JOIN mysql.help_topic b 
    ON b.help_topic_id < ( LENGTH (a.jdata) - LENGTH(REPLACE(a.jdata, "'}, {'",''))+1) where a.period in (202012);