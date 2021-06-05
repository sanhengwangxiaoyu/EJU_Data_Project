--统计标签个数
--  1.成功典范
SELECT count(1) FROM (SELECT col1,col2,LENGTH(col3) - LENGTH(repalce(col3,'ga','g') len) FROM TABLE WHERE col1 IN ('202012'))a WHERE a.len>30;

--  2.失败案例
select 
    distinct a.*,substring_index(substring_index(a.jdata, "'}, {'", b.help_topic_id + 1), ',',- 1) NAME 
from origin_estate.ori_jiguang_personal_tag_i a 
  JOIN mysql.help_topic b 
    ON b.help_topic_id < ( LENGTH (a.jdata) - LENGTH(REPLACE(a.jdata, "'}, {'",''))+1) WHERE a.period IN (202012);