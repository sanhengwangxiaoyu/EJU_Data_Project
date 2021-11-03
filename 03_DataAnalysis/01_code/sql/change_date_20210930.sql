-- 更新个别楼盘城市变化
--          1> dws_db_prd.dws_newest_popularity_rownumber_quarter       重跑   
--          2> dws_db_prd.dws_newest_investment_pop_rownumber_quarter   重跑
--          3> dws_db_prd.dws_customer_cre                              修改城市id或者删除
--          4> dws_db_prd.dws_customer_sum                              修改城市id或者删除
--          5> dws_db_prd.dws_customer_week                             修改城市id或者删除
--          6> dws_db_prd.dws_customer_month                            修改城市id或者删除

-- e62634b1ee0a2500cbfdd8f058a61984
-- 66e24a4649e66eb7cd18440e824f3065
-- fc17c7201ffa9a3fc31b1497acac8ae7


-- 9d144360311b4788b0f251460a37a6db
-- bbf24a2f1c03e1971355abc71e3a77b8
-- 95f3b729c646100b6bbe77ae8f460a34
-- a5fcd6ef147c185cc88831cac6a7bf5e
-- 3f8167cbf94d3776dc84ab311dbc4ab4
-- 9900ddf2a08cfd98655d22a9265cac80
-- eb9c1439c14d5dbd099951640e10d1bf
-- 38303a54251e2a1bb11a768d517d0ba3
-- bcbb6b663bcb3387edf3f0cb89b1b91e
-- ee4de28ab067da1e2f08c6ab9e1a5ecc

-- 110000
-- 310000
-- 360100
-- 610400
-- 330400
-- 330500
-- 320500

delete from dws_db_prd.dws_customer_cre where newest_id in ('e62634b1ee0a2500cbfdd8f058a61984','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7');
delete from dws_db_prd.dws_customer_sum where newest_id in ('e62634b1ee0a2500cbfdd8f058a61984','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7');
delete from dws_db_prd.dws_customer_week where newest_id in ('e62634b1ee0a2500cbfdd8f058a61984','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7');
delete from dws_db_prd.dws_customer_month where newest_id in ('e62634b1ee0a2500cbfdd8f058a61984','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7');

update dws_db_prd.dws_customer_cre a,(select city_id,newest_id from dws_db_prd.dws_newest_info where newest_id in ('9d144360311b4788b0f251460a37a6db','bbf24a2f1c03e1971355abc71e3a77b8','95f3b729c646100b6bbe77ae8f460a34','a5fcd6ef147c185cc88831cac6a7bf5e','3f8167cbf94d3776dc84ab311dbc4ab4','9900ddf2a08cfd98655d22a9265cac80','eb9c1439c14d5dbd099951640e10d1bf','38303a54251e2a1bb11a768d517d0ba3','bcbb6b663bcb3387edf3f0cb89b1b91e','ee4de28ab067da1e2f08c6ab9e1a5ecc')) b
set a.city_id = b.city_id where a.newest_id = b.newest_id;
update dws_db_prd.dws_customer_sum a,(select city_id,newest_id from dws_db_prd.dws_newest_info where newest_id in ('9d144360311b4788b0f251460a37a6db','bbf24a2f1c03e1971355abc71e3a77b8','95f3b729c646100b6bbe77ae8f460a34','a5fcd6ef147c185cc88831cac6a7bf5e','3f8167cbf94d3776dc84ab311dbc4ab4','9900ddf2a08cfd98655d22a9265cac80','eb9c1439c14d5dbd099951640e10d1bf','38303a54251e2a1bb11a768d517d0ba3','bcbb6b663bcb3387edf3f0cb89b1b91e','ee4de28ab067da1e2f08c6ab9e1a5ecc')) b
set a.city_id = b.city_id where a.newest_id = b.newest_id;
update dws_db_prd.dws_customer_week a,(select city_id,newest_id from dws_db_prd.dws_newest_info where newest_id in ('9d144360311b4788b0f251460a37a6db','bbf24a2f1c03e1971355abc71e3a77b8','95f3b729c646100b6bbe77ae8f460a34','a5fcd6ef147c185cc88831cac6a7bf5e','3f8167cbf94d3776dc84ab311dbc4ab4','9900ddf2a08cfd98655d22a9265cac80','eb9c1439c14d5dbd099951640e10d1bf','38303a54251e2a1bb11a768d517d0ba3','bcbb6b663bcb3387edf3f0cb89b1b91e','ee4de28ab067da1e2f08c6ab9e1a5ecc')) b
set a.city_id = b.city_id where a.newest_id = b.newest_id;
update dws_db_prd.dws_customer_month a,(select city_id,newest_id from dws_db_prd.dws_newest_info where newest_id in ('9d144360311b4788b0f251460a37a6db','bbf24a2f1c03e1971355abc71e3a77b8','95f3b729c646100b6bbe77ae8f460a34','a5fcd6ef147c185cc88831cac6a7bf5e','3f8167cbf94d3776dc84ab311dbc4ab4','9900ddf2a08cfd98655d22a9265cac80','eb9c1439c14d5dbd099951640e10d1bf','38303a54251e2a1bb11a768d517d0ba3','bcbb6b663bcb3387edf3f0cb89b1b91e','ee4de28ab067da1e2f08c6ab9e1a5ecc')) b
set a.city_id = b.city_id where a.newest_id = b.newest_id;



