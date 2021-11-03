-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 110000
-- 310000
-- 360100
-- 610400
-- 330400
-- 330500
-- 320500

select * from dwb_db.b_dwb_customer_imei_tag limit 10;

update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set dr = 1 where city_id like '110%' or city_id like '310%' or city_id like '3601%' or city_id like '6104%' or city_id like '3304%' or city_id like '3305%' or city_id like '3205%' ;

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where city_id like '110%' or city_id like '310%' or city_id like '3601%' or city_id like '6104%' or city_id like '3304%' or city_id like '3305%' or city_id like '3205%' ;

delete from dws_db_prd.dws_newest_popularity_rownumber_quarter where create_time = '2021-09-30 16:33:22.0'

show create table dws_db_prd.dws_newest_period_admit;
CREATE TABLE dws_db_prd.bak_20210930_dws_newest_period_admit (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `city_id` varchar(40) DEFAULT NULL,
  `period` varchar(40) DEFAULT NULL COMMENT '周期',
  `newest_id` varchar(80) DEFAULT NULL COMMENT '准入楼盘id',
  `dr` varchar(2) DEFAULT '0' COMMENT '作废标识(0,有效，1，作废）',
  `browse` varchar(5) DEFAULT NULL COMMENT '浏览页模板',
  `portrait` varchar(5) DEFAULT NULL COMMENT '画像报告模板',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`) USING BTREE COMMENT '楼盘id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '楼盘准入表---2021年9月30号备份';
insert into dws_db_prd.bak_20210930_dws_newest_period_admit select * from dws_db_prd.dws_newest_period_admit;

show create table dws_db_prd.dws_newest_popularity_rownumber_quarter;
CREATE TABLE temp_db.bak_20210930_dws_newest_popularity_rownumber_quarter (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` int(11) NOT NULL COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` decimal(10,2) DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period` varchar(10) DEFAULT NULL COMMENT '时间周期',
  `imei_c_avg` double DEFAULT NULL COMMENT '楼盘热度指数占比平均值',
  `index_rate_change` decimal(10,4) DEFAULT NULL COMMENT '楼盘热度指数占比与本市均值对比情况',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '数据更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识（1 无效  0 有效）',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=803300 DEFAULT CHARSET=utf8mb4 COMMENT='人气热度排行榜单表--20210930备份表';
insert into temp_db.bak_20210930_dws_newest_popularity_rownumber_quarter select * from dws_db_prd.dws_newest_popularity_rownumber_quarter;

show create table dws_db_prd.dws_newest_investment_pop_rownumber_quarter;
CREATE TABLE temp_db.bak_20210930_dws_newest_investment_pop_rownumber_quarter (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` int(11) NOT NULL COMMENT '城市/区域代码',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `imei_newest` bigint(20) DEFAULT NULL COMMENT '楼盘热度',
  `imei_city` double DEFAULT NULL COMMENT '城市/区域热度',
  `rate` double DEFAULT NULL COMMENT '楼盘热度占比',
  `imei_c_max` double DEFAULT NULL COMMENT '城市热度的最大值',
  `imei_c_min` double DEFAULT NULL COMMENT '城市热度的最大小值',
  `index_rate` decimal(10,2) DEFAULT NULL COMMENT '楼盘热度指数占比',
  `sort_id` double DEFAULT NULL COMMENT '楼盘热度指数占比排名',
  `period` varchar(10) DEFAULT NULL COMMENT '时间周期',
  `create_time` datetime DEFAULT NULL COMMENT '数据创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '数据更新时间',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识（1 无效  0 有效）',
  PRIMARY KEY (`id`),
  KEY `newest_id_IDX` (`newest_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=593707 DEFAULT CHARSET=utf8mb4 COMMENT='投资型人气热度排行榜单表--20210930备份表';
insert into temp_db.bak_20210930_dws_newest_investment_pop_rownumber_quarter select * from dws_db_prd.dws_newest_investment_pop_rownumber_quarter;

show create table dws_db_prd.dws_customer_cre;
CREATE TABLE temp_db.bak_20210930_dws_customer_cre (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留结果表--20210930备份表';
insert into temp_db.bak_20210930_dws_customer_cre select * from dws_db_prd.dws_customer_cre;

show create table dws_db_prd.dws_customer_sum; 
CREATE TABLE temp_db.bak_20210930_dws_customer_sum (
  `city_id` text COMMENT '城市id',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼盘id',
  `period` text COMMENT '季度周期',
  `cou_imei` bigint(20) DEFAULT NULL COMMENT '当前区域浏览人数',
  `city_avg` double DEFAULT NULL COMMENT '当前城市浏览人数平均值',
  `ratio` double DEFAULT NULL COMMENT '占比',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户总量结果表--20210930备份表';
insert into temp_db.bak_20210930_dws_customer_sum select * from dws_db_prd.dws_customer_sum; 

show create table dws_db_prd.dws_customer_week;  
CREATE TABLE temp_db.bak_20210930_dws_customer_week (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `week` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留周度结果表--20210930备份表';
insert into temp_db.bak_20210930_dws_customer_week select * from dws_db_prd.dws_customer_week; 

show create table dws_db_prd.dws_customer_month; 
CREATE TABLE temp_db.bak_20210930_dws_customer_month (
  `city_id` text,
  `newest_id` varchar(255) DEFAULT NULL,
  `month` text,
  `exist` text,
  `imei_num` bigint(20) DEFAULT NULL,
  `period` text,
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向客户增存留月度结果表--20210930备份表';
insert into temp_db.bak_20210930_dws_customer_month select * from  dws_db_prd.dws_customer_month;

select cityid ,city_name from dws_db_prd.dws_supply where period = '2020Q4' and dr = 0 group by cityid ,city_name; 

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------


select t1.newest_id,newest_name,city_id,address,t1.lng,t1.lat from 
  (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1
inner join 
  (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2
on t1.lng = t2.lng and t1.lat = t2.lat
where city_id in ('110000','310000');


select * from dws_db_prd.crawler_city_newest_lnglat_gd where newest_id in ('0e76cff9fa87ab76fe6357ff923d40f3','697bc5b1e5a67cf52e32066ba0746006','313d68df81e474e7f105c57a83b155ee','c569bd2ddb91944df07a006ef4f0c47f','0d5687562e00ac8143c57eab777dacf2','7fb91179bf1522c124c85b72f13d6280','5ee4bf37859304549a7e046fc5e4e5ad','b934abc4641183ee728a001a67c48732','0016bf6dd2a27c94f20be16d4eeb6248','16f190734e7ed4ef4e180e95703659b6','6fed0b58cbbea228f22c74e96a3770d4','61ddca7bed24b5c55ccbeec4554d41ea','7a968852273bf8d2cf1997d088c0f66e','210ccf264997cc7f96f7db79cca58b48','2db65dfc66641d1b93adaa74ff452822','8fa016bc14ba55f022b108ac8463469c','d0d7de2cffff57be1dc9426d8a8c6f25','3871dd8c82fafcb70278a5d84137999c','3bd230a8c7e4a57f2cc1ee6dce343e98','fcfcd28bc5c874bda38e3aa29cebfbe3','02463550a6e50cb955081691a15d749c','461ce5347c514575275021f0156afbff','67a5c862f4e0a3163f2b257c572c9e22','e63099e106036ea52b259002a14a45b3','16dcb2eb89e2d39360749480be7f6e5b','894fd8ff4c8d25e09818faf5e5c8441e','17e5783bf06750f0c4a1558bd633893a','858fdb3d8231de97cbbc61ee1053118d','66e24a4649e66eb7cd18440e824f3065','fc17c7201ffa9a3fc31b1497acac8ae7','1cdca67093ac534e3531ce89b5ec89a8','a9fedf8245ed3ef2b3f9d442326fba99','789071c5fef525d4cec3be71e168ee90','b5f508c14a40696a71a1634e2334606e','173e16e7eb7e3c1361584a3aa3d1132a','a5d9ba097999d3e2cc97776846682d45','ded81131fe748690d525e7dd975d8900','288aeef5eaa4061def7f1beed616ff2c','720e83a496f98a1540d2e8937b50a3ac','983305a4923cbdbaf38ab86d72465528','e5f2ec8b9a560714ad4c3e8246875f3b','246905541b4fa72da43a680facaf5545','6abb0e294ab29c7dd011e9a8dd653f0e','27dff39f56083ea6b0b1b99e058e1404','9a9504a4bf9c11eb86162cea7f6c2bde','1815ad9f1efda197754e890c3b7f9ffe','b7ad744009f5bc10cf16e5c877ed8a45','6a370b84bddf8b23183f87f79d5afe12','990ea0a756ca0180967066110aceae2e','304c542e194a4067cd4124dbdb8a556b','e93cd2659668c84577bcfcf61809985e','f0516ab203cfc748260843f1b97b8ba1','fae3093cb8c3309a9150b05123d96db7','807c31cebd524ec6f574c94c35f0668a','aa4bc787af22fc44a222611c45a1d899','cb38b430a38035b480fb3fc9a3b3df17','3ddb8aef481b0b24abb03282f9db6562','4fb5f518b86b6c7f200eb34061d2d077','ab1c9398525ea78ff74242c19b6f353a','f16c8fc569ac0574ac219eec3b0170dc','f411ff97e0dca75681747d9975a60ab6','186593b8c4de1147768436510a2327db','5836971ed227342159fd70b49e0f2297','d2833bc99c42f42a2e4fa097b28003ad','49b4cd24cb0fc229f46df674acfda3e1','8b9f9e0229173ca03bd42acdc0d9bb9a','59af7ef41c0c9feda5ce5e81406be586','9d009b2a363d17ebcb03eac972aefc57','ffa1b0e477cde3c87346f3812e06e7d7','1b9997a0ce002d7d7cf40018828aa3f1','71f0837c711956e5378da6a8f0eb5902','0ef3c320a8bc11ce70ac328eff9a1e77','fda9fb65bbdf0fa836ae27d8fa4e3a85','1041048202b230094be84bd30157ab8f','bae30fc3b1cf31cd1e085b8e6e673294','5ae832e484fd3ccb5cc22c935835f030','8be3361edc59388ea44ab6f2a9c078f4','e2185510202f47577833c847d0d27de1','ea4e76cbc6cacd742b9afb4a27956d8f','f56e0225c68a4dbc989ce56041534270','fc504e8f914ad5c6f9910c2617f108bb','5425ac0dee07c3dbc7449995fafe8f1d','b320ff24b43b6a4303a826cc963c5f80','5945c4498729ebb52f73406613f7176e','87b3ff14e4255961cb3a393f0c14196e','d28774372063e37514bc8bf9f8004ab6','f8540118fcdf8577c0ce595cd37381e8','5ba1244fa6b427659fdbda7d404b595a','8823b434139cdfe850164ba28a18d12a','5f5581e6b2b0a3e3d095abe3835690fc','dc6d45d56ea4aada4ec915223a815004','1fbeccae8633ccb006837193a86c4c89','852f66b76882ecda27bff40a82d232ac','be1f5a56b4eb0c2b88ac91eb9b1a0969','d898e7377ad33b85a8db8dbddec74dc8','f9a8d2ca033d0015fad69157c28ba43d','d35e00291994e2e18992944b3674f698','fd8fe87aba0fa2ad82b6a8184a4dfbe8','3a28f7abd91e401d794216f2c1d26e38','75f92e32cb07fa76fe0c6979552baf06','0426a39ad621bc4fbd12bfbf934b6b77','d9983e51fefa98a13876a708d2f546fd','f7868f77fce38acb1408914fb81f4a6d','fbbcc34295a65ad84f4057562178fcee','a8bebc2e7fd234a47d5cf6e0cc0e07a6','b8773866ce0af4d0db838f6492a4620f','13910d16a63e165940843c8d186c6184','ce5fa1ab1eff92288826503280b968e8','4e94965082275175dbb9732f21713066','52045a78bd53a2e9783159d6e5c5e2bc','cf506039eedc86e58d2410c9222d1aec','9f9394b13637483fe49f0ca74eddc24e','c9667d4333d8b69c91fa04d17139b9fd','91e23733eea7886a31fed432ec786aab','e48cf54a1fcd56511d0b5dea4e240bcc','45fbac86a99b2d89be2fd546a4394435','c5469d16b150bdd87dcd3003834f0b46','167b6660a0a9a38fd1560e53a9d930a9','ce0ada1e35861a76335a114c8f7eb52f','3918cd57b2795c4f5a0bfe95109e1a65','55c45bae53cc7730383fa974a0818ce3','561b0fd03a154df4cb4995908063b48c','07c69f02a8d31b719a60b5388bf228ca','fe6e658097788344277d66cb0836e93e','9d144360311b4788b0f251460a37a6db','bbf24a2f1c03e1971355abc71e3a77b8','95f3b729c646100b6bbe77ae8f460a34','a5fcd6ef147c185cc88831cac6a7bf5e','69fd2cb9d1a7992de3979f5aca4d54d3','8dc361a06dcd7d1e92a2314fac06b0e9','5ad3465b72227c9f391b75bb6223a073','a6cfd1ae72216b2f26c97e8ca97b9266','3f8167cbf94d3776dc84ab311dbc4ab4','9900ddf2a08cfd98655d22a9265cac80','dd11364ebc265bbc92d00d86375854fb','fe81b98e3f70f38db12dccb352b05ceb','5a5b9bc86a9174b0abbbcc8c3ea15167','8c6bc2c4a39b3ad48e162ce7abc6f85c','cd27da2967e34d977ddbdf5de2a0cd47','c735b265e9aee096531617175a7d0d64','eebeeec5205a145035b9589ce106fa4b','f0749f5a41b2369989dca779bff1fd54','f589d8a968d35f5d80a81a867ecb3d28','480adeb6dd1a0a2341e0f1725bf5d2cc','ad310a4e7e21feede9c992ad54795958','7a42a72d4b6c3091cf340890158472b1','d1b4c797211ffe5ca12890bfaf5104b5','2df652a54706662e163d406246a21421','484cd28468e01554557c52d6747f8e22','886a4039e8a1e83bb23894727d34865c','45a212e6739baa1ff9bc0ba7888cc52f','801e3c4be0eaeec778a91bce94cc74c6','811bcc15d93225d44bc767ba33cc733e','9a557262bf9c11eb86162cea7f6c2bde','9264c3cbc66fea10735b7f06047d177c','b5153d1d5f259daa0dc6f9d5a5faf5cc','c9294c659226946a11135e93105fd9ce','40b068b9da8e50f5025b0e74b8937f04','e603ce4daab8fd69e56cc32536cec5cb','243049829de773e2a2232412a6e804a8','8f4591d89a6b0f6304bc8fc8e9eac416','afa9588cf3c05db901a9c538f95ee66e','122d7199c5e2577b1aed9b65f01ee05f','b977dfdc9e6ed1fcd9f3d532cef6017d','9440dc692ee5943723b9f19282edf90b','db821759a07b0d985c250d5ac9491ebd','7eedd1b677b54eaf9a3a3190d164fa6d','cde734c2f4f057f0e9e3dee2121cba3f','f1e160ec2899c08f03f34494287f1be9','ff8824397dfbbb32757dd66f10737126','0a708b1ba4ea12ccd4303f0effb8557d','d212728a4ed288b7e63552ef01e5ba0f','06a72c4f7003968eac936136a690801a','6f17a9aa6aea8a3ebfda1c776c50bbac','38e9ce8bd730587d5d99a6e813e22cad','873fa36f4b2800def2f08927dc9f5523','02338414a182660fca7e88547f827c36','fb3e6912a48a2a76b81ba779e4527c79','1bbee948f25afc650aefc0dd85dfded3','fe434ab9eac2afb8205b39a1f453a817','137923d444face542509b093b315f17e','c731a7d944561439e405d1a43d5bcdb1','483bb5df48a2fd026c89c3fea4fbdebf','aa8ec0c47e30f61ec79fc9558acab918','35f4db2f2c7a093e3e777c5db39ea436','49655d92f4c1f1cd3d352a0083eaaf5b','3e01ff9a42fec36c4a5ccf0be970e895','95354efb6fa3af3a3fb51cc8ba49634b','e3577eabc68d510547970bbc4fe678ba','ee85a088ccb153b7271df51a47824a03','f74dcef9e8f8021a3345fa2ecd2e227e','548dae9a772b0a2d17b03d08042de772','cc166ab7b5d45687520d9a1acd197278','169fa9690e3c1c86b31736970e1a6d45','289360349f9a7cb1f255b51f02e6f44a','314d8b9db721b27e4d620ecca6da27ef','414315b8f0df993bfa7e3e041201414c','75db7c8318a9707064f8d2f6b62e94ad','7875ca727e6cf031fd112ecbdc220666','8a9f1746965a2fd040e0ac6d3df38fa9','a88fdd4628a366829b9a8e5ca9693394','bede07c2c1b58dfb0e5e8e6645c87099','cbcd181792b3a7906a3ef95435d292b7','de3ccfda6deead53016e335c70c10689','e4a87105743ab834c5b9db2a9296f00e','f931fc508df023baeeeb15a997e588a3','0fe00e049f42ed985d0296f7639bbc3b','42ddd2af2821f3e80ec9aadac78f5ee1','4a06386275c2e7829037cd00f0356f38','bc153f1448105579ae25fbbff4290439','5cb25f84e98662f51be293d88dace1b4','bc09fd4e9e01a018b7b5e5751b664493');



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select * from dws_db_prd.crawler_city_newest_lnglat_gd;

insert into dws_db_prd.crawler_city_newest_lnglat_gd(newest_id,newest_name,address,city_id,city_name,lng,lat,gd_city,gd_district,gd_lat,gd_lng,create_time,county_name)
values ('1e792e3c6a35b75b9b4612cb0577d56f','悦珑湾','金台大道69号','610300','宝鸡市','0','0','宝鸡市','金台区','34.359161','107.194552
',now(),'金台区'),('eb9c1439c14d5dbd099951640e10d1bf','新力铂园','龙虎山大道,近茶园街','360100','南昌市','0','0','南昌市','红谷滩区','28.63402','115.792473',now(),'青山湖区'),('38303a54251e2a1bb11a768d517d0ba3','福林海棠苑','秦都区文林路中段，能源学院对面','610400','咸阳市','0','0','咸阳市','渭城区','34.355242','108.71133',now(),'秦都区'),('bcbb6b663bcb3387edf3f0cb89b1b91e','力高御景天城','西站大街518号','360100','南昌市','0','0','南昌市','红谷滩区','28.62328','115.801787',now(),'青山湖区'),('415ebd8d9f381c6db99465698a430e57','牛山国际','鹿城牛山北路温州冶炼厂地块','330300','温州市','0','0','温州市','瓯海区','27.974649','120.650086',now(),'鹿城区'),('97911d39c588716f82af0579ace97714','碧桂园城央壹品','乌牛镇电力路与镇东路交叉口','330300','温州市','0','0','温州市','鹿城区','28.03169','120.79655',now(),'永嘉县'),('ee4de28ab067da1e2f08c6ab9e1a5ecc','绿腾新贵公馆','上海南路88号','360100','南昌市','0','0','南昌市','青云谱区','28.649875','115.93629',now(),'青山湖区'),('a8d982f46b12865231ae488715a23c4e','碧桂园星荟','云岩区中坝路74号','520100','贵阳市','0','0','贵阳市','花溪区','26.589821','106.67162',now(),'云岩区'),('e62634b1ee0a2500cbfdd8f058a61984','碧桂园龙城府','双龙航空港经济区核心区东侧500米贵龙大道与贵龙纵线交汇处','520100','贵阳市','0','0','黔西南布依族苗族自治州','龙里县','26.388853','106.729277',now(),'南明区'),('3eea3d6d19c9afe3e727f79d51dd60f8','海星中央首府','乌牛世纪名门南200米（新宅路南）','330300','温州市','0','0','温州市','乐清市','28.031686','120.794605',now(),'永嘉县');

insert into dws_db_prd.crawler_city_newest_lnglat_gd(newest_id,newest_name,address,city_id,city_name,lng,lat,gd_city,gd_district,gd_lat,gd_lng,create_time,county_name,change_clomn_index)
 values ('66e24a4649e66eb7cd18440e824f3065','合美-帝宝壹號','固安新中西街与迎宾大道交叉口向西200米路南侧查看地图','110000','北京市','116.438326','39.932222','廊坊市','固安县','39.43661243','116.2789583',now(),'东城区','3'),('fc17c7201ffa9a3fc31b1497acac8ae7','春辉时代','固安迎宾大道和新中西街交叉口查看地图','110000','北京市','116.438326','39.932222','廊坊市','固安县','39.43743981','116.2830423',now(),'东城区','3'),('9d144360311b4788b0f251460a37a6db','正黄翡翠合院','长安路正黄翡翠合院','310000','上海市','121.45312','31.247952','嘉兴市','海盐县','30.528428','120.944674',now(),'静安区','3'),('bbf24a2f1c03e1971355abc71e3a77b8','金成首品','长安路金成首品','310000','上海市','121.45312','31.247952','湖州市','吴兴区','30.869617','120.27832',now(),'静安区','3'),('95f3b729c646100b6bbe77ae8f460a34','优优中环','中环东路与长安路交叉口','310000','上海市','121.454012','31.242037','嘉兴市','南湖区','30.77093268','120.7796778',now(),'静安区','3'),('a5fcd6ef147c185cc88831cac6a7bf5e','招商雍雅苑','吴江长安路与芦荡路交叉口查看地图','310000','上海市','121.454012','31.242037','苏州市','吴江区','31.11326322','120.6627506',now(),'静安区','3'),('3f8167cbf94d3776dc84ab311dbc4ab4','天际云墅','区府路天际云墅','310000','上海市','121.473559','31.242696','湖州市','吴兴区','30.86499495','120.1902089',now(),'静安区','3'),('9900ddf2a08cfd98655d22a9265cac80','天际玖墅','吴兴吴兴大道与区府路交汇处向北500米查看地图','310000','上海市','121.473559','31.242696','湖州市','吴兴区','30.86403787','120.1884769',now(),'静安区','3');

select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and change_clomn_index is null or change_clomn_index = '';

select t1.*,t2.city_id,t2.region_id from 
  (select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and  change_clomn_index = '1') t1
 left join 
  (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name) t2 
on t1.gd_city = t2.city_name and t1.gd_district = t2.region_name;

select t1.*,t2.city_id,t2.region_id from
  (select newest_id,gd_city,gd_district,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where create_time = '2021-09-30 11:33:29' and change_clomn_index = '3') t1
 left join 
  (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name) t2 
on t1.gd_city = t2.city_name and t1.gd_district = t2.region_name;



select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography group by city_id,city_name,region_id,region_name ;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select newest_id,gd_lat,gd_lng from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10) = '2021-09-29' and city_name = gd_city and gd_lat is not null and gd_lat != '';

select t1.*,t2.city_name,t2.region_name,t3.gd_city,t3.gd_district from 
  (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10) = '2021-09-29' and city_name = gd_city and gd_lat is not null and gd_lat != '' group by newest_id)) t1
left join 
  (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.region_id 
left join 
  (select newest_id,gd_city,gd_district from dws_db_prd.crawler_city_newest_lnglat_gd where substr(create_time,1,10) = '2021-09-29' and city_name = gd_city and gd_lat is not null and gd_lat != '') t3
on t1.newest_id = t3.newest_id;

select t1.*,t2.city_name,t2.region_name from 
  (select newest_id ,city_id ,county_id from dws_db_prd.dws_newest_info group by newest_id ,city_id ,county_id) t1
left join 
  (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) t2
on t1.city_id = t2.city_id and t1.county_id = t2.region_id;

select t1.*,t2.city_name,t2.region_name from 
  (select newest_id ,city_id ,county_id from dws_db_prd.dws_newest_info where newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where county_name is null group by newest_id) group by newest_id ,city_id ,county_id) t1
left join 
  (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) t2
on t1.county_id = t2.region_id;

update dws_db_prd.crawler_city_newest_lnglat_gd a,(select t1.*,t2.city_name,t2.region_name from (select newest_id ,city_id ,county_id from dws_db_prd.dws_newest_info where newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where county_name is null group by newest_id) group by newest_id ,city_id ,county_id) t1 left join (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) t2 on t1.city_id = t2.city_id and t1.county_id = t2.region_id) b 
set a.county_name = b.region_name where a.newest_id = b.newest_id;

update dws_db_prd.crawler_city_newest_lnglat_gd a,(select t1.*,t2.city_name,t2.region_name from (select newest_id ,city_id ,county_id from dws_db_prd.dws_newest_info where newest_id in (select newest_id from dws_db_prd.crawler_city_newest_lnglat_gd where county_name is null group by newest_id) group by newest_id ,city_id ,county_id) t1 left join (select city_id,city_name,region_id,region_name from dws_db_prd.dim_geography where grade = 4 group by city_id,city_name,region_id,region_name) t2 on t1.county_id = t2.region_id) b 
set a.county_name = b.region_name where a.newest_id = b.newest_id;

truncate table dws_db_prd.dws_newest_info;
insert into dws_db_prd.dws_newest_info select * from dws_db_prd.bak_20210929_dws_newest_info; 


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address from (
  SELECT a.newest_id,a.newest_name,b.city_name,a.address 
    FROM dws_newest_info a 
  INNER JOIN dws_db_prd.dim_geography b 
    ON b.grade=3 AND b.city_id=a.city_id 
  WHERE a.newest_id IN (
      select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id 
          having max(period) in ('2020Q3','2020Q4','2021Q1','2021Q2'))) tt1 
left join (select newest_id from city_detail_baidu) tt2 
  on tt1.newest_id = tt2.newest_id 
where tt2.newest_id is null group by tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------


select t1.newest_id,newest_name,t1.city_id,address,t1.lng,t1.lat,b.city_name from 
  (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1
inner join 
  (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2
on t1.lng = t2.lng and t1.lat = t2.lat
left join (select newest_id from city_detail_baidu) tt2 
  on t1.newest_id = tt2.newest_id 
left join (select city_id,city_name from dws_db_prd.dim_geography group by city_id,city_name) b
on t1.city_id = b.city_id
where tt2.newest_id is null;

select t1.newest_id,newest_name,t1.city_id,address,t1.lng,t1.lat,b.city_name from (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1 inner join (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2 on t1.lng = t2.lng and t1.lat = t2.lat left join (select newest_id from city_detail_baidu) tt2 on t1.newest_id = tt2.newest_id left join (select city_id,city_name from dws_db_prd.dim_geography group by city_id,city_name) b on t1.city_id = b.city_id where tt2.newest_id is null;

select t1.newest_id,newest_name,city_id,address,t1.lng,t1.lat from 
  (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1
inner join 
  (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2
on t1.lng = t2.lng and t1.lat = t2.lat;

truncate table dws_db_prd.gd_lat_lng; 


select * from 
(	select tt1.newest_id,tt1.newest_name,tt1.city_id,tt1.address,tt1.lng,tt1.lat,tt2.gd_lng,tt2.gd_lat,b.city_name ,c.district from 
	(select t1.newest_id,newest_name,t1.city_id,address,t1.lng,t1.lat from 
		  (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1
		inner join 
		  (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2
		on t1.lng = t2.lng and t1.lat = t2.lat) tt1
	left join (select newest_id,gd_lng,gd_lat from dws_db_prd.gd_lat_lng ) tt2 
	  on tt1.newest_id = tt2.newest_id 
	left join (select city_id,city_name from dws_db_prd.dim_geography group by city_id,city_name) b
	on tt1.city_id = b.city_id
	where tt2.newest_id is not null) a
left join 
  (select newest_id,city,district from dws_db_prd.city_detail_baidu) c
on a.newest_id = c.newest_id order by lng,lat;

CREATE TABLE dws_db_prd.crawler_city_newest_lnglat_gd (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(400) DEFAULT NULL COMMENT '网址',
  `newest_name` varchar(400) NOT NULL COMMENT '城市名称',
  `address` varchar(800) DEFAULT NULL COMMENT '高德城市名称',
  `city_id` varchar(100) DEFAULT NULL COMMENT '原始项目名称',
  `city_name` varchar(100) DEFAULT NULL COMMENT '清洗过的新项目名称',
  `lng` varchar(100) DEFAULT NULL COMMENT '清洗过的项目名称（1）',
  `lat` varchar(100) DEFAULT NULL COMMENT '清洗过的项目名称（2）',
  `gd_city` varchar(100) DEFAULT NULL COMMENT '地址',
  `gd_district` varchar(100) DEFAULT NULL COMMENT '公司',
  `gd_lat` varchar(100) DEFAULT NULL COMMENT '许可证',
  `gd_lng` varchar(100) DEFAULT NULL COMMENT '发证日期',
  `create_time` text COMMENT '插入时间',
  PRIMARY KEY (`id`),
  KEY `idx_tmp_crawler_city_newest_lnglat_gd_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='爬虫城市最新城市-经纬度-高德';
truncate table dws_db_prd.crawler_city_newest_lnglat_gd;

insert into dws_db_prd.crawler_city_newest_lnglat_gd(
newest_id,newest_name,address,city_id,city_name,lng,lat,gd_city,gd_district,gd_lat,gd_lng,create_time)
select a.newest_id,newest_name,address,city_id,a.city_name,lng,lat,c.city gd_city,c.district gd_district,gd_lat,gd_lng,now() create_time from 
(select tt1.newest_id,tt1.newest_name,tt1.city_id,tt1.address,tt1.lng,tt1.lat,tt2.gd_lng,tt2.gd_lat,b.city_name from 
	(select t1.newest_id,newest_name,t1.city_id,address,t1.lng,t1.lat from 
		  (select newest_id,newest_name,city_id,address,lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id )) t1
		inner join 
		  (select lng,lat from dws_db_prd.dws_newest_info where dr = 0 and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id ) group by lng,lat having count(1)>1 ) t2
		on t1.lng = t2.lng and t1.lat = t2.lat) tt1
	left join (select newest_id,gd_lng,gd_lat from dws_db_prd.gd_lat_lng ) tt2 
	  on tt1.newest_id = tt2.newest_id 
	left join (select city_id,city_name from dws_db_prd.dim_geography group by city_id,city_name) b
	on tt1.city_id = b.city_id
	where tt2.newest_id is not null) a
left join 
  (select newest_id,city,district from dws_db_prd.city_detail_baidu) c
on a.newest_id = c.newest_id order by lng,lat;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------


show create table dws_db_prd.dws_newest_info ;
CREATE TABLE dws_db_prd.bak_20210929_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT NULL COMMENT '占地面积',
  `building_area` double DEFAULT NULL COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表---20210929备份';
insert into dws_db_prd.bak_20210929_dws_newest_info select * from dws_db_prd.dws_newest_info ;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select * from odsdb.city_newest_deal where city_name = '北京' and issue_date_clean < '2018-03-01' and issue_date_clean > '2018-01-01';

select * from temp_db.city_newest_deal_data_check where city_name = '北京' and substr(issue_date,1,6) = '2018/3' ;

update temp_db.city_newest_deal_data_check set city_name = '九江' where city_name IN (''); 

update temp_db.city_newest_deal_data_check set city_name = '扬州' where city_name IN (''); 

SHOW processlist;

delete from odsdb.city_newest_deal where city_name = '北京' and substr(insert_time,1,10) = '2021-09-28';

select case when replace(convert(replace(open_date ,'0:00:00','')using ascii),'?','') = '' then '9999/09/09' else replace(convert(replace(open_date ,'0:00:00','')using ascii),'?','') end from temp_db.city_newest_deal_data_check where city_name = '北京' and floor_name is not null and floor_name != '' and open_date like '%2021/8%';

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

SELECT a.newest_id,a.newest_name,b.city_name,a.address FROM dws_newest_info a 
INNER JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id 
WHERE a.newest_id IN (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id having max(period) = '2018Q2')  
  and a.newest_id not in (select newest_id from city_detail_baidu )
group by a.newest_id,a.newest_name,b.city_name,a.address ;

select tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address from (SELECT a.newest_id,a.newest_name,b.city_name,a.address FROM dws_newest_info a INNER JOIN dws_db_prd.dim_geography b ON b.grade=3 AND b.city_id=a.city_id WHERE a.newest_id IN (select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id having max(period) = '2018Q4')) tt1 left join (select newest_id from city_detail_baidu) tt2 on tt1.newest_id = tt2.newest_id where tt2.newest_id is null group by tt1.newest_id,tt1.newest_name,tt1.city_name,tt1.address;

select * from dws_newest_info limit 10;


select * from 
  (select newest_id,city_id from temp_db.newest_city_fialed_info) t1
left join 
  (select uuid,city_id,platform,url,issue_number from odsdb.ori_newest_info_base) t2
on t1.newest_id = t2.uuid and t1.city_id = t2.city_id where t2.uuid is not null and platform = '贝壳';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select newest_name,city_id,issue_number from odsdb.ori_newest_info_base limit 10;

select newest_name,city_id from dws_db_prd.dws_newest_info limit 10;

select * from dws_db_prd.dws_newest_period_admit limit 10;

show create table temp_db.newest_city_fialed_info ;
CREATE TABLE temp_db.newest_city_fialed_info (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(33) DEFAULT NULL COMMENT '楼盘id',
  `newest_name` varchar(80) DEFAULT NULL COMMENT '楼盘名称',
  `old_city_id` varchar(20) DEFAULT NULL COMMENT '历史城市id',
  `new_city_id` varchar(20) DEFAULT NULL COMMENT '新城市id',
  PRIMARY KEY (`id`),
  KEY `idx_newest_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='楼盘所在城市错误明细';


select newest_id from dws_db_prd.dws_newest_period_admit where dr = 0 group by newest_id having max(period) = '2018Q1' ;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select * from 
  (select newest_id ,imei from temp_db.tmp_newest_customer_info group by newest_id,imei) t1
inner join 
  (select newest_id ,substr(imei,1,14) imei from temp_db.jike_mingxi group by newest_id,imei) t2
on t1.newest_id = t2.newest_id and t1.imei = t2.imei;


select * from 
  (select newest_id ,imei from temp_db.tmp_newest_customer_info group by newest_id,imei) t1
left join 
  (select newest_id ,substr(imei,1,14) imei from temp_db.jike_mingxi group by newest_id,imei) t2
on t1.newest_id = t2.newest_id and t1.imei = t2.imei where t2.newest_id is null; -- 88,785

select newest_id ,`yiju-name` from temp_db.jike_mingxi group by newest_id,`yiju-name` ;

select newest_id from temp_db.jike_mingxi group by newest_id;

select customer,floor_name from odsdb.cust_browse_log_202107 where floor_name in ('中交白兰春晓','中基文博府','中铁琉森水岸','云镜天澄','保利-和光尘樾','保利领秀前城','光明当代拾光里','凤汇壹品居','华润置地御华府','唐山碧桂园凤凰星宸','大东海-晋棠府','广州增城万科城','新力翡翠湾','新城明昱东方','深国际万科和颂轩','滨江府1913','碧桂园-松湖明珠','金地藝墅家-酩悦','金科集美嘉悦','锦绣海湾城','雅居乐北城雅郡','颐璟名庭','龙湖-春江天越','龙湖上城','龙湖天璞') union all select customer,floor_name from odsdb.cust_browse_log_202108 where floor_name in ('中交白兰春晓','中基文博府','中铁琉森水岸','云镜天澄','保利-和光尘樾','保利领秀前城','光明当代拾光里','凤汇壹品居','华润置地御华府','唐山碧桂园凤凰星宸','大东海-晋棠府','广州增城万科城','新力翡翠湾','新城明昱东方','深国际万科和颂轩','滨江府1913','碧桂园-松湖明珠','金地藝墅家-酩悦','金科集美嘉悦','锦绣海湾城','雅居乐北城雅郡','颐璟名庭','龙湖-春江天越','龙湖上城','龙湖天璞') union all select customer,floor_name from odsdb.cust_browse_log_202109 where floor_name in ('中交白兰春晓','中基文博府','中铁琉森水岸','云镜天澄','保利-和光尘樾','保利领秀前城','光明当代拾光里','凤汇壹品居','华润置地御华府','唐山碧桂园凤凰星宸','大东海-晋棠府','广州增城万科城','新力翡翠湾','新城明昱东方','深国际万科和颂轩','滨江府1913','碧桂园-松湖明珠','金地藝墅家-酩悦','金科集美嘉悦','锦绣海湾城','雅居乐北城雅郡','颐璟名庭','龙湖-春江天越','龙湖上城','龙湖天璞') ;

select newest_id ,imei from temp_db.tmp_newest_customer_info group by newest_id,imei;

select alias_name,newest_id from dws_db_prd.dws_newest_alias where newest_id in ('83f23fd918398971c7295b3c07ed1967','7eb9ef20aea0b19c97fe839420e4350d','73d65da4561d6fcf738af55ff3c21467','495f6fbb3f35bdd14c7d4efb4022004a','8417be8224c97a16ce4935d8fc95a0e5','e86a44841446dd9a2edc06a6b22bd924','50976be57f9a8c5058813d3a6d34b01d','c10d68db25053e2e0bc0746e2ecfc2f6','05ed67003a563f6df6188423dddf9a64','08612952d30126bb87cf50554cf8cf60','7bef6dfa7fca742e5622dc2b25470300','783d390239928e4bbdd352004f8587dd','9aa57c09bf9c11eb86162cea7f6c2bde','1d427991cff480ac5b8b54c17b5b6d08','459f83383975b96704dea6e5a632b769','9ae729fcbf9c11eb86162cea7f6c2bde','9b8bb916bf9c11eb86162cea7f6c2bde','2bff59fe941afe48e1298945f0be3e88','5be89fa35ffcc06a267ce0da6fc38789','b493054673950d870d65ce6b62e11b69','9c053f0bbf9c11eb86162cea7f6c2bde','a28b6653c400c1f8509764079d1349d0','9a6ab320bf9c11eb86162cea7f6c2bde','a02255ccee0910d630369043f1a8ec5a','9ada1d3dbf9c11eb86162cea7f6c2bde');

select alias_name,newest_id from dws_db_prd.dws_newest_alias where newest_name in ('中交白兰春晓','中基文博府','中铁琉森水岸','云镜天澄','保利-和光尘樾','保利领秀前城','光明当代拾光里','凤汇壹品居','华润置地御华府','唐山碧桂园凤凰星宸','大东海-晋棠府','广州增城万科城','新力翡翠湾','新城明昱东方','深国际万科和颂轩','滨江府1913','碧桂园-松湖明珠','金地藝墅家-酩悦','金科集美嘉悦','锦绣海湾城','雅居乐北城雅郡','颐璟名庭','龙湖-春江天越','龙湖上城','龙湖天璞');

delete from temp_db.tmp_newest_customer_info where period = '2021Q3';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
show create table dwb_db.dwb_newest_customer_info;
CREATE TABLE temp_db.tmp_newest_customer_info (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(33) DEFAULT NULL COMMENT '楼盘id',
  `imei` varchar(8) DEFAULT NULL COMMENT 'imei唯一标识',
  `period` varchar(8) DEFAULT NULL COMMENT '周期',
  PRIMARY KEY (`id`),
  KEY `idx_newest_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='个别楼盘imei明细';
insert into temp_db.tmp_newest_customer_info (newest_id,imei,period) valus ('e86a44841446dd9a2edc06a6b22bd924','86600804538910','2021Q2');


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select newest_name from dws_db_prd.dws_newest_info where city_id = '310000' and newest_name in ('碧桂园十里芳华','花源湾','恒大海上威尼斯','绿地海湾','复地雅园公馆','华园','崇明岛大爱城','上海湾','杭州湾融创文旅城','长泰海滨城','碧桂园星著','新华联滨江雅苑','碧桂园枫景尚院','湖滨天地','东苑新天地公寓','大宁揽翠艺墅','苏河公园','湖光里水秀庭院','韵湖国际','浦西玫瑰园','建滔朗峰裕花园','宏图国际花园','廊桥公馆','滨江裕花园','海伦堡云璟台','建滔裕园','誉景澜庭','龙光花溪澜园','雅居乐聆湖雅苑','万科鑫苑SkyPark云璞','花溪公馆','湖境天著','诺丁公馆','倚云水岸苑','太仓珑悦兰庭','景秀江南','香缇雅苑','翡翠观澜','云澜天境','海伦堡溪璟园','云樾天境','金地翡翠名苑','碧桂园星著','建发天镜湾','合生晶萃','复游文旅城','臻之美舍','悦映澜庭','明月辰光','首开紫宸江湾','御府景苑','合生杭州湾','祥源漫城','柳岸荷风小区','新城吾悦华府','世外旭辉城','碧桂园城投西江月','诚通江南里','望竹山','悠隐南山','海王康山壹号','中天熙和诚品','碧桂园花溪源著','万科翡翠天御府','碧桂园嘉南首府','元宝湾','蓝城春风江南','龙光西塘江南大境','龙光玖台花苑','玖宸佳苑','龙光玖云华庭','雅居乐悦景庄春风渡','乌镇璟园','鸿翔天宸府','魏武宸章','树兰健康谷','新黄浦四象府','蓝城春风十里','荷玛诗湾','首创悦都','金凤城','湖庭别墅','象屿都城怡园','宝龙旭辉城','万科翡翠雅宾利','新湖青蓝国际','凯德茂名公馆','虹桥悦澜','乐颐小镇','乐颐小镇','上海湾','万科常春藤','东鼎名人府邸','湖畔首府','万联花园','翡翠公馆二期','新城悦隽','中交•阳羡美庐','新西塘孔雀城','云谷周庄','万和四季','海云间','绿地海湾','碧桂园海上传奇','碧桂园海上传奇','长泰海滨城','海伦堡氿月湾','苏河融景','海伦堡云璟台','睿怡庭','首创禧悦棠礼花园','碧桂园十里芳华','太仓恒大文化旅游城','阳光城中南翡丽云邸','春风南岸','海伦堡•星悦','聚福苑天玺','碧桂园•黄金时代','碧桂园•十里春风','世外旭辉城','爱家•曦霖樾','碧桂园•狮山源著','吾悦广场时代云境','鑫远•太湖健康城','鑫远•太湖健康城','金水湾','泛华东福城','桂语江南苑','华美公馆','春江天越','蓝城春风桃源里','荷园','杭州湾融创文旅城','碧桂园•星辰苑','海伦堡观澜','中天熙景诚品','碧桂园•碧浔府','港中旅•和乐小镇','港中旅•和乐小镇','云谷周庄','大众湖滨花园','杭州湾融创文旅城','新华联滨江雅苑','大理大华•锦绣华城','万科常春藤','碧桂园枫景尚院','亚太广场二期','碧桂园中南海上传奇','绿地海湾','湖庭庄园','碧桂园•淀湖花园','象屿都城','万科Mixtown','碧桂园・星澜','海伦堡•云璟台','万科海上传奇','富力湾','云谷周庄三期','棕榈湾','睿怡庭','凯德•都会新峰','恒海国际花园','首创禧悦棠礼花园','金大元御珑宫廷','纳帕溪醍','摩卡小镇三期','淀山湖壹号','碧桂园科技城项目','东鼎名人府邸','新西塘孔雀城','大众湖滨花园三期','新希望锦棠里','融创文旅城','海盐吾悦广场','新西塘孔雀城','融创文旅城','蓝城春风桃源','春江天越','柳岸春风','五芳斋芳华苑','海云间','朗诗绿色街区','圣特丽墅','西郊半岛名苑','玉兰公馆','华园','崇明岛紫竹园','万科mixtown','长泰淀湖观园','新西塘孔雀城','电建地产•泷悦蓝湾','碧桂园枫景尚院','碧桂园星辰苑','碧桂园云栖里','新希望锦棠里','汇景幸福里','碧桂园熙悦豪庭','越洋国际','绿地未来中心','虹桥大厦','琥珀公馆','奥克斯朗庭','铂金华府','云谷周庄','棕榈湾','富力湾','爱家华城柏景湾','月亮湾源墅','宏润花园','大发融悦四季','公元壹号','苏河融景','万科翡翠雅宾利','华侨城苏河湾(公寓)','新湖青蓝国际','复地雅园公馆','凯德茂名公馆','华园','湖滨天地','花源湾','虹桥悦澜','湖庭庄园','电建地产•泷悦蓝湾','电建地产•泷悦蓝湾','香逸铂悦','碧桂园十里芳华','象屿•西郊御府','象屿都城怡园','万科Mixtown','昆山玉兰公馆','嘉宝梦之悦','上海湾','上海湾','万科常春藤','香逸尚城','君悦豪庭','云谷周庄','鹿鸣九里','水月源岸','睿怡庭','建滔裕园','朗绿花园','象屿都城嘉园','德信都绘大境','首创禧悦棠礼花园','保利正荣•堂悦花园','棕榈湾花园(昆山)','碧桂园十里芳华','昆山玉兰公馆','万科未来之城','新力澄湖壹号院','新力澄湖壹号院','云谷周庄','千灯裕花园','水月源岸','花溪公馆','弘阳甪源','天润尚院','泰禾金尊府','中骏云景台','国瑞熙墅','合景云溪四季庭','合景泰富未名园映月台','普禧观澜','合生伴海','韵湖豪庭','海上时光','新沪紫郡','太仓熙岸原著','依云悦府','万和四季','象屿公园华府','都会之光花苑','太仓漫悦兰庭','太仓恒大文化旅游城','泱著花苑','阳光城中南翡丽云邸','越秀•向东岛','伴湖雅苑','锦著天逸花园','融侨悦江南','泱誉花苑','太仓悦园','建发天镜湾','海上风华花园','泊景庭','新华联滨江雅苑','星悦府','景瑞悦庭','电建地产•明悦苑','吉宝旭辉熹阅','大发熙悦澜庭','金科旭辉悦章','中南春江云锦','五洋橄榄岛花苑','春风南岸','御江南','太仓珑悦天境','万科翡翠铂樾','太仓佳源都市','招商五洋天镜华府','太仓依云四季苑','合景悦湖四季','恒大海上威尼斯','崇明岛大爱城','长泰海滨城','长泰海滨城','荣和园','复旦澜博湾','乐颐小镇','乐颐小镇','绿地海湾','余姚阳光城翡丽湾','中交•阳羡美庐','星澜府','余姚中南漫悦湾','卓越蔚蓝海岸','佳源都市','蓝光雍锦湾','绿地海湾','碧桂园海上传奇','碧桂园海上传奇','石榴•玉兰湾','石榴•太湖院子','江南桃源','融创霅溪桃源','首创•禧瑞太湖','绿雅原乡','绿雅原乡','湾上前璟','新南浔•孔雀城&水秀悦府','海伦堡•星悦','海伦堡•海伦湾','诚通•公园里','蓝光雍锦湾','富力绿地•西湖观邸','富力绿地•西湖观邸','富力•御西湖','富力•御西湖','合溪庄园','绿城•太湖明月','悦湖名城','铂悦府','聚福苑天玺','碧桂园城市之光','碧桂园•十里春风','碧桂园•黄金时代','南浔公馆','当代上品学府','中旅•锦绣东方国风小镇','中旅•宁波海泉湾','新宏水岸蓝庭','新宏水岸蓝庭','绿城•晓荷江南','中南滨海壹号','祥生•臻院','天际云墅','正大蓝城•春风蓝湾','光明梦想城','绿城观云小镇','长和庄园','爱家•曦霖樾','融创金成•湖山赋','城邦紫荆苑','骏宏•龙湾府','骏宏•龙湾府','金澜香溪','金澜香溪','富力城•御西湖','华纺湖誉府','富力城•御西湖','黄浦湾•玉象府','绿地尚湾','恒大•林溪竹语','碧桂园•狮山源著','吾悦广场时代云境','海王康山壹号','金成首品','新南浔孔雀城•都会江南','学仕嘉园','融创杭州湾壹号','鑫远•太湖健康城','鑫远•太湖健康城','伟业观塘壹号','光明•御品','奥园悦见山','奥园悦见山','惠建未来峯','海伦堡•海伦湾','黄浦湾•玉象府','同建大诚首府','同建大诚首府','合生国际名都','金成壹品','铂悦府','惠丰•悦君府','柳岸荷风小区','中南滨海壹号','金成•滨湖上境','融创杭州湾壹号','金凤城','旭辉公元城市','金科集美嘉悦','吉翔•欣隆府','当代和山悦MOMA','中南漫悦湾','聚米喆园','新城金樾','东鼎名人府邸','合景天峻','湖畔首府','运河大公馆','融创海越府','荣安•海上明月','万联花园','中都华庭','翡翠公馆二期','龙湖•春江郦城','阳光城翡丽湾','正黄翡翠合院','世茂璀璨时代','龙湖卓越紫宸','悦隽半岛','新城悦隽','山水庭院','萃湖上郡','新西塘孔雀城','荣盛祥云府','亲爱的洋房','杭州湾融创文旅城','融创海越府','柳岸春风景苑','祥符荡•泊樾','嘉兴恒大御景湾','龙润壹城','锦城豪庭','恒顺•澜山悦','东方禾苑','绿嘉兰园','欣隆盛世半岛','万科平澜玖著','碧桂园山湖源著','福晟天地','碧桂园天凝源著','海云间','中南正荣海上明悦','中朝桃源水岸','碧桂园蔚蓝','绿城乌镇蘭园','海伦堡氿月湾','合景尚峰','云泽嘉苑','江南御苑','金科集美嘉禾','恒大滨海御府','名悦华庭','金水湾','滨海之星','世茂•璀璨天悦','美好锦棠府','桃源香墅','万科翡翠四季','东亚沪西香颂','祥生国玥公馆','龙湖天宸原著','世合理想大地静安里','鸿翔•悦澜湾','江南国际城','绿地御景潮源','博锦苑','中南闻荷府','荣盛锦绣外滩','海樾风华','东亚左岸嘉园','江南春晓','新西塘孔雀城9•','新浦西孔雀城云樾东方苑','新浦西孔雀城云樾东方苑','石榴清水湾','奥园玺悦府','龙光玖悦华府','中骏•悦景府','金地云栖湾','泛华东福城','碧桂园枫景尚院','大诚•金廷府','桂语江南苑','华美公馆','新西塘孔雀城9•','春江天越','蓝城春风桃源里','龙光西塘江南大境','西江月','金地碧桂园风华四海','大众嘉苑','金科嘉悦名都','鸿翔•东辰名邸','梅里印象','荷园','中南九龙澜邸','大发德商熙悦花园','恒大都汇华庭','海盐恒大滨河左岸','杭州湾融创文旅城','弘阳•昕悦棠','湖畔景园','中南新悦府','华地观澜别院','润泽华庭','春风里','旭辉招商嘉樾府','海宁公园道一号','玺樾西塘','祥生悦澜湾','桃李春风景苑','旭辉梦想城','金洲海尚','佳源优优华府','乍浦锦园','金色江湾','万科中环公园','金地都会艺境(嘉兴)','优优中环','融创江南悦','洛塘公馆','中天•锦绣诚品','海伦堡观澜','中天熙景诚品','新大西塘里','荣安万科香樟国际','花语江南','雅居乐金茂中央公园','祥符荡•泊樾','麟湖公元壹号','交投祥生白鹤郡','嘉善-星罗城','卓越珑府','小城春秋','大众湖滨花园','久强幸福湾','鼎昌名邸','润泽嘉园','凤凰新城','招商新城雍景湾','观海华苑','山水壹号','澜府','如意佳园','百盛园','雍锦园','九龙溪畔','碧桂园•林肯公园','雍锦澜湾','香悦四季','星奕名筑','芝溪阁','玉兰臻园','镜湖宸院','镜湖宸院','湖语尚院','湖语尚院','中科澜庭','林溪源筑','融锦别院','常熟金科集美','碧桂园•碧浔府','南浔•贝尚湾','绿城龙王溪小镇','树兰健康谷','港中旅?和乐小镇','港中旅?和乐小镇','安吉•石榴玉兰湾','绿城柳岸晓风','安吉清华园','安吉清华园','安吉•缓山嘉院','新黄浦四象府','绿城柳岸晓风','电建地产•泷悦蓝湾','越秀向东岛','新西塘孔雀城天樾府','新华联滨江雅苑','龙光西塘江南大境','新南浔孔雀城都会江南','万科•泊樾湾','浦西玫瑰园','太仓恒大滨江悦府','海盐恒大都汇华庭','杭州湾融创文旅城','万科魅力花园','海盐恒大滨河左岸','万科常春藤','碧桂园蔚蓝','SkyPark雲璞','雅居乐聆湖雅苑','碧桂园枫景尚院','花桥象屿都城','宜兴绿地四季印象','玉澜花园','亚太广场二期');

select newest_id,period,intention from dwb_db.dwb_newest_customer_info where 
period in ('2021Q1','2021Q2') and
newest_id in ('83f23fd918398971c7295b3c07ed1967','7eb9ef20aea0b19c97fe839420e4350d','73d65da4561d6fcf738af55ff3c21467','495f6fbb3f35bdd14c7d4efb4022004a','8417be8224c97a16ce4935d8fc95a0e5','e86a44841446dd9a2edc06a6b22bd924','50976be57f9a8c5058813d3a6d34b01d','c10d68db25053e2e0bc0746e2ecfc2f6','05ed67003a563f6df6188423dddf9a64','08612952d30126bb87cf50554cf8cf60','7bef6dfa7fca742e5622dc2b25470300','783d390239928e4bbdd352004f8587dd','9aa57c09bf9c11eb86162cea7f6c2bde','1d427991cff480ac5b8b54c17b5b6d08','459f83383975b96704dea6e5a632b769','9ae729fcbf9c11eb86162cea7f6c2bde','9b8bb916bf9c11eb86162cea7f6c2bde','2bff59fe941afe48e1298945f0be3e88','5be89fa35ffcc06a267ce0da6fc38789','b493054673950d870d65ce6b62e11b69','9c053f0bbf9c11eb86162cea7f6c2bde','a28b6653c400c1f8509764079d1349d0','9a6ab320bf9c11eb86162cea7f6c2bde','a02255ccee0910d630369043f1a8ec5a','9ada1d3dbf9c11eb86162cea7f6c2bde');

select a.city_id,a.county_id,b.city_name,b.region_name county_name,issue_month period,issue_quarter,issue_room from 
  (select sum(issue_room) issue_room,issue_quarter,issue_month,city_id,county_id from 
    (select newest_id,issue_date,issue_month,issue_quarter,issue_room from dwb_db.dwb_newest_issue_offer where dr != 1 and newest_id is not null) t1
  left join 
    (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) t2 
  on t1.newest_id = t2.newest_id group by issue_quarter,issue_month,city_id,county_id) a
left join 
  (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b
on a.city_id = b.city_id and a.county_id = b.region_id
union all
select a.city_id,a.county_id,b.city_name,b.region_name county_name,issue_quarter period,issue_quarter,issue_room from 
  (select sum(issue_room) issue_room,issue_quarter,city_id,county_id from 
    (select newest_id,issue_date,issue_month,issue_quarter,issue_room from dwb_db.dwb_newest_issue_offer where dr != 1 and newest_id is not null) t1
  left join 
    (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) t2 
  on t1.newest_id = t2.newest_id group by issue_quarter,city_id,county_id) a
left join 
  (select city_id,city_name,region_id,region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name,region_id,region_name) b
on a.city_id = b.city_id and a.county_id = b.region_id
union all
select a.city_id,a.county_id,b.city_name,b.region_name county_name,issue_quarter period,issue_quarter,issue_room from 
  (select sum(issue_room) issue_room,issue_quarter,city_id,city_id county_id from 
    (select newest_id,issue_date,issue_month,issue_quarter,issue_room from dwb_db.dwb_newest_issue_offer where dr != 1 and newest_id is not null) t1
  left join 
    (select newest_id,city_id,county_id from dws_db_prd.dws_newest_info group by newest_id,city_id,county_id) t2 
  on t1.newest_id = t2.newest_id group by issue_quarter,city_id) a
left join 
  (select city_id,city_name,city_id region_id,city_name region_name from dwb_db.dwb_dim_geography_55city group by city_id,city_name) b
on a.city_id = b.city_id;





select * from
	(select t1.newest_id,t2.period from 
	  (select newest_id from dws_db_prd.dws_newest_info where city_id in (select city_id from  dwb_db.dwb_dim_geography_55city where city_name = '三亚市' group by city_id) and newest_id in (select newest_id from dwb_db.dwb_newest_issue_offer where city='三亚市' group by newest_id)) t1
	inner join 
	  (select newest_id,period from dws_db_prd.dws_newest_period_admit group by newest_id,period) t2
  on t1.newest_id = t2.newest_id where t2.period = '2021Q2') tt1
right join
  (select newest_id,issue_quarter from dwb_db.dwb_newest_issue_offer where dr != 1 and issue_quarter = '2021Q2' group by newest_id,issue_quarter) tt2
on tt1.newest_id = tt2.newest_id and tt1.period = tt2.issue_quarter order by tt1.period = '2021Q2' ;

select newest_id from dwb_db.dwb_newest_issue_offer where dr != 1 and issue_quarter = '2018Q2' group by newest_id;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------



update dwb_db.dwb_newest_issue_offer set alias_name = null where alias_name = '';

update dwb_db.dwb_newest_issue_offer set dr = 1;

insert into dwb_db.dwb_newest_issue_offer(newest_id,newest_name,address,developer,formal_newest_name,alias_name,city,issue_code,issue_date,issue_month,issue_quarter,issue_room,issue_area,dr,create_time,update_time,newest_url)
select newest_id,newest_name,address,developer,formal_newest_name,alias_name,city,issue_code,issue_date,issue_month,issue_quarter,issue_room,issue_area,0,create_time,update_time,newest_url from dwb_db.dwb_newest_issue_offer group by newest_id,newest_name,address,developer,formal_newest_name,alias_name,city,issue_code,issue_date,issue_month,issue_quarter,issue_room,issue_area,dr,create_time,update_time,newest_url;

delete from dwb_db.dwb_newest_issue_offer where dr = 1;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 重跑区域的供应套数 2020Q4,2021Q1,2021Q2三个季度的数据
-- 总人数更新到城市供应套数里
-- 按照代码执行，找到没有中山东莞供应套数的问题
-- 补充中山和东莞历史数据的供应套数

update dwb_db.dwb_issue_supply_city set dr = 1 where  period in ('2020Q4','2021Q1','2021Q2');
insert into dwb_db.dwb_issue_supply_city(city_id,city_name,period,supply_num,dr,create_time,update_time,cric_supply_num,num_index) select city_id,city_name,period,value,0,now(),now(),cric_value,null from dws_db_prd.dws_supply where period in ('2020Q4','2021Q1','2021Q2') and city_name = county_name and dr = 0;
select * from dwb_db.dwb_issue_supply_city;


update dwb_db.dwb_issue_supply_county set dr = 1 where period in ('2020Q4','2021Q1','2021Q2'); -- 1234
update dwb_db.dwb_issue_supply_county set dr = 1 where quarter in ('2020Q4','2021Q1','2021Q2');



update dws_db_prd.dws_supply set dr = 1 where  period in ('2020Q4','2021Q1','2021Q2');

update dws_db_prd.dwb_issue_supply_county set dr = 1 where period in ('2020Q4','2021Q1','2021Q2');

update dws_db_prd.dws_supply set dr = 1 where period in ('2020Q4','2021Q1','2021Q2');

insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '2' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_county where period = '"+date_quarter+"' and dr = 0 union all select city_name, city_name county_name, city_id city_id, period, sum(supply_value) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_county  where period = '"+date_quarter+"' and dr = 0  group by city_name,city_id,period union all select city_name, city_name county_name, city_id city_id, period, sum(supply_num) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'"+date_quarter+"' quarter from dwb_db.dwb_issue_supply_city where dr = 0 and period = '"+date_quarter+"' and city_id in ('442000','441900') group by city_name,city_id,period;

insert into dws_db_prd.dws_supply select city_name, city_name county_name, city_id city_id, period, sum(supply_num) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,period quarter from dwb_db.dwb_issue_supply_city where dr = 0 and period < '2020Q4' and city_id in ('442000','441900') group by city_name,city_id,period;

update dws_db_prd.dws_supply set period_index = 1 where period = quarter ;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------





select * from 
  (select city_id,city_name,value,period from dws_db_prd.dws_supply where period in ('2020Q4','2021Q1','2021Q2') and city_name = county_name and dr = 0) t1 
left join 
  (select city_id,supply_num,period from dwb_db.dwb_issue_supply_city where period in ('2020Q4','2021Q1','2021Q2') and dr = 0) t2
on t1.city_id = t2.city_id and t1.period=t2.period where t1.city_id not in ('442000','441900');

select * from dws_db_prd.dws_newest_popularity_rownumber_quarter where city_id in (select region_id from dwb_db.dwb_dim_geography_55city where region_name = '濠江区' and city_name = '汕头市' group by region_id) and period = '2019Q3';


select newest_id,sales_state from dws_db_prd.dws_newest_info where city_id in (select city_id from  dwb_db.dwb_dim_geography_55city where city_name = '宝鸡市' group by city_id) and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period = '2019Q2' and dr = 0 group by newest_id) and dr =0 ;

select newest_id,newest_name,alias_name,address,city_id,county_id,lng,lat from dws_db_prd.dws_newest_info where city_id in (select city_id from  dwb_db.dwb_dim_geography_55city where city_name = '三亚市' group by city_id) and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period = '2019Q1' group by newest_id) ;

select city_id,county_id from dws_db_prd.dws_newest_info where city_id in (select city_id from  dwb_db.dwb_dim_geography_55city where city_name = '三亚市' group by city_id) and newest_id in (select newest_id from dws_db_prd.dws_newest_period_admit where period = '2018Q1' group by newest_id) and newest_id in (select newest_id from dwb_db.dwb_newest_issue_offer where city='三亚市' group by newest_id);


select * from dwb_db.dwb_dim_geography_55city where city_name = '三亚市';


CREATE TABLE dwb_db.dwb_newest_issue_offer (
  `id` int(6) NOT NULL AUTO_INCREMENT COMMENT '主键',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼id',
  `newest_name` varchar(255) DEFAULT NULL COMMENT '楼名',
  `address` varchar(255) DEFAULT NULL COMMENT '楼地址',
  `developer` varchar(255) DEFAULT NULL COMMENT '楼公司',
  `formal_newest_name` varchar(255) DEFAULT NULL COMMENT '楼显示名',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼别名',
  `city` varchar(40) DEFAULT NULL COMMENT '城市名称',
  `issue_code` varchar(20) DEFAULT NULL COMMENT '编号',
  `issue_date` varchar(20) DEFAULT NULL COMMENT '发证时间',
  `issue_month` varchar(10) DEFAULT NULL COMMENT '发证月份',
  `issue_quarter` varchar(10) DEFAULT NULL COMMENT '发证季度',
  `issue_room` int(11) DEFAULT NULL COMMENT '房间数量',
  `issue_area` int(11) DEFAULT NULL COMMENT '面价',
  `dr` varchar(3) NOT NULL DEFAULT '0' COMMENT '有效标识(0,有效，1，作废）',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创时',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '更时',
  `newest_url` varchar(765) DEFAULT NULL COMMENT '网址',
  PRIMARY KEY (`id`),
  KEY `idx_newest_name` (`newest_name`) USING BTREE,
  KEY `idx_newest_issue_code` (`issue_code`) USING BTREE,
  KEY `idx_newest_city` (`city`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='楼盘预售证供应表';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 查看数据
-- 面积为0问题修复
-- 别名修复


select city_id,count(1) from 
  (select city_id,period from dws_db_prd.dws_customer_cre where city_id in (select city_id from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id) group by city_id,period) t group by city_id ;

select city_id,period from dws_db_prd.dws_customer_cre where city_id in ('320300') group by city_id,period;

select city_id,count(1) from 
  (select city_id,period from dws_db_prd.dws_customer_sum where city_id in (select city_id from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id) group by city_id,period) t group by city_id ;

select city_id,count(1) from 
  (select city_id,period from dws_db_prd.dws_customer_month where city_id in (select city_id from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id) group by city_id,period) t group by city_id ;
 
select city_id,period from dws_db_prd.dws_customer_month where city_id in ('360400') group by city_id,period;

select city_id,count(1) from 
  (select city_id,period from dws_db_prd.dws_customer_week where city_id in (select city_id from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id) group by city_id,period) t group by city_id ;


show create table dws_db_prd.dws_newest_alias ;
CREATE TABLE dws_db_prd.bak_20210923_dws_newest_alias (
  `alias_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '别名id',
  `alias_name` varchar(500) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(255) DEFAULT NULL COMMENT '城市id',
  `city_name` varchar(500) DEFAULT NULL COMMENT '城市',
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_name` varchar(128) NOT NULL COMMENT '楼盘名称',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0',
  `create_date` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  `update_user` varchar(45) DEFAULT NULL,
  PRIMARY KEY (`alias_id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=113738 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘别名表(去相似度低)---20210923备份';
insert into dws_db_prd.bak_20210923_dws_newest_alias select * from dws_db_prd.dws_newest_alias ;

show create table dws_db_prd.dws_newest_info; 
CREATE TABLE dws_db_prd.bak_20210923_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT NULL COMMENT '占地面积',
  `building_area` double DEFAULT NULL COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表 --- 20210923:10.23备份';
insert into dws_db_prd.bak_20210923_dws_newest_info select * from dws_db_prd.dws_newest_info; 

update dws_db_prd.dws_newest_info a ,(select newest_id,group_concat(alias_name) alias_name from dws_db_prd.dws_newest_alias group by newest_id) b set a.alias_name = b.alias_name where a.newest_id = b.newest_id;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
show create table dws_db_prd.dws_newest_layout ;
CREATE TABLE dws_db_prd.bak_20210922_dws_newest_layout (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `layout_id` bigint(20) DEFAULT NULL COMMENT '户型id（楼盘表id）',
  `newest_id` varchar(32) DEFAULT NULL COMMENT '楼盘id',
  `room` int(11) DEFAULT NULL COMMENT '室',
  `hall` int(11) DEFAULT NULL COMMENT '厅',
  `bathroom` int(11) DEFAULT NULL COMMENT '卫',
  `layout_area` double DEFAULT NULL COMMENT '户型面积',
  `layout_area_str` varchar(100) DEFAULT NULL COMMENT '户型面积字符串',
  `layout_price` double DEFAULT NULL COMMENT '户型总价',
  `layout_price_str` varchar(100) DEFAULT NULL COMMENT '户型总价字符串',
  `dr` tinyint(1) NOT NULL DEFAULT '0' COMMENT '删除标志',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` text,
  `update_date` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `update_user` text,
  `unit_price` double DEFAULT NULL COMMENT '均价',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=218991 DEFAULT CHARSET=utf8mb4 COMMENT='楼盘户型表--20210922备份表';
insert into dws_db_prd.bak_20210922_dws_newest_layout select * from dws_db_prd.dws_newest_layout;

select * from dws_db_prd.dws_newest_layout;

update dws_db_prd.dws_newest_layout set layout_area = 0 where layout_area < 20;

update dws_db_prd.dws_newest_layout set layout_price = null where layout_price < 6;

update dws_db_prd.dws_newest_layout set layout_price = null where layout_price = 0;

update dws_db_prd.dws_newest_layout a , dws_db_prd.dws_newest_info b set a.unit_price = b.unit_price where a.newest_id = b.newest_id ;

select * from dws_db_prd.dws_newest_layout where layout_area > 100 and layout_price < 100;

update dws_db_prd.dws_newest_layout set layout_price = unit_price*layout_area where newest_id not in (select newest_id from dws_db_prd.dws_newest_info where city_id in ('110000','310000','440100','440300') and dr = 0 group by newest_id );

update dws_db_prd.dws_newest_layout set layout_price = null where layout_price = 0;

select unit_price*layout_area from dws_db_prd.dws_newest_layout where dr = 0;

update dws_db_prd.dws_newest_layout set layout_price = ROUND((unit_price*layout_area)/10000,0);

select ROUND((unit_price*layout_area)/10000,0) from dws_db_prd.dws_newest_layout;

update dws_db_prd.dws_newest_layout set layout_price = null where layout_price = 0;



update dws_db_prd.dws_newest_layout a set a.unit_price = null where a.newest_id not in (select newest_id from dws_db_prd.dws_newest_info where dr = 0 group by newest_id)  ;

update dws_db_prd.dws_newest_layout a , dws_db_prd.dws_newest_info b set a.unit_price = b.unit_price where a.newest_id = b.newest_id ;

update dws_db_prd.dws_newest_layout set layout_price = ROUND((unit_price*layout_area)/10000,0);

update dws_db_prd.dws_newest_layout set layout_price = null where layout_price = 0;

select newest_id from dws_db_prd.dws_newest_info where unit_price is null and dr = 0  group by newest_id ; -- 10,765

select newest_id from dws_db_prd.dws_newest_info where unit_price is not null and dr = 0  group by newest_id ; -- 44,139

select newest_id ,max(replace(convert(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(layout_price_str , '万' , 1),'/',1),'.',1) using ascii),'?',''))*10000/min(layout_area) price  from dws_db_prd.dws_newest_layout where layout_area !=0 and layout_price is null and layout_price_str like '%万%' and replace(convert(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(layout_price_str , '万' , 1),'/',1),'.',1) using ascii),'?','') < 55000 and replace(convert(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(layout_price_str , '万' , 1),'/',1),'.',1) using ascii),'?','')> 30 and newest_id in (select newest_id from dws_db_prd.dws_newest_info where dr = 0 and city_id in ('310000') group by newest_id) group by newest_id;





update dws_db_prd.dws_newest_layout set layout_area = null where layout_area = 0;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select period from dws_db_prd.dws_newest_investment_pop_rownumber_quarter where city_id in ('110000','440100') and dr = 0 group by period ;

select period from dws_db_prd.dws_newest_popularity_rownumber_quarter where city_id in ('320600') and dr = 0 group by period ;

select city_id from dws_db_prd.dws_newest_investment_pop_rownumber_quarter where period = '2020Q2' and city_id like '%00' group by city_id ;

update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set dr = 1;

select period from dws_db_prd.dws_newest_investment_pop_rownumber_quarter where dr = 0 group by period ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select newest_id ,newest_name ,unit_price,a.city_id ,city_name,city_level_desc from dws_db_prd.dws_newest_info a left join dws_db_prd.dws_newest_layout on a.city_id = b.city_id where a.city_id in (select city_id from dwb_db.dwb_dim_geography_55city where dr=0 group by city_id) and city_level_desc = '一线城市' and unit_price < 3000;

update  dws_db_prd.dws_newest_info set unit_price = null where unit_price < 3000;

show create table  dws_db_prd.dws_newest_info ;
CREATE TABLE dws_db_prd.bak_20210922_dws_newest_info (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT NULL COMMENT '占地面积',
  `building_area` double DEFAULT NULL COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表---20210922备份（修改均价之前）';
insert into dws_db_prd.bak_20210922_dws_newest_info select * from dws_db_prd.dws_newest_info;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------


select tt1.newest_id,tt1.newest_name,tt1.address,tt2.developer,tt2.newest_name formal_newest_name,tt2.alias_name from (select newest_id,alias newest_name,address from (select min(id) housing_id,uuid newest_id,newest_name,address,developer from dwb_db.dim_housing where city_id = '460200' and uuid is not null group by uuid,newest_name,address,developer) t1 left join (select housing_id,alias from dwb_db.dim_housing_alias) t2 on t1.housing_id=t2.housing_id union select t1.newest_id,alias_name newest_name,address from (select newest_id,newest_name,address from dws_db_prd.dws_newest_info where city_id = '460200' and newest_id is not null group by newest_id,newest_name,address) t1 left join (select newest_id,alias_name from dws_db_prd.dws_newest_alias) t2 on t1.newest_id=t2.newest_id ) tt1 left join (select a1.*,b1.newest_name,b1.alias_name from (select uuid newest_id,developer from dwb_db.dim_housing where city_id = '460200' and uuid is not null group by uuid,developer) a1 left join (select newest_id,newest_name,alias_name from dws_db_prd.dws_newest_info where dr = 0 and city_id = '460200' and newest_id is not null group by newest_id,newest_name,alias_name) b1 on a1.newest_id = b1.newest_id) tt2 on tt1.newest_id = tt2.newest_id 
where tt1.newest_name is null ;

select newest_id,alias newest_name,address from (select min(id) housing_id,uuid newest_id,newest_name,address,developer from dwb_db.dim_housing where city_id = '460200' and uuid is not null group by uuid,newest_name,address,developer) t1 left join (select housing_id,alias from dwb_db.dim_housing_alias) t2 on t1.housing_id=t2.housing_id where newest_id = '0b2520dbfa96ae0d33f6718c724e583b';

select t1.newest_id,case when alias_name is null then t1.newest_name else alias_name end newest_name,address from (select newest_id,newest_name,address from dws_db_prd.dws_newest_info where city_id = '460200' and newest_id is not null group by newest_id,newest_name,address) t1 left join (select newest_id,alias_name from dws_db_prd.dws_newest_alias) t2 on t1.newest_id=t2.newest_id where t1.newest_id = '0b2520dbfa96ae0d33f6718c724e583b';

select tt1.newest_id,tt2.alias from 
  (select newest_id from dws_db_prd.dws_newest_info group by newest_id having newest_id not in (select newest_id from dws_newest_alias group by newest_id)) tt1
left join 
  (select t1.uuid,t2.alias from 
      (select uuid,id housing_id from dwb_db.dim_housing where uuid is not null group by uuid,id) t1
   left join 
      (select alias ,housing_id from dwb_db.dim_housing_alias group by alias ,housing_id) t2 
   on t1.housing_id = t2.housing_id)tt2
on tt1.newest_id = tt2.uuid;

SELECT tt1.newest_id, tt1.newest_name, tt2.alias alias_name FROM ( SELECT newest_id,newest_name FROM dws_db_prd.dws_newest_info GROUP BY newest_id,newest_name HAVING newest_id NOT IN (SELECT newest_id FROM dws_db_prd.dws_newest_alias GROUP BY newest_id)) tt1 LEFT JOIN ( SELECT t1.uuid, t2.alias FROM ( SELECT uuid, id housing_id FROM dwb_db.dim_housing WHERE uuid IS NOT NULL GROUP BY uuid, id) t1 LEFT JOIN ( SELECT alias , housing_id FROM dwb_db.dim_housing_alias GROUP BY alias , housing_id) t2 ON t1.housing_id = t2.housing_id)tt2 ON tt1.newest_id = tt2.uuid 
where tt2.alias is not null;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select newest_id,alias newest_name,address,developer from (select min(id) housing_id,uuid newest_id,newest_name,address,developer from dwb_db.dim_housing where city_id = '460200' and uuid is not null group by uuid,newest_name,address,developer) t1 left join (select housing_id,alias from dwb_db.dim_housing_alias) t2 on t1.housing_id=t2.housing_id ; 

select newest_id,period,exist,imei_num from dws_db_prd.dws_customer_cre group by newest_id,period,exist,imei_num ;

select newest_id,period,exist,sum(imei_num) imei_num from dws_db_prd.dws_customer_month group by newest_id,period,exist;

select * from 
  (select newest_id,period,exist,imei_num from dws_db_prd.dws_customer_cre group by newest_id,period,exist,imei_num) t1
left join 
  (select newest_id,period,exist,sum(imei_num) imei_num from dws_db_prd.dws_customer_month group by newest_id,period,exist) t2
on t1.newest_id = t2.newest_id and t1.period=t2.period and t1.exist = t2.exist
where t1.imei_num!=t2.imei_num and t2.newest_id is not null and t1.exist = '增量';


-- 重跑周度和月度
-- truncat
-- 新增数据

truncate table dws_db_prd.dws_customer_month;
truncate table dws_db_prd.dws_customer_week;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dwb_db'
AND TABLE_NAME LIKE  '%housing%';  


SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dw_a'
AND TABLE_NAME LIKE  '%housing%';  

truncate table dwb_db.dim_housing_alias; 

select min(id),uuid,newest_name,address,developer from dwb_db.dim_housing where city_id = '460200' and new  group by uuid,newest_name,address,developer;

select city_id ,period from dws_db_prd.dws_newest_popularity_rownumber_quarter where index_rate is null group by city_id ,period; 

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q1';

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q2';

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q3';

update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q4';

select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('330303','330304','330382','331102','350205','350211','350212','350213','360402','360403','360404','360424','360425','360702','360703','370302','370303','370305','370306','370321','440402','440403','440404','441202','441203','441204','441302','441303','441322','441323') and county_id is not null and county_id != ''  group by newest_id,city_id,county_id;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

SELECT newest_id,newest_name,t1.city_id,t1.county_id,city_name,region_name,unit_price,address FROM 
 (SELECT newest_id,newest_name,city_id,county_id,unit_price,address FROM dws_db_prd.dws_newest_info 
 WHERE city_id IN ('110000','310000','440300','440100') AND unit_price IS NOT NULL AND unit_price <20000) t1
LEFT JOIN (SELECT city_id,city_name FROM dws_db_prd.dim_geography WHERE grade=3) t2
ON t1.city_id=t2.city_id 
LEFT JOIN (SELECT region_id,region_name FROM dws_db_prd.dim_geography WHERE grade=4) t3
ON t1.county_id=t3.region_id ;


select ROUND(urgent*0.8) from dws_db_prd.dws_newest_customer_qua ;

show create table dws_db_prd.dws_newest_customer_qua ;
CREATE TABLE dws_db_prd.dws_newest_customer_qua_true (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `period` varchar(255) DEFAULT NULL COMMENT '周期',
  `newest_id` varchar(50) DEFAULT NULL COMMENT '楼盘id',
  `intention` int(6) DEFAULT '0' COMMENT '定向看盘数量',
  `urgent` int(6) DEFAULT '0' COMMENT '迫切买房数量',
  `investment` int(6) DEFAULT NULL COMMENT '投资客户数量',
  `owner` int(6) DEFAULT NULL COMMENT '自住客户数量',
  `investment_rate` double(10,8) DEFAULT NULL COMMENT '投资比例',
  `owner_rate` double(10,8) DEFAULT NULL COMMENT '自住比例',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='意向选房与迫切买房客户统计_真';
insert into dws_db_prd.dws_newest_customer_qua_true select * from dws_db_prd.dws_newest_customer_qua ;

update dws_db_prd.dws_newest_customer_qua set urgent = ROUND(urgent*0.8) ;

show create table dws_db_prd.dws_newest_city_qua; 
CREATE TABLE dws_db_prd.dws_newest_city_qua_true (
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区域',
  `period` varchar(255) NOT NULL COMMENT '周期',
  `for_sale` int(6) NOT NULL DEFAULT '0' COMMENT '待售数量',
  `on_sale` int(6) NOT NULL DEFAULT '0' COMMENT '在售数量',
  `sell_out` int(6) NOT NULL DEFAULT '0' COMMENT '售罄数量',
  `total_count` int(6) DEFAULT '0' COMMENT '项目总量',
  `follow` int(6) NOT NULL DEFAULT '0' COMMENT '关注楼盘数量',
  `intention` int(6) NOT NULL DEFAULT '0' COMMENT '意向选房数量',
  `urgent` int(6) NOT NULL DEFAULT '0' COMMENT '迫切买房数量',
  `increase` int(6) NOT NULL DEFAULT '0' COMMENT '当季新增',
  `retained` int(6) NOT NULL DEFAULT '0' COMMENT '当季留存',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `update_time` datetime DEFAULT NULL COMMENT '更新时间',
  `dr` int(11) DEFAULT NULL COMMENT '0有效;1无效'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='城市项目总量与意向客户统计_真';
insert into dws_db_prd.dws_newest_city_qua_true select * from dws_db_prd.dws_newest_city_qua;
select ROUND(urgent*0.8) from dws_db_prd.dws_newest_city_qua;

update dws_db_prd.dws_newest_city_qua set urgent = ROUND(urgent*0.8) ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
select * from 
  (select city_id from dws_db_prd.dws_newest_city_qua where period = '2020Q4' group by city_id) t1
left join 
  (select cityid from dws_db_prd.dws_supply where period = '2020Q4' group by cityid) t2
on t1.city_id = t2.cityid where t2.cityid is null ;

update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set dr = 1 where period in ('2020Q3','2019Q2') and city_id like '3206%' or city_id like '110%' or city_id like '4401%';
update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where city_id like '3206%' or city_id like '1101%' or city_id like '4401%'  ;

delete from dws_db_prd.dws_newest_popularity_rownumber_quarter where substr(create_time,1,10) = '2021-09-17'; 



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

show processlist;

kill 3409874;

0+cast(SUBSTRING_INDEX(park_rate,':',1) as char)

select * from 
  (select city_id ,county_id from dws_db_prd.dws_newest_city_qua where county_id is not null and period = '2021Q2' and dr = 1) t1
left join 
  (select city_id ,county_id from dws_db_prd.dws_newest_city_qua where county_id is not null and period = '2021Q2' and dr = 0) t2
on t1.city_id= t2.city_id and t1.county_id = t2.county_id where t2.city_id is null;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

truncate table dws_db_prd.bak_20210915_dws_imei_browse_tag_copy;

select count(1) from 
 (select imei,concern,cre from dws_db_prd.dws_imei_browse_tag where period = '2019Q1' group by imei,concern,cre) t; -- 570344

select count(1) from 
  (select * from dws_db_prd.dws_imei_browse_tag where period = '2019Q1' ) t; -- 570344
  
show create table dws_db_prd.dws_imei_browse_tag ;
CREATE TABLE dws_db_prd.bak_20210916_dws_imei_browse_tag (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `period` varchar(255) NOT NULL COMMENT '分析周期',
  `imei` varchar(255) NOT NULL COMMENT 'imei',
  `concern` varchar(255) DEFAULT NULL COMMENT '关注',
  `intention` varchar(255) DEFAULT NULL COMMENT '意向',
  `urgent` varchar(255) DEFAULT NULL COMMENT '迫切',
  `cre` varchar(255) NOT NULL COMMENT '增存',
  PRIMARY KEY (`id`),
  KEY `idx_dws_imei_browse_tag_period` (`period`) USING BTREE,
  KEY `idx_dws_imei_browse_tag_imei` (`imei`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=16194481 DEFAULT CHARSET=utf8mb4 COMMENT='20210916备份 -- 客户浏览标签结果表';
insert into dws_db_prd.bak_20210916_dws_imei_browse_tag select * from dws_db_prd.dws_imei_browse_tag;
truncate table dws_db_prd.dws_imei_browse_tag; 
truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter; 
truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter; 
  
update dws_db_prd.dws_newest_popularity_rownumber_quarter set dr = 1 where period = '2020Q1';
update dws_db_prd.dws_newest_investment_pop_rownumber_quarter set dr = 1 where period = '2020Q1';

truncate table dwb_db.dwb_newest_customer_info;
truncate table dwb_db.dwb_newest_city_customer_num;
truncate table dwb_db.dwb_newest_county_customer_num;

truncate table dws_db_prd.dws_customer_cre;
truncate table dws_db_prd.dws_customer_sum;
truncate table dws_db_prd.dws_customer_week;
truncate table dws_db_prd.dws_customer_month;

update dws_db_prd.dws_supply a ,(select city_id,city_name,county_id,county_name,intention,period,quarter from dwb_db.dwb_newest_county_customer_num where city_id != county_id and period != quarter and city_name in ('北京市','上海市','深圳市','广州市') and quarter in ('2020Q3','2020Q4','2021Q1','2021Q2')) b
set a.follow_people_num=b.intention where a.period_index = 2 and a.cityid = b.city_id and a.city_id = b.county_id and a.period = b.period;

select city_id,city_name,county_id,county_name,intention,period,quarter from dwb_db.dwb_newest_county_customer_num 
where city_id != county_id and period != quarter and city_name in ('北京市','上海市','深圳市','广州市') 
  and quarter in ('2020Q3','2020Q4','2021Q1','2021Q2');
 
select cityid,city_name,city_id,county_name,follow_people_num,period,quarter from dws_db_prd.dws_supply where period_index = 2;

update dws_db_prd.dws_newest_provide_sche set provide_sche = replace(provide_sche,'15元','15000元') where newest_id = '6d7beeded20add24373aa2a8c1c89b4f';


truncate table dws_db_prd.dws_newest_popularity_rownumber_quarter;
truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter;
truncate table dws_db_prd.dws_customer_sum;
truncate table dws_db_prd.dws_customer_week;
truncate table dws_db_prd.dws_customer_month;
truncate table dwb_db.dwb_newest_city_customer_num;
truncate table dwb_db.dwb_newest_county_customer_num;

truncate table dws_db_prd.dws_customer_cre ;

select b.city_id,a.* from (
select newest_id,'增量' exist,increase imei_num,period 
from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter 
union all 
select newest_id,'存量' exist,retained imei_num,period 
from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter
) a 
inner join 
(select newest_id,city_id,county_id from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,county_id) b on a.newest_id=b.newest_id 
where b.city_id is not null and a.newest_id in ('39ccb8bfbb901b7e160dfa048a6f733b','afe4138b946b93640508c63451876732','23a8c8843ecc07bb0a4d3dda2e8b2ebe');




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select * from dws_tag_purchase_mobile;

select * from dws_newest_layout_price where `type` = 'resi_district';

-- 居住小区价格（补充楼盘缺失季度的数据）
-- 将有居住小区价格的统计出楼盘全量
-- 统计每个季度没有居住小区价格的楼盘
-- 从18Q1开始，判断当前季度第二步的结果存在在第一步结果当中，求交集，将结果的数据改变季度，添加到表中

select city_id,newest_id,city_level_desc,quarter,`interval`,percent,`type` from dws_newest_layout_price where `type` = 'resi_district';

select city_id,newest_id,city_level_desc,max(quarter),`type` from dws_newest_layout_price where `type` = 'resi_district'
group by city_id,newest_id,city_level_desc,`type`;

select newest_id,city_id,city_name from dws_db_prd.dws_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,city_name,county_id,county_name;

show create table dws_db_prd.dws_newest_layout_price;
CREATE TABLE dws_db_prd.dws_newest_layout_price_copy (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(255) NOT NULL COMMENT '城市id',
  `newest_id` varchar(255) NOT NULL COMMENT '楼盘id',
  `city_level_desc` varchar(255) NOT NULL COMMENT '城市等级',
  `quarter` varchar(255) NOT NULL COMMENT '分析季度',
  `interval` varchar(255) NOT NULL COMMENT '区间',
  `percent` varchar(255) NOT NULL COMMENT '区间占比',
  `type` varchar(255) NOT NULL COMMENT '类型(layout_area-面积区间;layout_price-总价区间;layout_visit-关注周期;resi_district-居住小区均价;room-户型;unit_price-关注楼盘均价)',
  PRIMARY KEY (`id`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=7929736 DEFAULT CHARSET=utf8mb4 COMMENT='用户画像-购房特征----数据备份';
insert into dws_db_prd.dws_newest_layout_price_copy select * from dws_db_prd.dws_newest_layout_price;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select * from dws_db_prd.dws_newest_info dni where newest_name = '映虹桥';

select count(1) from 
  (select newest_id from dws_db_prd.dws_tag_purchase_mobile where mobile_brand = '苹果' and newest_id is not null group by newest_id) t ;

select newest_id from dws_db_prd.dws_tag_purchase_mobile group by newest_id;

select *,a_sum/b_sum rate from 
    (select count(1) a_sum from 
        (select newest_id from dws_db_prd.dws_tag_purchase_mobile where mobile_brand = '苹果' and newest_id is not null group by newest_id) a ) t1
,
    (select count(1) b_sum from (select newest_id from dws_db_prd.dws_tag_purchase_mobile where newest_id is not null group by newest_id) b) t2;


select newest_id from dws_db_prd.dws_newest_layout_price where `type` = 'resi_district' and newest_id is not null group by newest_id;

select newest_id from dws_db_prd.dws_newest_layout_price where newest_id is not null group by newest_id;

select *,a_sum/b_sum rate from 
    (select count(1) a_sum from 
        (select newest_id from dws_db_prd.dws_newest_layout_price where `type` = 'resi_district' and newest_id is not null group by newest_id) a ) t1
,
    (select count(1) b_sum from (select newest_id from dws_db_prd.dws_newest_layout_price where newest_id is not null group by newest_id) b) t2;


select count(1) from  
(select newest_name from odsdb.ori_newest_poi_info onpi where file_name in ('20210914_latlng_rs','20210914_latlng_rs_v3') group by newest_name) t ;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

-- 容积率
update dws_db_prd.dws_newest_info set volume_rate = null where volume_rate>10 or volume_rate<0.1;

update dws_db_prd.dws_newest_info set volume_rate = case when volume_rate is null then null else cast(volume_rate as decimal(10,3)) end;

-- 建筑类型
update dws_db_prd.dws_newest_info set building_type = right(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '^%';

update dws_db_prd.dws_newest_info set building_type = left(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '%^';

update dws_db_prd.dws_newest_info set building_type = replace(building_type,'^',',') where building_type like '%^%';

update dws_db_prd.dws_newest_info set building_type = replace(building_type,'、',',') where building_type like '%、%';

-- 车位数
update dws_db_prd.dws_newest_info set park_num = null where park_num<10;

-- 面积
update dws_db_prd.dws_newest_info set land_area = null where land_area<1000;

update dws_db_prd.dws_newest_info set building_area = null where building_area<1000;



-- 更新planinfo表和dwb_newest_info表
update dws_db_prd.dws_newest_planinfo a, dws_db_prd.dws_newest_info b set a.volume_rate=case when b.volume_rate is null then null else cast(b.volume_rate as decimal(10,1)) end where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dws_db_prd.dws_newest_planinfo a,(select newest_id, newest_name,building_type  from dws_db_prd.dws_newest_info where dr = 0) b set a.building_type=b.building_type where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dws_db_prd.dws_newest_planinfo a,(select newest_id, newest_name,park_num  from dws_db_prd.dws_newest_info where dr = 0) b set a.park_num=b.park_num where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dws_db_prd.dws_newest_planinfo a,(select newest_id, newest_name,land_area  from dws_db_prd.dws_newest_info where dr = 0) b set a.land_area=b.land_area where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dws_db_prd.dws_newest_planinfo a,(select newest_id, newest_name,building_area  from dws_db_prd.dws_newest_info where dr = 0) b set a.building_area=b.building_area where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dws_db_prd.dws_newest_planinfo a,(select newest_id, newest_name,park_rate  from dws_db_prd.dws_newest_info where dr = 0) b set a.park_rate=b.park_rate where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;



update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,volume_rate  from dws_db_prd.dws_newest_info where dr = 0) b set a.volume_rate=case when b.volume_rate is null then null else cast(b.volume_rate as decimal(10,1)) end where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,building_type  from dws_db_prd.dws_newest_info where dr = 0) b set a.building_type=b.building_type where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,park_num  from dws_db_prd.dws_newest_info where dr = 0) b set a.park_num=b.park_num where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,land_area  from dws_db_prd.dws_newest_info where dr = 0) b set a.land_area=b.land_area where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,building_area  from dws_db_prd.dws_newest_info where dr = 0) b set a.building_area=b.building_area where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;

update dwb_db.a_dwb_newest_info a,(select newest_id, newest_name,park_rate  from dws_db_prd.dws_newest_info where dr = 0) b set a.park_rate=b.park_rate where a.newest_id = b.newest_id and a.newest_name = b.newest_name ;



-- 车位配比
update dws_db_prd.dws_newest_planinfo set park_rate = concat(0+cast(SUBSTRING_INDEX(park_rate,':',1) as char),':',0+cast(SUBSTRING_INDEX(park_rate,':',-1) as char)) where park_rate is not null;



update dws_db_prd.dws_newest_planinfo set volume_rate = null where volume_rate>10 or volume_rate<0.1;

update dws_db_prd.dws_newest_planinfo set volume_rate = case when volume_rate is null then null else cast(volume_rate as decimal(10,1)) end;

-- 建筑类型
update dws_db_prd.dws_newest_planinfo set building_type = right(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '^%';

update dws_db_prd.dws_newest_planinfo set building_type = left(building_type,CHAR_LENGTH(building_type) - 1) where  building_type like '%^';

update dws_db_prd.dws_newest_planinfo set building_type = replace(building_type,'^',',') where building_type like '%^%';

update dws_db_prd.dws_newest_planinfo set building_type = replace(building_type,'、',',') where building_type like '%、%';

-- 车位数
update dws_db_prd.dws_newest_planinfo set park_num = null where park_num<10;

-- 面积
update dws_db_prd.dws_newest_planinfo set land_area = null where land_area<1000;

update dws_db_prd.dws_newest_planinfo set building_area = null where building_area<1000;


select concat(0+cast(SUBSTRING_INDEX(park_rate,':',1) as char),':',0+cast(SUBSTRING_INDEX(park_rate,':',-1) as char)) park_rate from dws_db_prd.dws_newest_info where park_rate;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select volume_rate from dws_db_prd.dws_newest_planinfo where newest_id = '7661c4fe4aa15ee1140c7acee8b0bfb5';

update dws_db_prd.dws_supply set period_index = 2 where period != quarter ;

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dwb_db'
AND TABLE_NAME LIKE  '%a_dwb_newest_info%';   -- a_dwb_newest_info	2021-08-28 17:32:40.0	2021-09-07 18:26:38.0

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dws_db_prd'
AND TABLE_NAME LIKE  '%_newest_planinfo%';    --  2021-08-27 09:50:27.0	2021-09-06 10:41:10.0


SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dw_a'
AND TABLE_NAME LIKE  '%a_dwb_newest_info%';  -- a_dwb_newest_info	2021-08-30 17:57:48.0


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

show create table dws_db_prd.dws_tag_purchase_poi; 
CREATE TABLE temp_db.bak_20210914_dws_tag_purchase_poi (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` varchar(255) NOT NULL COMMENT '城市名称',
  `newest_id` varchar(255) DEFAULT NULL COMMENT '楼盘id',
  `tag_value` varchar(255) NOT NULL COMMENT '三级标签值',
  `tag_detail` varchar(255) NOT NULL COMMENT '配套详情',
  `pure_distance` varchar(255) NOT NULL COMMENT '直线距离',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `tag_value2` varchar(255) NOT NULL COMMENT '二级标签值',
  PRIMARY KEY (`id`),
  KEY `ind_lng_lat` (`city_id`,`tag_detail`),
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB AUTO_INCREMENT=50577744 DEFAULT CHARSET=utf8mb4 COMMENT='配套信息表---20210914备份表';
insert into temp_db.bak_20210914_dws_tag_purchase_poi select * from dws_db_prd.dws_tag_purchase_poi;

update dwb_db.dwb_tag_purchase_poi_info set dr = 1 where poi_type = '火车站';



insert into dws_db_prd.dws_tag_purchase_poi(city_id,newest_id,tag_value,tag_detail,pure_distance,lng,lat,tag_value2) select city_id,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,
SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2  from dwb_db.dwb_tag_purchase_poi_info where dr = 0; 

update dwb_db.dwb_tag_purchase_poi_info set dr = 1; 




select city_id,newest_id,poi_index tag_value,poi_name tag_detail,pure_distance,SUBSTRING_INDEX(poi_lnglat,',',1) lng,
SUBSTRING_INDEX(poi_lnglat,',',-1) lat,poi_type tag_value2  from dwb_db.dwb_tag_purchase_poi_info where dr = 0;

select newest_id ,max(id),pure_distance from dws_db_prd.dws_tag_purchase_poi where pure_distance<=3 group by newest_id ;

select newest_id ,max(id),count(1) distance from dws_db_prd.dws_tag_purchase_poi where pure_distance<=3 group by newest_id;

CREATE TABLE temp_db.tmp_poi_exsited_newest_id (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `newest_id` varchar(255) not NULL COMMENT '楼盘id',
  `max_id` varchar(33) COMMENT '原始表id',
  PRIMARY KEY (`id`),
  KEY `tmp_poi_exsited_newest_id` (`newest_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='配套已存楼盘临时表';

insert into temp_db.tmp_poi_exsited_newest_id(newest_id,max_id) select newest_id ,max(id) from dws_db_prd.dws_tag_purchase_poi where pure_distance<=3 group by newest_id;





-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

select imei from dwb_db.a_dwb_customer_browse_log where visit_date >= '2020-10-01' and visit_date <= '2020-12-31' and newest_id = '5e966110d7e0c7064dc68072990b926f' group by imei ;

select imei,newest_id,visit_date from dwb_db.dwb_customer_browse_log where visit_date >= '2020-10-01' and visit_date <= '2020-12-31' and newest_id = '5e966110d7e0c7064dc68072990b926f';

select newest_id ,count(1) from
(select newest_id ,substr(issue_code,1,2) from dws_db_prd.dws_newest_issue_code group by newest_id ,substr(issue_code,1,2)) t
group by newest_id having count(1)>1; 

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

update dws_db_prd.dws_newest_info a , 
(select newest_id,browse_avg_price from dwb_db.dwb_cust_2_newest_price_are where pal_name = 'clean_result') b
set a.unit_price = b.browse_avg_price where a.newest_id = b.newest_id;

update dws_db_prd.dws_newest_info set volume_rate = null where volume_rate>10 or volume_rate<0.1;

update dws_db_prd.dws_newest_info set volume_rate = case when volume_rate is null then null else cast(volume_rate as decimal(10,3)) end;

truncate table  dws_db_prd.dws_newest_info;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

update odsdb.ori_newest_period_admit_info set dr = 1 where id <= 100233;

select * from dwb_db.dwb_cust_2_newest_price_are;

insert into dwb_db.dwb_cust_2_newest_price_are 
(newest_id,newest_name,direct_avg_price,browse_avg_price,avg_price_rate,browse_avg_price_sum,browse_avg_price_count,browse_count,period,dr,create_time,update_time,pal_name)
select newest_id,newest_name,direct_avg_price,
       cast(sum(browse_avg_price_sum)/sum(browse_avg_price_count) as decimal(18,0))  browse_avg_price,
       cast(case when direct_avg_price is null then 0 else direct_avg_price end /(sum(browse_avg_price_sum)/sum(browse_avg_price_count)) as decimal(18,2)) avg_price_rate,
       sum(browse_avg_price_sum) browse_avg_price_sum,sum(browse_avg_price_count) browse_avg_price_count,null,null,dr,now(),now(),pal_name 
from dwb_db.dwb_cust_2_newest_price_are 
where newest_id not in (select newest_id from dwb_db.dwb_cust_2_newest_price_are where avg_price_rate = 1 and pal_name = '贝壳' group by newest_id)
      and browse_avg_price_count != 0 and browse_avg_price_sum != 0
group by newest_id,newest_name,direct_avg_price,dr,pal_name 
  having avg_price_rate<0.9 or avg_price_rate>1.1;

select newest_id from dwb_db.dwb_cust_2_newest_price_are where avg_price_rate = 1 and pal_name = '贝壳' group by newest_id ;

select pal_name,count(1) from dwb_db.dwb_cust_2_newest_price_are group by pal_name; 


show create table dws_db_prd.dws_newest_info;
CREATE TABLE dws_db_prd.dws_newest_info_test_price (
  `newest_id` varchar(32) NOT NULL COMMENT '楼盘id',
  `newest_sn` varchar(45) DEFAULT NULL COMMENT '楼盘编号',
  `newest_name` varchar(100) NOT NULL COMMENT '楼盘名称',
  `alias_name` varchar(255) DEFAULT NULL COMMENT '楼盘别名',
  `city_id` varchar(6) DEFAULT NULL COMMENT '城市id',
  `county_id` varchar(6) DEFAULT NULL COMMENT '区县id',
  `address` varchar(128) DEFAULT NULL COMMENT '楼盘地址',
  `lng` decimal(10,7) DEFAULT NULL COMMENT '经度',
  `lat` decimal(10,7) DEFAULT NULL COMMENT '纬度',
  `property_type` varchar(100) DEFAULT NULL COMMENT '物业类型',
  `property_fee` varchar(255) DEFAULT NULL COMMENT '物业费',
  `property_id` varchar(32) DEFAULT NULL COMMENT '物业公司id',
  `building_type` varchar(100) DEFAULT NULL COMMENT '建筑类型',
  `land_area` double DEFAULT NULL COMMENT '占地面积',
  `building_area` double DEFAULT NULL COMMENT '建筑面积',
  `building_num` varchar(255) DEFAULT NULL COMMENT '楼栋数',
  `floor_num` varchar(255) DEFAULT NULL COMMENT '建筑层数',
  `household_num` int(11) DEFAULT NULL COMMENT '规划户数',
  `right_term` varchar(45) DEFAULT NULL COMMENT '产权年限',
  `green_rate` varchar(10) DEFAULT NULL COMMENT '绿化率',
  `volume_rate` float(10,3) DEFAULT NULL COMMENT '容积率',
  `park_num` varchar(255) DEFAULT NULL COMMENT '车位数',
  `park_rate` varchar(255) DEFAULT NULL COMMENT '车位配比',
  `decoration` varchar(100) DEFAULT NULL COMMENT '装修情况',
  `sales_state` varchar(45) DEFAULT NULL COMMENT '销售状态',
  `sale_address` varchar(255) DEFAULT NULL COMMENT '售楼处地址',
  `opening_date` date DEFAULT NULL COMMENT '开盘时间',
  `recent_opening_time` text COMMENT '最近开盘时间',
  `recent_delivery_time` text COMMENT '最近交房时间',
  `unit_price` int(11) DEFAULT NULL COMMENT '均价',
  `dr` tinyint(1) unsigned NOT NULL DEFAULT '0' COMMENT '删除标记（0：未删，1:已删）',
  `create_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `create_user` varchar(45) DEFAULT NULL,
  `update_date` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_user` varchar(45) DEFAULT NULL,
  `index` varchar(255) DEFAULT NULL COMMENT 'jpg图片索引',
  `jpg` varchar(255) DEFAULT NULL COMMENT 'jpg图片路径',
  KEY `newest_id` (`newest_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='新房楼盘表--均价更新结果表';
insert into dws_db_prd.dws_newest_info_test_price select * from dws_db_prd.dws_newest_info;

update dws_db_prd.dws_newest_info a , 
(select newest_id,browse_avg_price from dwb_db.dwb_cust_2_newest_price_are where pal_name = 'clean_result') b
set a.unit_price = b.browse_avg_price where a.newest_id = b.newest_id;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
select * from 
    (select city_name,issue_code,newest_name from dws_db_prd.dws_newest_issue_code) t1
right join
    (select city,issue_code,newest_name from dwb_db.dwb_newest_issue_offer) t2  -- 66332
on t1.city_name=t2.city and t1.issue_code = t2.issue_code 
where t1.city_name is not null;   -- 37884
select * from 
(select t2.city_id,sum(intention),sum(orien),sum(urgent),sum(increase),sum(retained) from 
  (select newest_id,quarter,intention,orien,urgent,increase,retained from dwb_db.dwb_newest_customer_info where quarter = '2021Q2') t1
left join 
  (select newest_id,city_id from dwb_db.a_dwb_newest_info where newest_id is not null and city_id in ('110000','120000','130100','130200','130600','210100','220100','310000','320100','320200','320300','320400','320500','320600','321000','330100','330200','330300','330400','330500','330600','331100','340100','350100','350200','360100','360400','360700','370100','370200','370300','370600','370800','410100','420100','430100','440100','440300','440400','440500','440600','441200','441300','441900','442000','450100','460100','460200','500000','510100','520100','530100','610100','610300','610400') and county_id is not null and county_id != '' group by newest_id,city_id,city_name,county_id,county_name) t2
on t1.newest_id = t2.newest_id
group by t2.city_id,t1.quarter) tt1
left join 
(select city_id,follow,intention,urgent,increase,retained from dws_db_prd.dws_newest_city_qua where period = '2021Q2' and county_id is null) tt2
on tt1.city_id = tt2.city_id;



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
truncate table dws_db_prd.dws_newest_investment_pop_rownumber_quarter ;

select imei,'投资型' type  from dwb_db.b_dwb_customer_imei_tag where is_college_stu = '否' and marriage = '已婚' and education = '高' and have_child = '有' group by imei;

delete from dwb_db.dwb_cust_2_newest_price_are where period >= '2019Q1';

truncate table dwb_db.dwb_cust_2_newest_price_are; 

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
CREATE TABLE dwb_db.dwb_cust_2_newest_price_are (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`newest_id` varchar(33) NOT NULL COMMENT '楼盘id',
	`newest_name` varchar(255) COMMENT '楼盘名',
	`direct_avg_price` int(11) COMMENT '直取均价',
	`browse_avg_price` int(11) COMMENT '浏览均价',
	`avg_price_rate` double COMMENT '均价占比',
	`direct_total_price` int(11) COMMENT '直取总价',
	`browse_total_price` int(11) COMMENT '浏览总价',
	`total_price_rate` double COMMENT '均价占比',
	`direct_area` int(11) COMMENT '直取面积',
	`browse_area` int(11) COMMENT '浏览面积',
	`area_rate` double COMMENT '均价占比',
	`browse_avg_price_sum` int(22) COMMENT '均价总和',
	`browse_avg_price_count` int(22) COMMENT '均价条数',
	`browse_total_price_sum` int(22) COMMENT '总价总和',
	`browse_total_price_count` int(22) COMMENT '总价条数',
	`browse_area_sum` int(22) COMMENT '面积总和',
	`browse_area_count` int(22) COMMENT '面积条数',
	`browse_count` int(11) COMMENT '浏览总条数',
	`period` varchar(30) COMMENT '浏览周期',
	`dr` int(2) COMMENT '有效标识',
	`create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	`update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_cust_2_newest_newest_id` (`newest_id`) USING BTREE COMMENT '楼盘id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户浏览楼盘均价面积表';


SELECT housing_id,plat_name,concat(substr(visit_time,1,4),'Q',QUARTER(visit_time)) `period`,avg_price FROM dwb_db.cust_browse_log_201801_202106 where housing_id is not null and avg_price is not null and avg_price != 'null' and avg_price != 'NULL' 
limit 100;

SELECT newest_id,unit_price from dws_db_prd.dws_newest_info where newest_id is not null group by newest_id,unit_price;

select plat_name from dwb_db.cust_browse_log_201801_202106 group by plat_name ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 用户浏览楼盘均价和楼盘均价对比
--   1、先把用户浏览楼盘均价计算出来（楼盘（id,名），均价加和，总价加和，面积加和，条数，周期（季度，null），浏览所得均价均价，楼盘原有均价，差额，dr，create_time,uodate_time）
--   2、更新楼盘表中价格
SHOW processlist;

kill 2450100;

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dwb_db'
AND TABLE_NAME LIKE  '%cust_browse_log_201801_202106%';


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
select city_id ,city_name  from dwb_db.dwb_issue_supply_city where num_index = '-' and period = '2019Q1' group by city_id ,city_name ;

select * from odsdb.city_newest_deal;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 区县id修改的楼盘，导致这次纠错，还有加上准入楼盘
-- 第一步： 查看已经整理好的楼盘有没有包含在这次纠错的楼盘内
--       		  包含： 删除
-- 			不包含： 保留原样，并查看是否包含在准入楼盘内   
--        		 包含： 保留                       
-- 			不包含： 删除                            是否在准入楼盘都保留
 
-- 第二步： 再整理一版楼盘爬取需求表，查看楼盘是否已经包含在第一步的结果当中
--                 包含： 删除
--          不包含：保留并开始爬取poi信息

select newest_id from dwb_db.dwb_tag_purchase_poi_info where newest_id in ('0286394cf680eb2d544f365ea8d11159','097a061a8569139796ac7cfe63c986fd','1380c3c0fc6fd919e42b16a300cefad4','153e5388057ac828a83c76013a347ca3','15adf036f7edb3946fb681f56d48ff08','184d0c90656c7eb355fa904e1bc3191b','18cacc9f25f8452bb5edccd1058ae568','1c2c1290af5795e1e91ad6fa81ae3eb4','22dfff963cf05e5c85f57a53cb20d9d3','260e48755ee1620d555d34d960856e2f','2b2df1c9f3d375dadf90dc61691fb4cd','2f07807c34b0942b73e770c9ef728838','4278c0e9f06680f294fb977cff3bf08c','4edc976fa99db891d373b5bbaa116d3e','52ee9c0160f0c3c60edc7cc22a834cbd','54d1c89ba7e61d95b28ad36867f773ce','558af565f79a26721f4a230a3e6da52f','5ed4bc6b126ede6da6f5b35973329b09','5edd9481d42fbd67af445106f680b72a','5f1c558717a6a568de41d7048f8092c6','60d47b2e8bdb3f5d863987a9656f3317','60def9f4324fa3d27501dace869c10ee','65e3a2d3afaec2fca28e9dc54fa83e9c','686eec4b2a33cd3847c7145148a9ee74','6afd96f17b0bd28fba753d1093a74a4a','6e4e2843c9439a1498be869c2db78f5d','72a4387804ceda007e36663e31b57a99','76660232df672d046c4df5f7a18025d0','866d79f39d109f35c1d881ded45f8a9c','870b3b3fb666f159760a19b21c30e9d3','8b067021409e6b5c1d87dab1157256c5','8c6b399e7f51f9a838aa630169851b18','8d602b4bcf6ba6e2e955825100302db4','99d7d9e0b4ce945d240c4ea4c49e0358','9c4416372d7dd4e64817059d6b394d47','9cfdff7ac7c91a99826dda0e45b54799','9db90dc8e2f3712de5811297c9923bef','a4cddab67185454f85c2c868d766cdd5','a9c7d29c297f7daf6a9fb8998b5b2371','ad7355d1f9a22cd8a43b6ea84848ef6e','af954e609f0ffd7afa8fad611d9bf1fc','b1074f5f32c2630356fc4d99fdfddade','b4479e8a1c3f91fd12bce024e10386de','c1f406a23c759644830f48f17a0fa530','c4fa84470217fd4279e52e831d8d129e','ca5a19de7cabd1c229c2f5572f15cd32','cb622289696e46abd62fdec0c6832c58','cce1829f236e2ccfdd5c767a900367e8','d09d49e53eb433ff8dc43e6b5aa8be9e','d6cafb4fb2a4f844c82402a66c93f09b','daba0682fd97e598f3acf59ee90d5839','dcbc49cfd8471fee8d64bed16b39de3d','eb1a6e587f53baa87c51f87ddee350fa','ebc3ab19677f6a4b2b417083bc3fb75c','ef6f0015a38f46535a35c3b5deb00f43','f2275f38d77b787eb9adb4a2b15be65c','fc8a7b860028bd719ed019417b82464f','fd3fc6b9a1d8f24a412a48743bd12b34','02d0c8a7b5269621fc1058a2fa01af48','0309d588f628960c5f79263bf6b2c5ca','0ab17c2230b8e0dcf5f31888fd599c48','0fe00e049f42ed985d0296f7639bbc3b','11531ce052f7c3a0e88133f8ca9fa93a','183b64db187169280001a26f443183fa','18bf81a2d65615660af3a00591613dcc','1fdd5fded8859f841bf2c9b7311208a2','2cb9adbdec4c03a4cb9758d36c9b26e9','30912b72c146325893d42f292fbd32da','3cfcc742e49fa872846ba2c754c41b96','3d219c9306cd2567a90bfa3c4773661c','4f02f020468b5dc92c5b43c6460eb4fd','62961de8c7190d500db5b9436e949865','7540fbda049be560b69e2d9af9de8eec','7e368b50611734d73fd41cb86920009e','823d9bcdce2021ebd36d3114869b5bf7','904f5ff90a4154921e734c6e52a04be5','9196531454f210675e670f763d33cd1f','953224f45dea23ccbb99d86897859840','9c2e72a79fc2b728985f0f0515544144','9ebc8d938600bf54fb894d4870d3d3db','9f22b0f03326faf98dc0062b2ac86c80','a574fef86c9227bcf84f9b2a4057a17e','a9ba8976c40e4db6e8258b1de3f98039','a9dd3d96b1a4f33573f5ba561b104f8b','ae449ae0cfc5a3c935cde8a76c466fdd','ae782f0d4c589d01b3e478680520cd97','b3343281d2f0d4698764afbaf414d733','c60c83f8e421bbea7f8f8eb66ecc6928','caab3b475b0891ab89630d908d5cca9b','d06e953e3f613d6fa65af087ca14e8fc','d7538ca71038fb11527cb789422aa5e8','d7f26c7e5e3064e19e4c4c77efa50046','dbf844ac048180c7d067e5c6aa7bc40a','dde33ccc513c6e08c5a79fb534f41c9a','e9298c5e54bf628e87b33f42d5bd07dd','efe2af8684ceb03b8459fcf918ba20ca','f0e423d6d4edf9bde7f52d91f4dea615','f8d3b5e96f29dc37c6858ac5c2ae6888','fc473e8da8d652a646d52237fcaeee85','ffaa798ef0df74ee19b03e1833ef58af','0100d812772f2eb93c2c170384bb8f2c','011b364f4f6ec92daa1cbe09f85e484a','012a4712f145be4ecc1f02b04839283d','0410a5c7a7ea29cfacfddcb8043814ee','04d3a1877576b7a113dd2a2b5f025e64','0653b01336da7bc230fbb8d7f45a19fd','072750e17a9b26d50d6e3d891e59572f','072f2c406ab008711abbb65e7c17a51d','08c6829f82929b83b60108f4abee943b','0a66cb56c064c8c610aa640e22220b61','0d3f997c9db46860b3f089d7a8162f14','0f0821214f56ffc7c4a1c84b59d7640b','1015de4b0182db7ec870a2d912f793e3','13e36bbcb332cc9bd36618411acb268c','14d0a4adedcde21a82db11d22022f7e1','15c3355181a3bf7fff4e3aab0e82348e','1663649e7841adbe09ea9e1b9d2ed837','18e51822a409bd45524de28ca6e66d20','1a2df90092b392d4869746633e65024a','1b94fbcb3cf05556c79e327116b6047f','1c4b9fdd14fb2298bf9698f1a8959010','1cb30b99dbe7818eca74dd2a7d17450b','1dcf669e8103cb76fab14a52ea891cfb','1ed5b6bdf553689881feb860588ec6af','238b72b5dea1658d4985928845a0a939','258600ff2f101cbaca86deacae744cd2','261e5613c93a3e5cb38aea186e765fad','287ebcd77128440a1728e30e7fb91a90','2c95224b62a3b39bf7cb92d6918a33a4','2ffb67844ea234c1f944ed96d6f25e71','3016b7066b64696a8c7a3ac7a42f6dd4','306e0e1e3be34dec0f28f4f4b9b71436','31024f0508196f2b6ea6def5c2942911','35927b3e885db846a3970a9b472d5f25','37449b5981899b868a54cb10cf16487d','38cd1c997896e83abf36096d90f7c7fd','38d52af499e1ce08a7fe5e5e0ebe5615','391eee28c84aaad8f44e15769aefefbb','396ef5d46299cba3a690b9312a756690','399f978d9390a4d2a7dfad2407ca9da6','3a57c47a251d2cd5b18514661da1da68','3cd12e7dfeab8448915242d31c24f74d','3eb6b1f1020049da2c2b48ea70b127b6','43c92ed9421b5b42a84d4e63f711218c','441b69200c14618ba96d0a27344bb1ad','453dac702025ad2ebce227ba89da618c','46769eba999a28dbe6ffbf0e3686d833','46f44ef28ae286e400c43375674f94ba','4912637f305d871343c466972d042136','4a7610e6709eb505d9b7219ca9c1b0bc','4a9bb23b4ac59c21d2b8d37c89238e59','4bae38e40e640fe735ca8b646f8df9e5','4bef9b9e141500250beabce5b8046361','4c9a9be8b7cd4cd0dbb237341b14310e','4c9b63d0f144431702a03fdda7189265','4dc4a3dd0392a67aa49cc95a6b559f61','4f825d046d099bea290e8ee1698a0fd2','5095993b356f71d1633f55e56f3566d1','54c9fcce3c98e187d90006e4465bc9de','561161a9d6f9afd38b832d81b6676bb4','5684898cb1e4f6d6b376002ebdbdea61','57177ed5dc93959f39879b8a9f3861c4','5783e645adc29a56c6a09067f48f8bad','57bd67715d72a2991961de06ad9fdedb','5b8e276c5f69aa0ab10ecedc7d982b92','5ce6c9ac3b945a2c81b144ec3d2f8f51','5d7a0674da3f27023868e88760326862','5d9e4b91a39b1dc45a6a8df1e3d5873b','5da3e29c5a18a3ac6b1d67927959a43a','5e9eed51680d50e63b5d2ac72f970954','60326bd27903aa176b7a5436cf8268e1','61a1b65bb56bea8d541f0bc8601ffa6f','62daac25fbeaff0577a7c68d8c47dfc9','631e7c667a78fcdac3e47d1991832a58','63b8b35768802719c0cdf1584861b3f0','67975c76a0710c50cf0e1a4fd3cb18e1','6856af5b6621f222fdd385cf38a396d9','6a53ddc6f8e26dc0e327fb9600028ce7','6ce3257fd6dd901f5a77e803cbedecb5','6e3abb221d5f6507d609a348f86d85fe','6e487d14c5c33049b35379227ec5f1b2','6ff15763865d2a21720092b6d652c8d2','704f7b286b8d4925187abbd9ba22e08d','71e3d2a5c89eff6baa3136e7487dfe9e','72e5c6d80c3d1026e1a3c4e132d2af4c','74262f517e1268e93e527a3323661318','742b5189aeb36aa12391eece04372336','75c1bf932eadd5fa008f966c304ece95','787091dcd4f86386935a35ce11d2714a','792d2ae515365e608a7a2ee3448989e4','7a3d0d41a4902835aedf78106538a8e7','7bab858ae2ce11dffd5155e385047bd0','7d371556513ac5b1cfba6793fbe4219c','7e645a1e19a2ae5eeb18e55f245cdaf1','7e8ada608880cbc033d981f0d65543d4','7f65c87e3432b97e043b417e34bb9aab','804ec09160bd8b6054931a10a1906e90','819fc81456596ca47e5069e0d0746841','81e17eec64f83b97cc0a94d79f597dbd','82942e2b560e7e842adcadd0813ca748','832c247dc6d961ab76dffbf06348c92c','849d1ea4cb88caa68e97f589cbc9cacd','88bd24dcc7ba8eb460e6c96d42513136','88cca922055e1e418713dfacd3ea8147','88eddfef37864193a5a29dac3b5e9b8c','8d39040970f19e77c865acda81c552d2','90a5ae9e0029bfcc4e169af46c7f2803','92626f6114a04f8bfa4a941be829669d','927fdd963285c429249f055f3f583149','933e66c7a92ce597d1c61213969885e6','9430e211a25113ba73d411b578723b5d','94b85efaf2f60d2b1cd2e2d57a373570','94cd7d9d7a9a5c459ea9d3578e69bc77','96095b74004b8a72993900412f5412cf','961d95a96955b80e14351b2c847a7c21','96e92a55eec60a7da795157e7db1bc2f','9993a757f20b184ab5b918961cce471c','9b1f6e5a9b2b65080ab1c03daaa62fca','9b637890bf9c11eb86162cea7f6c2bde','9f217b52793180bc16dbac3841983c23','a07300df528840337a32af016a76f7f5','a65ac4c774874ccc2c860b2d348fd35c','a6c7f64a91f99a778b1755db108d18db','a826fb0a5410377d75734d00517ccb40','a86351e135e612fc76a22e78e0c2ace6','a90b92f18b88f9463dd01cd32bc16d40','aa16cbb0e5af8510bddec68889932d47','ab78c309b21ac782069a1b14aa7ae4d7','acdbf03226e250a638a984ee5dfe8393','ad32e55d022c1fd33c573805c633d883','ad3690b0e17ca6e86498457553af0db9','b150993008a4c4f06d32c0bf0dd04be4','b3490040b5cd071274a3f557285012c9','b566a02c004d2af28cf430312afba327','b67247e5ceb72d28477d2012e5807e89','b73757b325b7d7cb263ca11b99e52dd6','ba57f0cc22390cc3ceada233e248e7ad','bb3addc1f7d1b7a7326b33edbdae4f5f','bece7dd2eaf68cf9b34bdc05a7d47d3a','c0a1cbdb8673c8cb1978b308917ed35b','c21adc236c9114335bbccd6e605ac9dc','c28d56887fb8b8b11e394cf4ad769a91','c354be6a5d84eb5fd23da89916814862','c3f4d846d097f4402ff425b4f1f38772','c40ddfdbf535905666f0bd0a0a6773f8','c49095d86fe37dc70946a0d9846b7d9c','c529d95f05e35a870f1b80e9264a7663','c7d4b09b5898752861dc4d34263c8434','c9f3200498d8805838007656f577d493','cbd943e6953fad33c9f6cf085e5883ca','cc7257de1672037e499be112012dd782','cdbe5b843602025b7fd7bc1d3ac26f86','cf61bbc7e4c60b872ee2871bd566a8da','cf65dcc2000cd1529ad00caddd489701','d0f8f1c49735731bd2da6e4f1c46c5fe','d1266d79088a1cb1c8792d55d4b5a15c','d266138899b586599f0e132b9d866c2e','d30a6bd6f99a57b4a0861513a56dd119','d3725ff373bc5cf16c6a26cc7e1c989c','d54a34973d43b5f358802e13d2af5968','d54b2d529cf841b0447264dff882784e','d7ca26781da24a687b7b38089308e472','d858d0101d77ba1446a5fb12c83094b0','d8900478acc29ecd0848bd83636c1eb5','d926f8675f46e59b63fefafa28fa3e4a','d95018319cef00e2178cbdb768a7acd5','daa039d6367e627e710151d6a0227e18','dd63a2ff42f11641a1c2068005112c86','ddf3ad32447714087f2e614629efd81c','de066d1e65c5fa354251434714b618d7','de0f9b14b216c86f218611da5f9fa22d','ded55832a12d1bc6c3a05717d4838a03','e2c723be33609b5c4745bee5eeab7062','e64e5fe873198ca8075239c688e900f2','e806bfa1ead75e8b52ba711850548456','e885558c312f03a2dc3aeea80a9d6222','ede851045cb7affdec133a4530325c38','eeee43f3c75f970a93827eaf4b6ce9f5','eef1df05c6b17249fefd3aaca80ac87e','ef2d5ce0784571e5ce6c416cc1752504','ef48d7bb1b7ce9cad6b9777b2543fb8d','f037882ebbab599239185b63372ecec9','f071113f597d052e99012aa6b464ebd6','f9497acd5978d4c8ea86e9694c044a4a','fa2d21521daee4ed1c79fbe10bf47295','fc94958ef947362dcb2788600a109d1c','fe9da230a6088b3840d325f5bf165ac5','fecf0c39e840c21d76ce36b2cda4af86','290b6a5d640c8861264ea7d0ce012bf7','f904ceeccc8f86e2edab12ac7ebde894','bfabdf52cff11cef5968bb72360d533e','e129e526c76d14a40aa8ca6b881084da','28748da6fd369e0222fdd4ecf6f0e4eb','00d807f408ce6f26e7adb7542e63fad8','02d4c527b1c1f7dbfb7583afeb6b9a03','0524ddf40f48d42daa61619ab3a96d83','1a9d6ad2e05ed768551b917ce0c1381e','25ef704e9395c0875ba4c679df90f8cb','26f2d84c83f47d01dd3a95a9aed6ad9a','29fa016eea9520eefc4d927607aed6e7','2d639e2136e6d2ec91ac4c04022fba38','31c33f4279ba752478a86710515a8397','3e6cda454f0bd0188ba874bc24779fe9','408cd486f08cd12487daa6de0f04b01c','62a50eff7cc5e0029e7364a85eda67fe','734fed399aaca017100e786678d64fbe','7a501b1e352c86154f27cec3db4ddd5a','7b00516aac7ccb5243ea7e2552bbf1b3','8245c2d1465a1226916f1b500a5038a1','8e5a65f32bdb612b151106ae1b771c2a','8f77844c6b0bffe692b716c24b7e1dc2','9439b5c3bf5ada7899d4869bd39c63be','9a31803ef7580fcb93e75b136435b820','9a68540d27608c8bea6dd95e158f9913','9d6427ccb7874de95c48b22dc0bf5056','cca8c483ef60608f37d43ec46ad080fc','cd1568809839b8e37722585716617a85','d02cb703495e22cf95f63d25b5b11f0a','e213ed1d3cd4531c7dd3d2da38f44c6c','e4869521fe152299be2e5aa0026c6393','ed902e1321ca8a96002a473b6f067d38','0605201182acfaffad63c7255bdfac36','0a44f7287ca0cc9ff0ac2c8585b54364','136722f490d8f91663cb7cabd67942a0','162371410be409e79fe42c2225bc2004','1e7f5320d40c7827cbb6e475a750691e','3800c472aefe52ec2fa708bc211b8cbd','48466a114da50f61c9a011b5e5f05cc0','4b5f34989d7f27daf9d042a40ccb6096','4beb7bcab0c3b7d867f5e50e19982e5d','8cd5b32cccef9607dfee1a24ed576344','8ed7780344cf72369108e6db0782bd2e','8f720f6b808797540cfd222eee29e7ff','913aa41c3eeb443a47b1618a0e3a9fd0','94e1881cd0e554f4d2964abbae1c163e','9c96ad6acd1889dd084653ed59bc213d','9ff255b515e797fca3049caf0caeddcd','aff0a4863600f5e0d649d7e026bdfd54','b5731a65a417d5364650ad21395f910f','c35c25c4428acd34c9ad2dc38a16c725','c7b624ff926c15834525339dc78c3890','cb6dddcad0f3dd9218a1e45772e3cf5a','df74fdb24b636059bea5cd75f55a379e','e40cae6a04e710c15dbb6ebb8c5b0929','f901e1d3de167fc39d132108b7bb8cb7','fc0e490425eece655ad9f679a159e561','00e784b05d492206d55fc21e067fb574','0faf6a32dc602dda7c9165d809773e79','523bf84df84ae33f1ec0237e5a9c5321','773b41d70ff638867c2500ab64a39ffe','d4a1fcfdbc6b5cfda54d9e7aedaafcee','da3b97da16ba3e8410696d80836c0d1f','e6600dc739d50c945bad9d48da33ff8b','ff56f8a8637ece838dff044db7c60a79','011cfd4b6908cef28385c3d3092e2088','020399d9e2d50b499c79f7fa4462b32d','0acf9ccaa5722b2e0cdd1a66bdd6e89e','11b88b6bebaa34d02377082fb537e6c1','11ce2d509f7cb4bf8c54f15ecd5e5f96','145c89b3c17ff0447af0abff274014cd','19fbfc7366ec77d5757b761c56dae160','1a23ac1ac1773d16bc30acb4ed05f0fa','267a5d07344ecdb09e473c66634453c7','2ff447485500fc5f7abb75b695cec34e','3038cbf8a2052fa5a784a3135c90c6cd','3631d0de0193a3b2c83d9ed014c8f6d0','3ad3b14267ea46c35ce956f9a8632142','3bc51debcb28402fd9b6ffac128857da','43db4ff457ab671024a407505b0e49a0','46b31beaa9ea3250536fdb2d45a68abc','496174cd5e6843a9e3db011d8edeb9ff','4ac5353b20f3eaad8e1614f0efef2ff1','4df357d49197ad7b8d58002b5bd09fb2','59c9d0e5f6e49321aaa67e94e4355372','613c63d21dc3530b19cfec1d759ba99f','6145cd21257308601ac3e7a69235103f','6858b964bf75af252b6edd6f59aff043','703ce0db56de49f10a46f3600c52bc05','70b1a17db93b8d049b90e08128907bae','71dcb2c763edb807197467b9e48ee65c','75cd3ca4d3359366781a0cd2e7851acc','7741d60f438a901e6063c73c8bcc9263','79f6f18c7de09c24cd43a950155d42fb','7a6d22265afce0ea9922e7bdc322ff2b','80137a9fae233ce01194cfbb2d7fe94e','829d1e15780f0bd6fd9c0227dea1864e','8301f608e246755e1f70b736519c884c','83ef4a3d0fdd7af62c41790896df1a5c','85f53c75b1fce6b226691a85e9e91cee','869922ce7f6cc607c752c5c6b8302400','890ce76edb22fbde582e2459d43fa32c','899070e434b099acce8e4d058955abe1','8bcf94330da3a8bda6d2f0bbc3ec33b3','8fe7071a701fdd55d6bc0e3f21f380fe','929e2477c8a56053831f67a8095b7f2d','9653857f4c1e80b129e193ae83b0f963','983c22fc57c08bae2d0beab126f93180','9a03843f94cc0de7fd0df0ef89000b1d','9b1f0ccda402691e06ed239b2d0d4e59','a402d47f8b3e7d6540c7942469a98357','a5b6e147207dc0028ebd01d4c88734dc','a6e3310337649f89811de4bb014e10cd','adbbf196bfeb8eb40dd09cafc54851f6','b216eacaa957a825ff688246bc45e99a','bb0a52c0fe793b860473e5db86355eab','c0558a00cf9ff77b5730cd3e6da27c25','cab1a93fb838d3e2ca35c5f0162c1ef7','d139dbdf1c8d7ffef83a6e0222a78432','d3e82281038d28212c0b8ea55f508102','e5b3b0d127946c68e5f630f33620b6f6','f04a4e73921a2755f0c30ebb9d0dbc04','f0da1b6c533eb0349b603952db6b9a38','f1d377c9be02e6aad806a5474ca0b331','f392589a1422add3bfa14ab49ef1c542','f6f67f4b2da824e42dce92175a7d5044','ad150205f268ed633a688f94e4fba1fc','0cea6498314849d78a892f48697623de','336583b6e50f468c649044116733139e','3be9ef85b5283d56dde4c63cca34df8a','54577b3684a3065fed495fd8f5a0c638','57dfd462a593ddad9eab471a236d3c89','585ea94a31550a19e8ef19915726b3b5','820f917f43d823f58c9cc8d97ce5ae22','8b8f9a76f410e4263bcd228bf4696693','9558a8105b10e5004e776a31fe299e02','a4067090b7f198849888c3e1a677726d','b3a8f98802a36823bfd7259f8abe24a2','c46bc449527bc711802f076ce9afb40e','dd8fdad0d02b47a87d2d81336650fc41','e0d07c878815b251ac98980a877c55d3','e93b60333817e4a13993b3e041fb7b79','ebf81d3d4a9ffe2dc4bfb0e19738d6f8','ee76b5fbcc358b7b2ca732c1f6983fb5','efdf0493ef07bd1f092644ddc79963d6','f5d170c458ed76e38fb1b4c18017e335','11f9a8f1b6430e7adbb65e268c1489ad','be8f8678783163f03c7ebf5c380b888f','7b884115f628add91baa5eb8f4495e1b','04931dca1bb6cca2eee229816adc7da4','0ed6454127d780971e5ee099252cfa39','180c065fa2e600db116c99bc2615b078','207c024efb6b336b961c13168ab118c8','243ce993affd99e787b755a7ac55f188','243df7029895ea210685911d5c0638f7','2503103b79f45e6a22a229e8618b0eb0','2dded5c3b628b207bb03304177196af7','2fa1f238ccf8c2c66619e877ebc5e92b','39b65cced9e459167a99cfbd93a925b7','3a6b99357d38394da5fc24bf6c93a4f0','3b8d0820690a192188baa63739b92cea','434ca179489ed6614687dafbac4ce6f8','54a6d5613eacd98ef6c08c4649090b0a','6099372e1e0e36df9c1f4c0a87da4a40','6166175b9196565165e85dd279981602','640c4139f113aad810393344e1316ad8','67f1a356272b7df1d1dc93a7b1b91884','6956744f5257c86086394941aff973ed','6e5fc8f03d26e01ed4126cb310aab48c','722b5fca7f14b9a37f8579f05799cb15','734b707292005e2a6b1095817ce19aa9','7704f14f1d644f00b67f64380c827555','7a48904922f08cbc0dd354528ed33217','82b9588b35c2018d458c0a8280183cbc','86ccdf1d5da1b3bc44acaa53c74cb8b3','8a93f788aacca89446896157f9721e57','91e51694e14b19d9f2ad43b84d708079','954f521aa3333ca4f5115586b626ba2c','96307dea92e3af7065fe1ff62fe68f23','97165932b1b9407161562533cc901ae4','9a79a25944fa7316810d2f7c76bd6583','9b5deff8bf9c11eb86162cea7f6c2bde','ad3823541e869dd3711f41be416b3fe9','ae1434d909b36d811833e7d6c70b539b','b9ade453d9a93e7fb21372281e2f474a','bca87eda56a847e41c76eb6811f209fd','c0438ea0882bb7bc9e41a6f3b070af58','c1aa4f8ef82ff05c083182c9ff039347','c47b9db5e6a9a3bdab863ce0638e59e8','cc59faca664bd4beeeb869e3c9b0388e','d02ea8de3cbc243c9f856a978369191c','d4f20b772c5ecf9eb8015010cd6016fa','d592465db6b864d358e62c667560f033','da1e2db12e420602506f11cbc26c49cf','e40ad94cf15a6c4d6f41860612e0c4e5','e559b3cd8a7bbcb61a59bea6b4232101','e5d6f901bf0761707fb8d527a50e226f','e9185da08ed7e0da5dcdf5b58e74bf4f','eefb4f4df9397b21345592cc254e7b4e','efd1e9939bc6a6b3a2f5a0af29a19f40','f070f627f33e6f11429ca1a8fc006790','f6eb9cf857c6014a1c0b32fb1a1ff999','fa0d1b3059d8587ca9b0a658dfb77afa','6f5aacc0958e6665cdce1ec67a504990','7141c73c4f4021ca2ae7f9f5b7510722','d963421ea7f91aadd935c872efce930c','58b825e0efc231edef308e425cc80f95','e4ff7c4104b27fcb939fa381f4646b38','cfc34224a7781df34426a06631c943f5','ee2a680a9566489634f18bbb5f9828a9','8863a818ff3b7b4cbe092439542652da','967727c0a347e27ad623fe9de414c1b8','039974737cbe31cea920c0668419f128','09a32ab9f8452535752bf16048f2ea42','0fd7311f747831f895cbb0a286db399a','2036ff65696ff96c9f159e3c176fa02a','3fb26cd05ba704f834a2d3e6a00e87ea','4cbb3136c5cd959605892b8b191cdc39','529eec28a0474bfb21ae33af3c3f4411','56ed1027ac54b364f54fc778d0444b3e','7282ef488121f2c1a20e93d78afe1bdc','765b34760f763b2d9a1f6cb7a9668193','77ebf0496fbb570e1a4e30e359620a4f','78e8cb2837907fc4fd28be1059c6642d','8190c4b7ca11bf501a957b576bb41050','86f68938d849de3f28a8b600691a8a11','9ab1c84bbf9c11eb86162cea7f6c2bde','b6b332eff9620d88b2982ec857743b88','c2a5f63ffecc345bf7f6a9c1ef48146c','d12a1bdb6a83ea05af2e7455eda129b9','d7eaa7e7674520aaea4602f92691a7d4','d8f1d0adf3d6e56cf772907b10815946','dda94f9cbab9553b42a968bbf973b158','e1f4f7bef107bda6cb94046a3669cf2d','e7f75056d0b18f0b4cf8711d4c4f742b','e7fcd8dae35e49759e0fe475a66487ad','eb89e678cd070a21aee6896784cc6275','f3d8c8a472f42b5c90fd5d9715b55d58','0f5be05e094d201536bd4b1853be95a4','1590de569dc2df19ec2c451084e65a35','2e778a0e1c890267a687527220588896','37ef0cea41f64642e36f4c10a9e25047','404f2025706f1368438f5e839279e857','65a02d78c76b2272dce65d21e6e82d82','957f6bc4c9b06d7a43e44f6b2ec58dbe','b50e27d40364da35f672e175044424c3','d051b750d38eb8b1370e9a22cd1b29ff','2b5fefacc479d1088ff475ca66c5ee01','00a6c1f2be85bafa98c2217c1080af40','00fe73961de860e70c63c2b910ec33bb','07b074d4e20320605a689a0c4b4607ae','0866b056ca5d2403644fa02e5749f7f6','08fb30b1887b0a83a70b31eb1391e709','0ac8f07a42e62a0762e2aa80dc08a9cc','0c20f0b2b17ad19ac95d1064a0e88746','0e2edac2230132e59a99ad91fae5db89','110275ae9f81da5253a7b7d2a6dcadfc','114afa104a4ce64c4f165c87b0f17503','117868d73154e8edbed3b29ad773ba9a','11f65c232256f2d1dc3ed7e490317d07','120510e9ba9a388ecbb1bc080f24b8f1','12e7ca806686a85ee409fb487d869005','134a4280c15913017c2223152b3b1b56','134f862b12a9a2898af13c294fa13da5','13620bfe063b894ceff8b06a70e89458','a60428fff43fe8001a886b62d2f4c691','1390efcb4aca44cd4dd5ee03b811c1fe','18d655fa470f4d85bd9073fd258e745c','1a0134007bff8219c016cc88a2624268','1b2545c231cfc4ec9b70f7fa5999a430','1c8f47fb53b792645997474a858e0a34','1d1eaa25b1700391d2a362969a04dfa7','1d532de9eada00bf033997f039bd7566','1f05279db7a815319e815af863182e0e','20f2071207ab7c4b51390965d3144abb','211db14ba9b87768646a92b7a4973fb4','2484c71211c33a7e6eb49fca7d777634','2572775ad3ab7eb9de80375d2ed433f7','25ef3cf9779e8703e9e5b414a3a40d05','26757a67373d00eb8ff1e3ca47f6cc52','281e12caee649ba344bed06d9dfb1950','2952e1e2b45d16e7bf6c5cfda3e66f18','2ac6caf538512fa0a01288e907ba39b2','2ba509b5f6f662e4fb85f0d29de611e1','2d923d13a4565df8321a1883d5a59309','2e1bb56a8904d1d27371517e163b061c','2ea1d40266c99e3a6c2766d3ec25dfb0','2f66e9281f88a976f89c9c9e8044d85f','340ce6d8c57104c273ca51dd32daa89b','35eaf25f0bdf883e6ae03fcdbd57eba2','36a209c46810b0624f2a02c8bbea2086','37cbb74c800e464794baefa02d75021b','382f92e1cf72a8b459ea7c44b51a41f0','399ca2e36817b2caa3a8c1c5a9a53375','39b6037c5b513a2dfdb6dd77fc3487ed','3aa4f77d4c1b019583cae663da1b95bf','3deb2cfa3a6a5e38ef3abc77fc99bfc8','3f5c39c76e50ea9007fcabe6fc107ecf','416ad52e11a0b2c030e5e7ed9d52fbad','46be35cdf94bad2ef4e31b14165f0f16','489f64beb02546c1e0d19517d04c8088','48a453bc941557c3788b49b920fe370a','490a2f898329f0b353c2699bf1fd82f1','4aca9b496bd10b83d9f3ea9bae2a581e','4b00003e3db6f726b2421cc8b38749ff','4b088afcdaf5f78e2c41fa5796aa11dd','4c53dac5dd179cf60ef5a91d5a958687','53cc942d034f8d475ed0a2ae15cdbf15','54423a19be60e9731c43e492eebb2a78','547a406ad864610e991b9d3ab04656e2','566684d46e97593f8fefa057bd12002a','5667be3b8500527ebed3efe0335b9331','569868f9d9f5f281ebf66ba8d874ebe2','56a249904afb81401be8b2cc14f04c46','582b0ec6d7a10e6f882fa13e69a20809','58d2512e2d3120266bffb467d34919b8','58ea684c5d23f6e16f9ca9b3755d15d8','5adca92240304a1e6dcbb995686352f2','5c7998efef7f7b599be4ca8607d5466b','5e22c2afce2aa85d23fe257965760243','5eec6b05c4f6bb6015c969f52a019f90','5ff3ea9ad21b8bb4b8ed29fd11760101','60e7e51bc6c2718c61200a061631402f','613f906c3e32c0859e1b2350168c81ef','67114d410716a3639d07e119c9075386','673209f69cccf29456033823443cd5f5','6807f3cdc020f7ddf783a0753fbe4ae6','686cdfb26375378b6071cb2ccf3b7d90','689f76a106f9e0c46a7b72e2bb1234d4','693010bf68e6ede466d0a8fd78241d00','7200bceab9329b802527896aa0e3838e','72f581ff8bfc3154c8e46052ef880775','74e4320a042cf4b62a6b53c0186e715a','76c336aefde19a850437d40ee6e593d5','79c71b790fc71d6bed9f21eef09882fd','7a55bf9e8605ec1760940ca11a4549ab','7b4cd4e549bb2413ac2c769cc5b5ddef','7c04ab563c85d4a068cf6a6c8d67ea74','7c1a87e30c73437f6273772c886a29f7','7d6afec500ae33dc305b1e396feb4527','7dd3c9f25ab886d4478fb5a56d1e955a','7e08e66ad28e809ab88e2efc4a57d880','80c7300b463880b4a7de326d0bb01367','81737a63328efed3358de79d4192562c','81bee911e07ae19c457b61bb1b0dacd9','820da043350107ccb5e2ef0218122bdd','83d4f3584fa62df5d18f4fc46b406ac3','840072253df65cf6c6b2b5b8a052df98','84edabc218bc52c782738e89f10166d1','866d493ee6efcd4457a57623299f62df','8717788ad5c8cc6f7a0fb76dcbf5b32c','8b1f29391e1b9cd4201cf5e5c86170b4','8c1f1505f90cbd2be5d0e1d12ca1a852','8c9cf30229d3e8052d1148e04f074287','8ce3d384a7ec53b10df95703417ffc7d','8febe78facef5e5a04efbed2a9924e2d','91475a9b38202af3f6a0054fe6713638','91e0f511d1931b0f946addc4db5ccb5f','95d15acc9375133d72222f90d3d01aa4','96c0de322bdbe014e161305cfb4477a2','9721ee94997488968a271d3085d54198','97a2ca58e61a4c4206213cfc1b607021','998b8204cadc8bb3000a66c1630f40f9','9b317207b7475748c00cfc0b70946595','9cd12ec4fb39065ed823a60a0c786408','9e03cdad63db532cd77784bf83246c30','9f2eb95cd577baf73cc742b779be92ad','a1ffac492b2fd364e094fbe22d05d099','a22eee2f368321251b47b66c47b59666','a26bc46d11cd68db47830406535ca7bb','a3de7aa255e8bb8339135177a1a83e2d','a4eb15fe7b68d7ccbaa1abf12864eea1','a599fb3786ab5ecbd12f1ca8d58eb577','a80035f7367aca921028dc7c12d5b8b2','a92e3dccb7d614147bad49b978109f23','aa7979a5715a27984f7a18033e8d8935','ad71599f8a30096f07e9001e2db15b63','ada81e663f847d205fff3ad6a8e43a8c','af197a1fb3e021f7deb6628d65fb95f7','af626ad69b19d7f3a94d8cbe5cf88f19','affdd34c54044df4b6280c5c6df87abd','b093e6e02c4a14cf86f4e6c15ab227ae','b096be6f721baa65e93a40b68c49625a','b47224063ed2bced736686a3a6a07ad9','b5b5dbeb1e8f2ad49777df76688c45fe','b805d00e6520af57286e9f86e075cd59','b96c4ba6b023af352a04624616725971','bc060cbf1edf1e28c0d50ade3a39d8da','bcb9ef0a8a608f7c0408925aedc0afd0','bd61ac45396739b7f26e68ea30f1ccca','bd78da5469fd5f802d1860dfd34325bf','bdd11c889497438228be9cee1a5d716b','c1129733d474fe620796bc1b2791a162','c3226fccfdd8d0f27579ad955dcbb3f0','c3650d4ab8df76ff057503e5154d90ae','c8811e8b8f69b5f3464299959f18b17a','c987200e31c48cf0eb932ae99aaf753e','c9a63c5fb10b424802f4d18f771df138','cb812af832920538426783a913f37c0f','cc221bd062abdf1673fea3094a5b7e47','cc82302e07fb3e9d1021b4f992f7ef09','cf008df3343e5d681a9a66007665daf2','d0d80a9a1507a76d73d8eb389845a866','d14c89861757141e17c8760c07a1a156','d20597a701eebd5572df53770f16e50a','d2f10ed3322b79c31f562d90602d19cc','d41f52f1aedb453728daeb5d38a6dd46','d4bc5f31c03a7e5b2d0d2595ab5cda78','d5adf70597a1c3e48ef33660916eda89','d726e82d50f0f8df42de24032ac5fb5f','d7299a3d573989a80c283ea86be529c1','d75bb472a13f918d269d9018eb8e0287','d793a45fa0437d5a342bab0a4932fe40','d8af9b90b831341487de3f3158c8bea7','dd98ce4e4904b8cdd826d6ba39e8787b','defd50a1aadf4fb5f205c4f7c4851be6','df1f0ac354861aa13970be8480728dde','dfc23c1a93f431956c8242f7e25b7af5','dfd247038a0217db9bd341c8599fb3f6','dff9cb490ddcf5a89cff229dec667c19','e0d8de89a555a1c3d3d8521de6dafc5d','e258621654dcb33ccdcdc0e6f4374db2','e8cb4103b22aa4df04625bf793b56226','ea765a03b4006bad7fe91bfcba133efd','ebb10073a1c90835ba176a4899cb0a3f','ebc6e05867914247ffcfca5050b14403','ed9c4452ceb367f4b2eec8b55ffc71ac','eefddd064422f7557a9fc66c359ca00d','f07c8d60dac3b8b5af1bb453c01e4757','f25187259277507f74692680a47d3cc3','f31f0919d92e954e8d0ccc8a732657fa','f438015ee07269f34280e49b9bcf51f8','f56e7ca49b1cdbde213a39b0d46384f8','f7888fdd9154104656ac50010e8ff18d','f7a8dbbca572d088c06028c470fbedb5','f845d8cd45289c58680fc318148dafb9','fcd4dcdae317ca3246ff1df114c44fba','fd978580bd959380694b0ab53ee21cb7','fdc8ddef9e983b01f83062b6fae0182f','ff89b2b7e93dfbf97d5f49cd4dc70e0b','7f42079a63d23fce96489add762382b8','e7961539e683f6403b35a5f1813c8857','6e78fae26e4c87fcd881fb2ec36ff434','a643bc2b9fcc56bffa5d3b1cc168c7cb') group by newest_id ;
-- 00a6c1f2be85bafa98c2217c1080af40
-- 00d807f408ce6f26e7adb7542e63fad8
-- 00e784b05d492206d55fc21e067fb574
-- 00fe73961de860e70c63c2b910ec33bb
-- 0100d812772f2eb93c2c170384bb8f2c
-- 011b364f4f6ec92daa1cbe09f85e484a
-- 011cfd4b6908cef28385c3d3092e2088
-- 012a4712f145be4ecc1f02b04839283d
-- 020399d9e2d50b499c79f7fa4462b32d
update dwb_db.dwb_tag_purchase_poi_info set dr = 1 where newest_id in ('00a6c1f2be85bafa98c2217c1080af40','00d807f408ce6f26e7adb7542e63fad8','00e784b05d492206d55fc21e067fb574','00fe73961de860e70c63c2b910ec33bb','0100d812772f2eb93c2c170384bb8f2c','011b364f4f6ec92daa1cbe09f85e484a','011cfd4b6908cef28385c3d3092e2088','012a4712f145be4ecc1f02b04839283d','020399d9e2d50b499c79f7fa4462b32d');
select newest_id from dwb_db.dwb_tag_purchase_poi_info where newest_id not in (select newest_id from dws_db_prd.dws_newest_period_admit dnpa where dr = 0 group by newest_id) group by newest_id ;
update odsdb.ori_newest_period_admit_info set dr = 1 ;
select newest_id from dwb_db.dwb_tag_purchase_poi_info where newest_id in (select newest_id from odsdb.ori_newest_period_admit_info where dr = 0 group by newest_id) group by newest_id ;
select newest_id from dws_db_prd.dws_newest_period_admit dnpa group by newest_id ;
update  odsdb.ori_newest_period_admit_info a , (select newest_id from dwb_db.dwb_tag_purchase_poi_info where newest_id in (select newest_id from odsdb.ori_newest_period_admit_info where dr = 0 group by newest_id) group by newest_id) b set a.dr = 1 where a.newest_id = b.newest_id ;





-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
select newest_id,poi_index,poi_lnglat,poi_name,poi_type from dwb_db.dwb_tag_purchase_poi_info group by newest_id,poi_index,poi_lnglat,poi_name,poi_type;

select newest_name,newest_lnglat,poi_address,poi_index,poi_lnglat,poi_name,poi_type from odsdb.ori_newest_poi_info group by newest_name,newest_lnglat,poi_address,poi_index,poi_lnglat,poi_name,poi_type;

truncate table dwb_db.dwb_tag_purchase_poi_info;

update odsdb.ori_newest_period_admit_info set dr = 1 where id>=98994 and id <= 99433; 







-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
truncate dws_db_prd.dws_customer_cre;                            
truncate dws_db_prd.dws_customer_month;                          
truncate dws_db_prd.dws_customer_sum;                            
truncate dws_db_prd.dws_customer_week;                           
truncate dws_db_prd.dws_newest_investment_pop_rownumber_quarter; 
truncate dws_db_prd.dws_newest_popularity_rownumber_quarter; 
truncate dwb_db.dwb_supply; 
truncate dwb_db.dwb_issue_supply_county; 
truncate dwb_db.dwb_newest_city_customer_num; 
truncate dwb_db.dwb_newest_county_customer_num ; 


                               

insert into dws_db_prd.dws_customer_cre(city_id,newest_id,exist,imei_num,period)
select b.city_id,a.* from 
  (select newest_id,'增量' exist,increase imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter 
  union all 
  select newest_id,'存量' exist,retained imei_num,period from dwb_db.dwb_newest_customer_info where dr=0 and period =quarter) a
left join 
  dwb_db.a_dwb_newest_info b on a.newest_id=b.newest_id ;


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------




CREATE TABLE temp_db.tmp_lost_county_lnglat (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`newest_id` varchar(255) COMMENT '楼盘id',
	`city_id` varchar(255) COMMENT '城市id',
	`county_id` varchar(255) COMMENT '区县id',
  PRIMARY KEY (`id`),
  KEY `idx_lost_county_lnglat_newest_id` (`newest_id`) USING BTREE COMMENT '楼盘id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='丢失区县和经纬的楼盘临时表';

truncate table temp_db.tmp_lost_county_lnglat;



update dwb_db.a_dwb_newest_info a ,dws_db_prd.dws_newest_info b set a.city_id=b.city_id where a.newest_id = b.newest_id ;
update dwb_db.a_dwb_newest_info a ,dws_db_prd.dws_newest_info b set a.county_id =b.county_id where a.newest_id = b.newest_id ;
update dwb_db.a_dwb_newest_info a ,dws_db_prd.dws_newest_info b set a.gd_lng =b.lng where a.newest_id = b.newest_id ;
update dwb_db.a_dwb_newest_info a ,dws_db_prd.dws_newest_info b set a.gd_lat =b.lat where a.newest_id = b.newest_id ;


select city_id,city_name,county_id,county_name from dwb_db.dwb_dim_geography_55city where dr = 0 group by city_id,city_name,county_id,county_name;



truncate table  dwb_db.dwb_dim_geography_55city;
insert into dwb_db.dwb_dim_geography_55city (province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,dr,create_time,update_time)
select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,0,now(),now() from dws_db.dim_geography where city_name in ('南昌市','重庆市','天津市','常州市','绍兴市','郑州市','咸阳市','成都市','南通市','石家庄市','深圳市','北京市','三亚市','杭州市','贵阳市','西安市','武汉市','济南市','合肥市','肇庆市','青岛市','惠州市','长春市','珠海市','扬州市','广州市','南京市','沈阳市','徐州市','苏州市','上海市','保定市','宁波市','湖州市','赣州市','烟台市','济宁市','汕头市','昆明市','宝鸡市','佛山市','福州市','海口市','嘉兴市','九江市','丽水市','南宁市','厦门市','唐山市','温州市','无锡市','长沙市','淄博市') and grade = 4
union all 
select province_id,province_name,city_id,city_name,region_id,region_name,city_level_desc,0,now(),now() from dws_db.dim_geography where city_name in ('东莞市','中山市') and grade = 3;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------


CREATE TABLE odsdb.newest_period_admit_info (
  `id` int(11) NOT NULL auto_increment COMMENT '自增id',
  `ods_id`text DEFAULT NULL COMMENT '源表id',
  `ods_table_name` text DEFAULT NULL COMMENT '源表名称',
  `city_id`text DEFAULT NULL COMMENT '城市',
  `newest_id` varchar(255) NOT null COMMENT '楼盘id',
  `newest_name` text DEFAULT NULL COMMENT '楼盘名称',
  `address` text DEFAULT NULL COMMENT '楼盘地址',
  `lnglat` text DEFAULT NULL COMMENT '楼盘经纬度',
  `poi_num` text DEFAULT NULL COMMENT '现有配套数量',
  `dr` int(2) DEFAULT NULL COMMENT '有效标识',
  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_period_admit_info_newest_id` (`newest_id`) USING BTREE COMMENT '楼盘id'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;



select newest_id,count(1) poi_num from dws_db.dws_tag_purchase_poi group by newest_id having count(1) = 1 ;


update odsdb.newest_period_admit_info set dr = 1 where id <= '31856';

update odsdb.ori_newest_period_admit_info set dr = 1 where id <= '32057';



SHOW processlist;

kill 2450100;

SELECT
	TABLE_NAME,
	CREATE_TIME,
	UPDATE_TIME
FROM
	INFORMATION_SCHEMA.TABLES
WHERE
	TABLE_SCHEMA = 'dws_db'
AND TABLE_NAME LIKE  '%cre%';
	


-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 2020Q1季度取消
update dws_db_prd.dws_supply set dr = 1 where quarter = '2021Q1';




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 区县季度相加是否等于城市
select cityid city_id ,city_name ,value,period from dws_db_prd.dws_supply where dr = 0 and period = '2020Q3' and city_name = county_name and city_name not in ('中山市','东莞市');

select cityid city_id ,city_name ,sum(value),period from dws_db_prd.dws_supply where dr = 0 and period = '2020Q3' and city_name != county_name group by cityid ,city_name ,period;

select * from 
    (select cityid city_id ,city_name ,value,period from dws_db_prd.dws_supply where dr = 0 and period = '2020Q3' and city_name = county_name and city_name not in ('中山市','东莞市')) t1
left join 
    (select cityid city_id ,city_name ,sum(value) value,period from dws_db_prd.dws_supply where dr = 0 and period = '2020Q3' and city_name != county_name group by cityid ,city_name ,period) t2
on t1.city_id = t2.city_id where t1.value!=t2.value;


select distinct newest_id,imei from dwb_db.a_dwb_customer_browse_log  limit 10;

select * from 
  (select cityid,city_name ,sum(value) value from dws_db_prd.dws_supply where city_id != cityid and period_index =1 and dr = 0 and period = '2020Q2'  group by cityid,city_name) t1
left join 
  (select cityid,city_name,value from dws_db_prd.dws_supply  where city_id = cityid and dr = 0 and period = '2020Q2') t2
on t1.cityid=t2.cityid where t1.value!=t2.value;


select * from 
  (select city_id,county_name ,sum(follow_people_num) follow_people_num from dws_db_prd.dws_supply where city_id != cityid and period_index =1 and dr = 0 and period = '2020Q4'  group by city_id,county_name) t1
left join 
  (select county_id,follow from dws_db_prd.dws_newest_city_qua where county_id is not null and dr = 0 and period = '2020Q4') t2
on t1.city_id = t2.county_id where t1.follow_people_num<t2.follow;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 北上广深 月度是否等于城市的


select cityid city_id ,city_name ,city_id county_id ,county_name ,value,period,quarter from dws_db_prd.dws_supply where dr = 0 and quarter = '2020Q3' and period != quarter ;

select cityid city_id ,city_name ,city_id county_id ,county_name ,value,period,quarter from dws_db_prd.dws_supply where dr = 0 and period = '2020Q3' and city_name in ('北京市','上海市','深圳市','广州市');

select * from 
    (select cityid city_id ,city_name ,city_id county_id ,county_name ,sum(value) value,quarter from dws_db_prd.dws_supply where dr = 0 and quarter = '2020Q4' and period != quarter 
    group by cityid ,city_name ,city_id ,county_name ,quarter) t1
left join 
    (select cityid city_id ,city_name ,city_id county_id ,county_name ,value,quarter from dws_db_prd.dws_supply where dr = 0 and period = '2020Q4' and city_name in ('北京市','上海市','深圳市','广州市')
    ) t2
on t1.city_id = t2.city_id and t1.county_id = t2.county_id where t1.value!=t2.value;










-- 310000	上海市	310106	静安区	1017.0	2020Q3	310000	上海市	310106	静安区	2017	2020Q3
-- 310000	上海市	310115	浦东新区	11293.0	2020Q3	310000	上海市	310115	浦东新区	11295	2020Q3
 
delete from dws_db_prd.dws_supply where period != quarter;

insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,quarter from dwb_db.dwb_issue_supply_county where dr=0 and quarter = '2020Q4' and period != quarter and city_id = '310000';

insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,quarter from dwb_db.dwb_issue_supply_county where dr=0 and quarter = '2021Q2' and period != quarter;




insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) 
select city_name, county_name, county_id city_id, period, supply_value value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '2' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, intention follow_people_num, city_id cityid,'2021Q2' quarter from dwb_db.dwb_issue_supply_county where period = '2021Q2' and dr = 0 
union all 
select  city_name, city_name county_name, city_id city_id, period, sum(supply_value) value, null local_issue_value , null local_room_sum_value, null cric_value, null value_from_index , null county_name_merge, '1' city_county_index, '1' period_index, now() update_time, 0 dr, now() create_time, null follow_people_num, city_id cityid,'2021Q2' quarter from dwb_db.dwb_issue_supply_county  where period = '2021Q2' and period=quarter and dr = 0  group by city_name,city_id,period;




-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

insert into dws_db_prd.dws_supply(city_name,county_name,city_id,period,value,local_issue_value,local_room_sum_value,cric_value,value_from_index,county_name_merge,city_county_index,period_index,update_time,dr,create_time,follow_people_num,cityid,quarter) select city_name,city_name county_name,city_id,period,supply_num,null local_issue_value,null local_room_sum_value,cric_supply_num cric_value, num_index,null county_name_merge,1 city_county_index,1 period_index,update_time,dr,create_time,null follow_people_num,city_id cityid,period from dwb_db.dwb_issue_supply_city where dr=0 and city_name in ('中山市','东莞市') and period in ('2020Q3','2020Q4','2021Q1','2021Q2');





-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- dws_db.....cre清除数据
truncate table dws_db.dws_customer_cre;
insert into dws_db.dws_customer_cre select * from temp_db.dws_customer_cre; 


-- dws_newest_issue_code 导入
truncate table dws_db_prd.dws_newest_issue_code;
insert into dws_db_prd.dws_newest_issue_code (city_id,city_name,county_id,county_name,newest_id,newest_name,address,business,issue_code,issue_date,issue_quarter,issue_month,issue_area,supply_num,housing_id,dr,create_time,update_time)
select b.city_id,b.city_name,b.region_id county_id,b.region_name county_name,b.uuid newest_id,b.newest_name,b.address,b.developer business,a.issue_number issue_code,a.issue_date,
       a.issue_quarter,a.issue_month,a.issue_area,a.room_sum supply_num,a.housing_id,0 dr ,now() create_time ,now() update_time from 
  (select housing_id,issue_number,issue_date,case when issue_date is not null then date_format(issue_date,'%Y%m') else issue_date end issue_month,case when issue_date is not null then concat(substr(issue_date,1,4) ,'Q',QUARTER(issue_date)) else issue_date end  issue_quarter,room_sum,issue_area from dwb_db.dim_housing_issue where is_del = 0) a
left join 
  (select t1.*,t2.region_name from  (select id,uuid,newest_name,city_id,city_name,region_id,address,developer from dwb_db.dim_housing) t1 left join (select city_id ,region_id ,region_name  from dws_db.dim_geography dg where grade = 4 group by city_id ,region_id ,region_name ) t2 on t1.city_id = t2.city_id and t1.region_id=t2.region_id) b
on a.housing_id = b.id 
where b.uuid is not null and city_id is not null 
group by b.city_id,b.city_name,b.region_id,b.region_name,b.uuid ,b.newest_name,b.address,b.developer,a.issue_number,a.issue_date,a.issue_quarter,a.issue_month,a.issue_area,a.room_sum,a.housing_id ;

-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- 爬虫poi
show create table odsdb.ori_newest_period_admit_info ;
CREATE TABLE odsdb.ori_newest_poi_info (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`newest_name` varchar(255)  COMMENT '楼盘名',
	`newest_address` text  COMMENT '楼盘地址',
	`newest_lnglat` text  COMMENT '楼盘经纬度',
	`poi_type` text  COMMENT '配套类型',
	`poi_index` text  COMMENT '配套标签',
	`poi_name` text  COMMENT '配套名称',
	`poi_lnglat` text  COMMENT '配套经纬度',
	`poi_pro_name` text  COMMENT '省名称',
	`poi_city_name` text  COMMENT '城市名称',
	`poi_county_name` text  COMMENT '区县名称',
	`poi_address` text  COMMENT '配套地址',
	`dr` int(2)  COMMENT '有效标识',
	`create_time` datetime  COMMENT '创建时间',
	`update_time` datetime  COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_period_admit_info_newest_name` (`newest_name`) USING BTREE COMMENT '楼盘名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='poi爬虫结果信息表';

update odsdb.ori_newest_poi_info set dr = 0 ;
update odsdb.ori_newest_poi_info set create_time = now() ;
update odsdb.ori_newest_poi_info set update_time = now() ;
update odsdb.ori_newest_poi_info set file_name = '20210906_latlng_rs-31657-31856' ;

update odsdb.ori_newest_poi_info set dr = 0 where create_time is null;
update odsdb.ori_newest_poi_info set update_time = now() where create_time is null;
update odsdb.ori_newest_poi_info set file_name = '20210907_latlng_rs-31857-32057' where create_time is null ;
update odsdb.ori_newest_poi_info set create_time = now() where create_time is null;


update odsdb.ori_newest_poi_info set dr = 0 where create_time is null;
update odsdb.ori_newest_poi_info set update_time = now() where create_time is null;
update odsdb.ori_newest_poi_info set file_name = '20210908_latlng_rs-32058-32257' where create_time is null ;
update odsdb.ori_newest_poi_info set create_time = now() where create_time is null;

update odsdb.ori_newest_poi_info set dr = 0 where create_time is null;
update odsdb.ori_newest_poi_info set update_time = now() where create_time is null;
update odsdb.ori_newest_poi_info set file_name = '20210908_latlng_rs-98994-99433' where create_time is null ;
update odsdb.ori_newest_poi_info set create_time = now() where create_time is null;


CREATE TABLE dwb_db.dwb_tag_purchase_poi_info (
    `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
	`newest_ods_id` varchar(8) COMMENT '楼盘源表id',
	`poi_ods_id` varchar(6) COMMENT 'poi源表id',
	`newest_id` varchar(255) COMMENT '楼盘id',
	`newest_lnglat` varchar(255) COMMENT '楼盘经纬度',
	`poi_type` varchar(255) COMMENT '配套类型',
	`poi_index` varchar(33) COMMENT '配套标签',
	`poi_name` varchar(255) COMMENT '配套名称',
	`poi_lnglat` varchar(255) COMMENT '配套经纬度',
	`pure_distance` varchar(33) COMMENT '直线距离',
	`dr` int(2) COMMENT '有效标识',
	`create_time` datetime COMMENT '创建时间',
	`update_time` datetime COMMENT '更新时间',
  PRIMARY KEY (`id`),
  KEY `idx_poi_info _newest_id` (`newest_id`) USING BTREE COMMENT '楼盘名称'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='poi信息表';



-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------
-- ------------------------------------------------------------ =========================== --------------------------------------------------------------------

update odsdb.city_newest_deal set business = '三亚鹰君置业有限公司' where business= '三亚君鹰置业有限公司';




