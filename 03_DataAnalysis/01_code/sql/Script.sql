CREATE TABLE dwb_db.dim_issue (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '自增id',
  `city_id` int(11) DEFAULT NULL COMMENT '市id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '市名',
  `county_id` int(11) DEFAULT NULL COMMENT '区id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '区名',
  `issue_s` varchar(40) NOT NULL COMMENT '证简称',
  `dr` int(2) NOT NULL DEFAULT 0 COMMENT '有效标识',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '建时',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更时',
  PRIMARY KEY (`id`),
  KEY `idx_dim_issue_issue_s` (`issue_s`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='预售证简称维度表';