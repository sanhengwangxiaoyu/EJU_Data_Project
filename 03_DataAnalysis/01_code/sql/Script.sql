CREATE TABLE dwb_db.dim_issue (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '����id',
  `city_id` int(11) DEFAULT NULL COMMENT '��id',
  `city_name` varchar(64) DEFAULT NULL COMMENT '����',
  `county_id` int(11) DEFAULT NULL COMMENT '��id',
  `county_name` varchar(64) DEFAULT NULL COMMENT '����',
  `issue_s` varchar(40) NOT NULL COMMENT '֤���',
  `dr` int(2) NOT NULL DEFAULT 0 COMMENT '��Ч��ʶ',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '��ʱ',
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '��ʱ',
  PRIMARY KEY (`id`),
  KEY `idx_dim_issue_issue_s` (`issue_s`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Ԥ��֤���ά�ȱ�';