-- MySQL dump 10.13  Distrib 5.7.28, for linux-glibc2.12 (x86_64)
--
-- Host: 127.0.0.1    Database: precision_marketing
-- ------------------------------------------------------
-- Server version	5.7.28-log

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;

--
-- Dumping routines for database 'precision_marketing'
--
/*!50003 DROP PROCEDURE IF EXISTS `create_test_data_fun` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE PROCEDURE `create_test_data_fun`( 
	IN create_date_in varchar(50), 
	IN account_id_in varchar(50),
	IN order_id_in varchar(50),
	IN offline_num_in varchar(50),
	IN call_num_in varchar(50), 
	IN connect_num_in varchar(50), 
	IN omits_num_in varchar(50), 
	IN effect_num_in varchar(50)
)
BEGIN
  
	DECLARE v_offline_second_num int unsigned DEFAULT 3600 * FLOOR(RAND() * (11-7) + 7); -- 上线总时长 1小时=3600 秒
	DECLARE v_offline_num int unsigned DEFAULT 0; -- 上线次数
	DECLARE v_offline_start_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR ); -- 上线开始时间
	DECLARE v_offline_end_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR );  -- 下线时间
	DECLARE v_offline_time_num int unsigned DEFAULT 0; 
	
	DECLARE v_call_num int unsigned DEFAULT 0; -- 拨打次数
	
	DECLARE v_connect_num int unsigned DEFAULT 0; -- 接通次数
	DECLARE v_connect_start_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR ); -- 接通开始时间
	DECLARE v_connect_end_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR );  -- 接通结束时间
	
	DECLARE v_omits_num int unsigned DEFAULT 0; -- 漏接次数
	DECLARE v_omits_call_log_id int unsigned DEFAULT 0; -- 漏接的拨打记录id
	
	DECLARE v_effect_num int unsigned DEFAULT 0; -- 有效次数
	DECLARE v_effect_start_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR ); -- 有效开始时间
	DECLARE v_effect_end_time varchar(50) DEFAULT DATE_ADD(create_date_in, INTERVAL 9 HOUR );  -- 有效结束时间
	DECLARE v_phone_id int unsigned DEFAULT 0; -- 号码id
	DECLARE v_call_log_id int unsigned DEFAULT 0; -- 拨打记录id
	DECLARE v_customer_archive_id int unsigned DEFAULT 0; -- 档案id
	DECLARE v_next_follow_datetime varchar(50) DEFAULT NULL;  -- 下次跟进时间
	DECLARE v_customer_surname varchar(500) DEFAULT '郭王李张赵刘陈杨吴黄朱孙胡吕高宋徐程林郑范何韩曹马许田冯杜周曾汪苏董方蔡梁石谢贾薛彭崔唐潘邓任史钱侯魏罗叶沈孟姚傅丁章萧蒋卢陆袁江晁谭邵欧孔俞尹廖阎洪夏雷葛文柳陶毛丘龚康蒲邢郝庞安裴折施游金邹汤虞严钟'; 
	DECLARE v_customer_name varchar(50) DEFAULT NULL; -- 客户姓名
	DECLARE v_customer_sex varchar(50) DEFAULT NULL;-- 客户性别
	
	-- 上下线记录 pm_account_offline_online 
	WHILE v_offline_num < offline_num_in DO
		SET v_offline_num = v_offline_num + 1;
		
		SET v_offline_start_time = v_offline_end_time;
		SET v_offline_time_num = FLOOR(RAND() * (v_offline_second_num - 3600) + 7);
		SET v_offline_end_time = DATE_ADD(v_offline_start_time, INTERVAL v_offline_time_num SECOND );
		SET v_offline_second_num = v_offline_second_num - v_offline_time_num;

		INSERT INTO `pm_account_offline_online` ( 
			`account_id`, 
			`order_id`, 
			`begin_datetime`, 
			`end_datetime`, 
			`create_by`, 
			`create_time`, 
			`update_by`, 
			`update_time`
		) VALUES ( 
			account_id_in, 
			order_id_in, 
			v_offline_start_time, 
			v_offline_end_time, 
			account_id_in, 
			create_date_in, 
			account_id_in, 
			create_date_in
		);
	END WHILE;

	-- 拨打记录-未接通 pm_call_log 是否接通1是2否  主动联系1是2否  挂断类型：1主叫挂断，2被叫挂断，3取消挂断，4下线挂断
	WHILE v_call_num < call_num_in - connect_num_in DO
		SET v_call_num = v_call_num + 1;
		
		INSERT INTO `pm_call_log` (
			`account_id`,
			`phone_id`,
			`order_id`,
			`connect_flag`,
			`active_connect_flag`,
			`closure_type`,
			`create_by`,
			`create_time`,
			`update_by`,
			`update_time`
		)
		VALUES
		(
			account_id_in,
			0,
			order_id_in,
			'2',
			'2',
			'1',
			account_id_in,
			create_date_in,
			account_id_in,
			create_date_in
		);
	END WHILE;

	-- 拨打记录-接通无效 pm_call_log 是否接通1是2否  主动联系1是2否  挂断类型：1主叫挂断，2被叫挂断，3取消挂断，4下线挂断
	WHILE v_connect_num < connect_num_in - effect_num_in DO
		SET v_connect_num = v_connect_num + 1;
		
		SET v_connect_start_time = v_connect_end_time;
		SET v_connect_end_time = DATE_ADD(v_connect_start_time, INTERVAL FLOOR(RAND() * (30 - 1) + 1) SECOND );
		
		INSERT INTO `pm_call_log` (
			`account_id`,
			`phone_id`,
			`order_id`,
			`begin_datetime`,
			`end_datetime`,
			`call_begin_datetime`,
			`call_file_url`,
			`connect_flag`,
			`active_connect_flag`,
			`closure_type`,
			`create_by`,
			`create_time`,
			`update_by`,
			`update_time`
		)
		VALUES
		(
			account_id_in,
			0,
			order_id_in,
			v_connect_start_time,
			v_connect_end_time,
			v_connect_start_time,
			'/profile/2022/04/21/_20220421132748A040.mp3',
			'1',
			'2',
			'1',
			account_id_in,
			create_date_in,
			account_id_in,
			create_date_in
		);
	END WHILE;
	
	-- 漏接记录 pm_call_omits_log
	WHILE v_omits_num < omits_num_in DO
		SET v_omits_num = v_omits_num + 1;
		SET v_omits_call_log_id = (SELECT id FROM pm_call_log WHERE create_time = create_date_in AND connect_flag = '1' AND del_flag = '0' LIMIT v_omits_num,1);
		
		INSERT INTO `pm_call_omits_log` ( 
			`account_id`, 
			`order_id`, 
			`call_log_id`, 
			`create_by`, 
			`create_time`, 
			`update_by`, 
			`update_time`
			)
		VALUES
		( 
			account_id_in, 
			order_id_in, 
			v_omits_call_log_id, 
			account_id_in, 
			create_date_in, 
			account_id_in, 
			create_date_in
		);
	END WHILE;

	-- 有效记录-接通有效 pm_call_log  否接通1是2否  主动联系1是2否  挂断类型：1主叫挂断，2被叫挂断，3取消挂断，4下线挂断
	WHILE v_effect_num < effect_num_in DO
		SET v_effect_num = v_effect_num + 1;
		
		SET v_phone_id = (SELECT min(phone_id) FROM pm_phone_pool WHERE phone_id NOT IN (SELECT phone_id FROM pm_call_log WHERE account_id = account_id_in AND del_flag = '0') AND del_flag = '0' LIMIT 1);
		
		
			SET v_effect_start_time = v_effect_end_time;
			SET v_effect_end_time = DATE_ADD(v_effect_start_time, INTERVAL FLOOR(RAND() * (60 * 5 - 30) + 30) SECOND );
			SET v_call_log_id = (SELECT MAX(id) FROM pm_call_log) + 1;
			
			INSERT INTO `pm_call_log` (
			`id`,
			`account_id`,
			`phone_id`,
			`order_id`,
			`begin_datetime`,
			`end_datetime`,
			`call_begin_datetime`,
			`call_file_url`,
			`connect_flag`,
			`active_connect_flag`,
			`closure_type`,
			`create_by`,
			`create_time`,
			`update_by`,
			`update_time`
		)
		VALUES
		(
			v_call_log_id,
			account_id_in,
			v_phone_id,
			order_id_in,
			v_effect_start_time,
			v_effect_end_time,
			v_effect_start_time,
			'/profile/2022/04/21/_20220421103722A016.mp3',
			'1',
			'2',
			'1',
			account_id_in,
			create_date_in,
			account_id_in,
			create_date_in
		);
		
		-- 有效客户 pm_customer_effect  认定方式0:系统认定1：用户认定
		INSERT INTO `pm_customer_effect` ( 
			`account_id`, 
			`phone_id`, 
			`order_id`, 
			`call_log_id`, 
			`affirm_type`, 
			`create_by`, 
			`create_time`, 
			`update_by`, 
			`update_time`
		)
		VALUES
		( 
			account_id_in, 
			v_phone_id, 
			order_id_in, 
			v_call_log_id, 
			FLOOR(RAND() * (2 - 0) + 0), 
			account_id_in, 
			create_date_in, 
			account_id_in, 
			create_date_in 
		);
		
		-- 客户跟进记录 pm_customer_follow
		IF FLOOR(RAND() * (2 - 0) + 0) = 0 THEN
			SET v_next_follow_datetime = DATE_ADD(create_date_in, INTERVAL FLOOR(RAND() * (4 - 1) + 1) DAY );
		END IF;
		
		INSERT INTO `pm_customer_follow` ( 
			`phone_id`, 
			`call_log_id`, 
			`account_id`, 
			`follow_datetime`, 
			`next_follow_datetime`,
			`create_by`, 
			`create_time`, 
			`update_by`, 
			`update_time`
			)
		VALUES
			( 
				v_phone_id, 
				v_call_log_id, 
				account_id_in, 
				v_effect_end_time, 
				v_next_follow_datetime,
				account_id_in, 
				create_date_in, 
				account_id_in, 
				create_date_in
			);
		SET v_next_follow_datetime = NULL;
		
		-- 客户档案子表 pm_customer_archive_detail
		SET v_customer_name = SUBSTRING(v_customer_surname, FLOOR(RAND() * (101 - 1) + 1),1);
		
		IF FLOOR(RAND() * (2 - 0) + 0) = 0 THEN
			SET v_customer_name = CONCAT(v_customer_name, '女士');
			SET v_customer_sex = '女';
		ELSE
			SET v_customer_name = CONCAT(v_customer_name,'先生');
			SET v_customer_sex = '男';
		END IF;
		
		SET v_customer_archive_id = 0;
		IF v_phone_id is NOT null THEN
			SET v_customer_archive_id = (SELECT id FROM pm_customer_archive WHERE phone_id = v_phone_id);
		END IF;
		
		INSERT INTO `pm_customer_archive_detail` (
			`customer_archive_id`,
			`order_id`,
			`account_id`,
			`customer_name`,
			`sex`,
			`create_by`,
			`create_time`,
			`update_by`,
			`update_time`
		)
		VALUES
			(
				v_customer_archive_id,
				order_id_in,
				account_id_in,
				v_customer_name,
				v_customer_sex,
				account_id_in,
				create_date_in,
				account_id_in,
				create_date_in
			);
	END WHILE;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `pm_account_daily_data_fun` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE PROCEDURE `pm_account_daily_data_fun`(IN statistics_date_in varchar(50))
BEGIN
	DELETE FROM pm_account_daily_data WHERE statistics_date = statistics_date_in;
	-- 呼叫数量
	INSERT INTO pm_account_daily_data(
		statistics_date, 
		account_id,
		order_id, 
		call_num
	)(
		SELECT
			statistics_date_in,
			account_id,
			order_id,
			count(1) call_num
		FROM
			pm_call_log 
		WHERE
			date( create_time ) = statistics_date_in AND 
			active_connect_flag = 2 
			AND del_flag = 0
		GROUP BY account_id,order_id,statistics_date_in
	);

	-- 接通数量
	UPDATE pm_account_daily_data a,(
		SELECT
			account_id,
			order_id,
			count(1) connect_num
		FROM
			pm_call_log 
		WHERE
			date( create_time) = statistics_date_in AND 
			active_connect_flag = 2 AND
			connect_flag = 1
			AND del_flag = 0
		GROUP BY account_id,order_id
	)b
	SET a.connect_num = b.connect_num
	WHERE 
		date( a.statistics_date) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
		
	-- 在线时长
	UPDATE pm_account_daily_data a,(
		SELECT
			account_id,
			order_id,
			SUM(TIMESTAMPDIFF(SECOND, begin_datetime, end_datetime)) online_time
		FROM
			pm_account_offline_online 
		WHERE
			date( create_time) = statistics_date_in
			AND del_flag = 0
		GROUP BY account_id,order_id
	)b
	SET a.online_time = b.online_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
	-- 有效通话时长
		UPDATE pm_account_daily_data a,(
			SELECT 
				aa.account_id,
				aa.order_id,
				SUM(TIMESTAMPDIFF(SECOND, bb.begin_datetime, bb.end_datetime)) connect_time
			FROM 
				pm_customer_effect aa
				LEFT JOIN pm_call_log bb ON aa.call_log_id = bb.id 
			WHERE
				date( aa.create_time ) = statistics_date_in
				AND bb.connect_flag = 1
				AND aa.del_flag = 0
			GROUP BY aa.account_id, aa.order_id
		)b
		SET a.connect_time = b.connect_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
	-- 平均有效通话时长
	UPDATE pm_account_daily_data a,(
			SELECT 
				aa.account_id,
				aa.order_id,
				SUM(TIMESTAMPDIFF(SECOND, bb.begin_datetime, bb.end_datetime))/count(1) avg_connect_time
			FROM 
				pm_customer_effect aa
				LEFT JOIN pm_call_log bb ON aa.call_log_id = bb.id 
			WHERE
				date( aa.create_time ) = statistics_date_in
				AND bb.connect_flag = 1
				AND aa.del_flag = 0
			GROUP BY aa.account_id, aa.order_id
		)b
		SET a.avg_connect_time = b.avg_connect_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
	-- 漏接数量
	UPDATE pm_account_daily_data a,(
		SELECT 
			aa.account_id,
			aa.order_id,
			COUNT(1) omits_num
		FROM 
			pm_call_omits_log aa
		WHERE
			date( aa.create_time ) = statistics_date_in
			AND aa.del_flag = 0
		GROUP BY aa.account_id, aa.order_id
	)b
	SET a.omits_num = b.omits_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
	-- 有效数量
	UPDATE pm_account_daily_data a,(
		SELECT 
			aa.account_id,
			aa.order_id,
			COUNT(1) effect_num
		FROM 
			pm_customer_effect aa
		WHERE
			date( aa.create_time ) = statistics_date_in
			AND aa.del_flag = 0
		GROUP BY aa.account_id, aa.order_id
	)b
	SET a.effect_num = b.effect_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
	-- 保留客户数量
	UPDATE pm_account_daily_data a,(
		SELECT 
			aa.account_id,
			aa.order_id,
			COUNT(1) save_num
		FROM 
			pm_customer_effect aa
		WHERE
			date( aa.create_time) = statistics_date_in AND
			aa.affirm_type = 1
			AND aa.del_flag = 0
		GROUP BY aa.account_id, aa.order_id
	)b
	SET a.save_num = b.save_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.account_id = b.account_id AND
		a.order_id = b.order_id;
		
		UPDATE pm_account_daily_data set call_num_show = call_num - (call_num - connect_num) * 0.5 WHERE call_num_show = 0;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `pm_order_daily_data_fun` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE PROCEDURE `pm_order_daily_data_fun`(IN statistics_date_in varchar(50))
BEGIN
		DELETE FROM pm_order_daily_data WHERE statistics_date = statistics_date_in;
		INSERT INTO pm_order_daily_data(
		statistics_date, 
		order_id, 
		call_num
	)(
		SELECT
			statistics_date_in,
			order_id,
			count(1) call_num
		FROM
			pm_call_log 
		WHERE
			date( create_time ) = statistics_date_in AND active_connect_flag = 2 
			AND del_flag = 0
		GROUP BY order_id,statistics_date_in
	);
	-- 呼叫数量call_num_show
	-- 接通数量connect_num
	UPDATE pm_order_daily_data a,(
		SELECT
			order_id,
			count(1) connect_num
		FROM
			pm_call_log 
		WHERE
			date( create_time ) = statistics_date_in AND 
			active_connect_flag = 2 AND
			connect_flag = 1 AND 
			del_flag = 0
		GROUP BY order_id
	)b
	SET a.connect_num = b.connect_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 在线时长online_time
	UPDATE pm_order_daily_data a,(
		SELECT
			order_id,
			SUM(TIMESTAMPDIFF(SECOND, begin_datetime, end_datetime)) online_time
		FROM
			pm_account_offline_online 
		WHERE
			date( create_time ) = statistics_date_in
			AND del_flag = 0
		GROUP BY order_id
	)b
	SET a.online_time = b.online_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
  -- 有效通话时长connect_time
	UPDATE pm_order_daily_data a,(
			SELECT 
				aa.order_id,
				SUM(TIMESTAMPDIFF(SECOND, bb.begin_datetime, bb.end_datetime)) connect_time
			FROM 
				pm_customer_effect aa
				LEFT JOIN pm_call_log bb ON aa.call_log_id = bb.id  
			WHERE
				date( aa.create_time ) = statistics_date_in
				AND bb.connect_flag = 1  
				AND aa.del_flag = 0
			GROUP BY aa.order_id
		)b
		SET a.connect_time = b.connect_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 平均有效通话时长avg_connect_time
	UPDATE pm_order_daily_data a,(
			SELECT 
				aa.order_id,
				AVG(TIMESTAMPDIFF(SECOND, bb.begin_datetime, bb.end_datetime))/count(1) avg_connect_time
			FROM 
				pm_customer_effect aa
				LEFT JOIN pm_call_log bb ON aa.call_log_id = bb.id 
			WHERE
				date( aa.create_time ) = statistics_date_in
				AND bb.connect_flag = 1  
				AND aa.del_flag = 0
			GROUP BY aa.order_id
		)b
		SET a.avg_connect_time = b.avg_connect_time
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 漏接数量omits_num
	UPDATE pm_order_daily_data a,(
		SELECT 
			aa.order_id,
			COUNT(1) omits_num
		FROM 
			pm_call_omits_log aa
		WHERE
			date( aa.create_time ) = statistics_date_in
			AND aa.del_flag = 0
		GROUP BY aa.order_id
	)b
	SET a.omits_num = b.omits_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 有效数量effect_num
	UPDATE pm_order_daily_data a,(
		SELECT 
			aa.order_id,
			COUNT(1) effect_num
		FROM 
			pm_customer_effect aa
		WHERE
			date( aa.create_time ) = statistics_date_in
			AND aa.del_flag = 0
		GROUP BY aa.order_id
	)b
	SET a.effect_num = b.effect_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 保留客户数量save_num
	UPDATE pm_order_daily_data a,(
		SELECT 
			aa.order_id,
			COUNT(1) save_num
		FROM 
			pm_customer_effect aa
		WHERE
			date( aa.create_time) = statistics_date_in AND
			aa.affirm_type = 1
			AND aa.del_flag = 0
		GROUP BY aa.order_id
	)b
	SET a.save_num = b.save_num
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
	-- 账单金额bill_amount
	UPDATE pm_order_daily_data a,(
		SELECT
			aa.order_id,
			COUNT( 1 )  * cc.phone_price bill_amount
		FROM
			pm_customer_effect aa 
			LEFT JOIN pm_order bb on aa.order_id = bb.id
			LEFT JOIN pm_cont cc on bb.cont_id = cc.id
		WHERE
			date( aa.create_time ) = statistics_date_in
			AND aa.del_flag = 0
		GROUP BY
			aa.order_id
	)b
	SET a.bill_amount = b.bill_amount
	WHERE 
		date( a.statistics_date ) = statistics_date_in AND
		a.order_id = b.order_id;
		
		UPDATE pm_order_daily_data set call_num_show = call_num - (call_num - connect_num) * 0.5 WHERE call_num_show = 0;
END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `test_data_reset_phone_id_fun` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE PROCEDURE `test_data_reset_phone_id_fun`(IN account_id_in varchar(50))
BEGIN
	DECLARE v_customer_surname varchar(500) DEFAULT '郭王李张赵刘陈杨吴黄朱孙胡吕高宋徐程林郑范何韩曹马许田冯杜周曾汪苏董方蔡梁石谢贾薛彭崔唐潘邓任史钱侯魏罗叶沈孟姚傅丁章萧蒋卢陆袁江晁谭邵欧孔俞尹廖阎洪夏雷葛文柳陶毛丘龚康蒲邢郝庞安裴折施游金邹汤虞严钟'; 
	DECLARE v_customer_name varchar(50) DEFAULT NULL; -- 客户姓名
	DECLARE v_customer_sex varchar(50) DEFAULT NULL;-- 客户性别
	DECLARE v_customer_archive_id int unsigned DEFAULT 0; -- 档案id
	DECLARE v_phone_id int unsigned DEFAULT 0; -- 号码id
	DECLARE v_order_id int unsigned DEFAULT 0; -- 订单id
	DECLARE v_create_date varchar(50) DEFAULT null; -- 创建时间
	DECLARE v_i INT DEFAULT 0;
	DECLARE v_call_log_id VARCHAR(255);
	DECLARE cursor_done INT DEFAULT FALSE;
	DECLARE call_log_id_cursor_pro CURSOR FOR SELECT id, order_id, create_time FROM pm_call_log WHERE account_id = account_id_in AND connect_flag = '1' AND phone_id > 0 ORDER BY call_begin_datetime DESC;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET cursor_done = TRUE;
	
	-- 清空档案子表
	DELETE FROM pm_customer_archive_detail WHERE account_id = account_id_in AND date_format(create_time, '%H:%i:%s') = '00:00:00';

	OPEN call_log_id_cursor_pro;
		proLoop: LOOP
		FETCH call_log_id_cursor_pro into v_call_log_id, v_order_id, v_create_date;
		IF cursor_done THEN -- 判断是否继续循环  
			LEAVE proLoop; -- 结束循环  
		END IF;
	
		SET v_i = v_i + 1;
		SET v_phone_id = (SELECT phone_id FROM pm_phone_pool WHERE phone_id >= 394 AND del_flag = '0' LIMIT v_i,1);

		-- 有效记录-接通有效
		UPDATE
			pm_call_log 
		SET 
			phone_id = v_phone_id
		WHERE
			id = v_call_log_id;
			
		-- 有效客户 pm_customer_effect	
		UPDATE
			pm_customer_effect 
		SET 
			phone_id = v_phone_id
		WHERE
			call_log_id = v_call_log_id AND
			account_id = account_id_in;
			
		-- 客户跟进记录 pm_customer_follow
		UPDATE
			pm_customer_follow 
		SET 
			phone_id = v_phone_id
		WHERE
			call_log_id = v_call_log_id AND
			account_id = account_id_in;
			
		-- 客户档案子表 pm_customer_archive_detail
		SET v_customer_name = SUBSTRING(v_customer_surname, FLOOR(RAND() * (101 - 1) + 1),1);
		
		IF FLOOR(RAND() * (2 - 0) + 0) = 0 THEN
			SET v_customer_name = CONCAT(v_customer_name, '女士');
			SET v_customer_sex = '女';
		ELSE
			SET v_customer_name = CONCAT(v_customer_name,'先生');
			SET v_customer_sex = '男';
		END IF;
		
		SET v_customer_archive_id = 0;
		IF v_phone_id is NOT null THEN
			SET v_customer_archive_id = (SELECT id FROM pm_customer_archive WHERE phone_id = v_phone_id);
		END IF;
		
		INSERT INTO `pm_customer_archive_detail` (
			`customer_archive_id`,
			`order_id`,
			`account_id`,
			`customer_name`,
			`sex`,
			`create_by`,
			`create_time`,
			`update_by`,
			`update_time`
		)
		VALUES
		(
			v_customer_archive_id,
			v_order_id,
			account_id_in,
			v_customer_name,
			v_customer_sex,
			account_id_in,
			v_create_date,
			account_id_in,
			v_create_date
		);		
		SET cursor_done=FALSE;
  END LOOP;
 CLOSE call_log_id_cursor_pro;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `test_data_roll_date_fun` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = utf8mb4 */ ;
/*!50003 SET character_set_results = utf8mb4 */ ;
/*!50003 SET collation_connection  = utf8mb4_general_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION' */ ;
DELIMITER ;;
CREATE PROCEDURE `test_data_roll_date_fun`(
IN account_id_in varchar(50)
)
BEGIN
	DECLARE v_min_datetime varchar(50) DEFAULT (SELECT MIN(create_time) FROM pm_account_offline_online WHERE account_id = account_id_in);
	DECLARE v_min_date varchar(50) DEFAULT (SELECT DATE(MIN(create_time)) FROM pm_account_offline_online WHERE account_id = account_id_in);
	DECLARE v_diff_days varchar(50) DEFAULT timestampdiff(DAY, v_min_date, DATE(NOW()));
	
	-- 上下线记录 begin_datetime end_datetime create_time update_time
	UPDATE
		pm_account_offline_online 
	SET 
		begin_datetime = DATE_ADD(begin_datetime, INTERVAL v_diff_days DAY),
		end_datetime = DATE_ADD(end_datetime, INTERVAL v_diff_days DAY),
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;
		
	-- 拨打记录 begin_datetime end_datetime call_begin_datetime create_time update_time
	UPDATE
		pm_call_log 
	SET 
		begin_datetime = DATE_ADD(begin_datetime, INTERVAL v_diff_days DAY),
		end_datetime = DATE_ADD(end_datetime, INTERVAL v_diff_days DAY),
		call_begin_datetime = DATE_ADD(call_begin_datetime, INTERVAL v_diff_days DAY),
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;
		
	-- 漏接记录 create_time update_time
	UPDATE
		pm_call_omits_log 
	SET 
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;
		
	-- 有效客户 create_time update_time
	UPDATE
		pm_customer_effect 
	SET 
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;
		
	-- 客户跟进记录 follow_datetime next_follow_datetime create_time update_time
	UPDATE
		pm_customer_follow 
	SET 
		follow_datetime = DATE_ADD(follow_datetime, INTERVAL v_diff_days DAY),
		next_follow_datetime = DATE_ADD(next_follow_datetime, INTERVAL v_diff_days DAY),
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;
		
	-- 客户档案子表 create_time update_time
	UPDATE
		pm_customer_archive_detail 
	SET 
		create_time = DATE_ADD(create_time, INTERVAL v_diff_days DAY),
		update_time = DATE_ADD(update_time, INTERVAL v_diff_days DAY)
	WHERE
		create_time = v_min_datetime AND
		account_id = account_id_in;

END ;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

-- Dump completed on 2022-06-23 17:28:11
