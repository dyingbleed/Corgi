CREATE TABLE IF NOT EXISTS `datasource` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(32) UNIQUE NOT NULL,
  `url` varchar(100) NOT NULL,
  `username` varchar(32) NOT NULL,
  `password` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `ods_task` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(32) UNIQUE NOT NULL,
  `datasource_id` bigint(20) NOT NULL,
  `source_db` varchar(32) NOT NULL,
  `source_table` varchar(32) NOT NULL,
  `mode` char(8) NOT NULL,
  `sink_db` varchar(32) NOT NULL,
  `sink_table` varchar(32) NOT NULL,
  `time_column` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `dm_task` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(32) DEFAULT NULL,
  `source_db` varchar(32) NOT NULL,
  `source_table` varchar(32) NOT NULL,
  `mode` char(8) NOT NULL,
  `datasource_id` bigint(20) NOT NULL,
  `sink_db` varchar(32) NOT NULL,
  `sink_table` varchar(32) NOT NULL,
  `where_exp` varchar(1024) DEFAULT NULL,
  `day_offset` int(11) DEFAULT '-1',
  PRIMARY KEY (`id`),
  UNIQUE KEY `dm_task_name_uindex` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `execute_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `batch_task_name` varchar(32) NOT NULL,
  `execute_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `measure` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `submission_time` datetime NOT NULL,
  `completion_time` datetime NOT NULL,
  `elapsed_seconds` bigint(20) NOT NULL,
  `input_rows` bigint(20) NOT NULL,
  `input_data` bigint(20) NOT NULL,
  `output_rows` bigint(20) NOT NULL,
  `output_data` bigint(20) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `alert` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `level` tinyint(4) NOT NULL,
  `type` varchar(64) NOT NULL,
  `batch_task_id` bigint(20) NOT NULL,
  `msg` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `alert_unique_index` (`type`,`batch_task_id`)
) ENGINE=InnoDB AUTO_INCREMENT=3 DEFAULT CHARSET=utf8;