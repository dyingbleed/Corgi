CREATE TABLE IF NOT EXISTS `datasource` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(32) UNIQUE NOT NULL,
  `url` varchar(100) NOT NULL,
  `username` varchar(32) NOT NULL,
  `password` varchar(100) NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE IF NOT EXISTS `batch` (
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