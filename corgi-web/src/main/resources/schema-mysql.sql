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


CREATE TABLE IF NOT EXISTS `metric` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `batch_task_name` varchar(32) NOT NULL,
  `execute_time` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
