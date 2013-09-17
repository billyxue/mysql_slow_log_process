CREATE TABLE `t_slow_config` (
	  `id` smallint(6) NOT NULL AUTO_INCREMENT,
	  `host_ip` char(16) NOT NULL,
	  `host_port` int(11) NOT NULL DEFAULT '3306',
	  `host_socket` char(60) NOT NULL DEFAULT '/var/run/mysqld/mysqld.sock',
	  `db_user` char(16) NOT NULL DEFAULT 'backup',
	  `db_pass` char(16) NOT NULL DEFAULT 'backuppwd',
	  `slow_collect_flag` enum('1','0') DEFAULT '1',
	  `slog_center_host` char(16) NOT NULL,
	  `slog_center_port` smallint(6) NOT NULL DEFAULT '6677',
	  `proj_id` smallint(6) NOT NULL DEFAULT '1',
	  PRIMARY KEY (`id`),
	  KEY `proj_id` (`proj_id`)
) ENGINE=innodb CHARSET=utf8 ;

CREATE TABLE `t_slow_log` (
	  `log_id` int(11) NOT NULL AUTO_INCREMENT,
	  `host_ip` char(16) NOT NULL,
	  `proj_name` varchar(50) DEFAULT NULL,
	  `host_port` int(11) NOT NULL DEFAULT '3306',
	  `slow_query` varchar(512) NOT NULL,
	  `count` smallint(6) NOT NULL,
	  `query_time` tinyint(4) NOT NULL,
	  `lock_time` tinyint(4) NOT NULL,
	  `return_rows` bigint(20) NOT NULL,
	  `dbuser` char(64) NOT NULL,
	  `host` char(32) NOT NULL,
	  `gen_date` date NOT NULL DEFAULT '0000-00-00',
	  PRIMARY KEY (`log_id`),
	  KEY `slow_query` (`slow_query`(255),`gen_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 ;

CREATE TABLE `t_ip_to_proj` (
	  `host_ip` char(16) NOT NULL,
	  `host_port` int(11) NOT NULL DEFAULT '3306',
	  `proj_name` varchar(20) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
