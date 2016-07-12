
#CREATE DATABASE IF NOT EXISTS `etl`;

#GRANT ALL PRIVILEGES ON `etl`.* TO 'etl'@'localhost' identified by 'etl';
#FLUSH PRIVILEGES;

use `etl`;

CREATE TABLE IF NOT EXISTS `grocery_status` (
  `variant_id` bigint NOT NULL,
  `source_log_id` bigint NOT NULL,
  `target_log_id` bigint NOT NULL DEFAULT 0,
  `source_order_last_updated_on` datetime NOT NULL DEFAULT '1970-01-01 00:00:00',
  `target_order_last_updated_on` datetime NOT NULL DEFAULT '1970-01-01 00:00:00'
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `bucket` int(11) NOT NULL DEFAULT 9999,
  `last_error` varchar(1024) NOT NULL DEFAULT '',
  PRIMARY KEY (`variant_id`),
  KEY `source_log_id` (`source_log_id`),
  KEY `target_log_id` (`target_log_id`),
  KEY `bucket` (`bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `grocery_bookmark` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `log_id` bigint NOT NULL,
  `recs` int(11) NOT NULL,
  `run_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `time_ms` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `log_id` (`log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `grocery_orders_bookmark` (
  `id` bigint NOT NULL AUTO_INCREMENT,
  `updated_on` datetime NOT NULL,
  `recs` int(11) NOT NULL,
  `run_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `time_ms` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `updated_on` (`updated_on`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


