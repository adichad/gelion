
CREATE DATABASE IF NOT EXISTS `etl`;

GRANT ALL PRIVILEGES ON `etl`.* TO 'etl'@'localhost' identified by 'etl';
FLUSH PRIVILEGES;

use `etl`;

CREATE TABLE IF NOT EXISTS `product_status` (
  `product_id` int(11) NOT NULL,
  `source_dt` datetime NOT NULL,
  `target_dt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `bucket` int(11) NOT NULL DEFAULT 9999,
  `last_error` varchar(1024) NOT NULL DEFAULT '',
  PRIMARY KEY (`product_id`),
  KEY `source_dt` (`source_dt`),
  KEY `target_dt` (`target_dt`),
  KEY `bucket` (`bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `product_bookmark` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_id` int(11) NOT NULL,
  `recs` int(11) NOT NULL,
  `run_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `time_ms` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `log_id` (`log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


