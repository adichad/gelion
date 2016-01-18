
CREATE DATABASE `etl`;

-- GRANT ALL PRIVILEGES ON `etl`.* TO 'etl'@'localhost'

use `etl`;

CREATE TABLE `product_status` (
  `product_id` int(11) NOT NULL,
  `source_dt` datetime NOT NULL,
  `target_dt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `last_error` varchar(1024) NOT NULL DEFAULT '',
  PRIMARY KEY (`product_id`),
  KEY `source_dt` (`source_dt`),
  KEY `target_dt` (`target_dt`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `product_bookmark` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_id` int(11) NOT NULL,
  `recs` int(11) NOT NULL,
  `run_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `log_id` (`log_id`)
) ENGINE=InnoDB AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


