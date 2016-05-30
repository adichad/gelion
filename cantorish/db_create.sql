
CREATE DATABASE IF NOT EXISTS `etl`;

GRANT ALL PRIVILEGES ON `etl`.* TO 'etl'@'localhost' identified by 'etl';
FLUSH PRIVILEGES;

use `etl`;

CREATE TABLE IF NOT EXISTS `mpdm_status` (
  `base_product_id` int(11) NOT NULL,
  `source_base_log_id` bigint(20) NOT NULL,
  `source_subscribed_log_id` bigint(20) NOT NULL,
  `target_base_log_id` bigint(20) NOT NULL,
  `target_subscribed_log_id` bigint(20) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `bucket` int(11) NOT NULL DEFAULT 9999,
  `last_error` varchar(1024) NOT NULL DEFAULT '',
  PRIMARY KEY (`product_id`),
  KEY `source_base_log_id` (`source_base_log_id`),
  KEY `source_subscribed_log_id` (`source_subscribed_log_id`),
  KEY `target_base_log_id` (`target_base_log_id`),
  KEY `target_subscribed_log_id` (`target_subscribed_log_id`),
  KEY `bucket` (`bucket`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE IF NOT EXISTS `mpdm_bookmark` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `log_id` bigint(20) NOT NULL,
  `recs` int(11) NOT NULL,
  `run_stamp` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `time_ms` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  KEY `log_id` (`log_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


