USE CSVCatalog;
CREATE TABLE IF NOT EXISTS `CSVColumns` (
  `table_name` varchar(16) NOT NULL,
  `column_name` varchar(16) NOT NULL,
  `column_type` varchar(10) NOT NULL,
  `not_null` varchar(5) NOT NULL,
  PRIMARY KEY (`table_name`,`column_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
