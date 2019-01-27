USE CSVCatalog;
CREATE TABLE IF NOT EXISTS `CSVTables` (
  `table_name` varchar(16) NOT NULL,
  `file_path` varchar(128) NOT NULL,
  PRIMARY KEY (`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
