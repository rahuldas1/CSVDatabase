USE CSVCatalog;
CREATE TABLE iF NOT EXISTS `CSVIndexes` (
  `table_name` varchar(16) NOT NULL,
  `index_name` varchar(16) NOT NULL,
  `index_type` varchar(16) NOT NULL,
  `columns` varchar(45) NOT NULL,
  PRIMARY KEY (`table_name`,`index_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
