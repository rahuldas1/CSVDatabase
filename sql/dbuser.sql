CREATE USER 'dbuser'@'localhost' IDENTIFIED BY 'dbuser';
GRANT ALL PRIVILEGES ON CSVCatalog . * TO 'dbuser'@'localhost';
FLUSH PRIVILEGES;