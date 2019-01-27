## CSV Database

This program emulates an SQL database in python, retrieving data from CSV files .\
[CSVCatalog.py](/src/CSVCatalog.py) defines the classes TableDefinition, ColumnDefinition, IndexDefinition, and CSVCatalog. CSVCatalog uses the other classes to initialize a table and stores the metadata (file path, columns, and indexes; essentially the information schema) in an SQL database for data integrity, so that table definitions can be retrieved after being initialized once.
After a table is initialized in the CSVCatalog, the table can be retrieved by creating a CSVTable object using only the table name.\
CSVTable supports many of the standard SQL clauses, including SELECT, WHERE, INSERT, UPDATE, DELETE, JOIN, HAVING, and ORDER BY.\
\
Optimizations are based on the [MySQL 8.0 Reference Manual](https://dev.mysql.com/doc/refman/8.0/en/optimization.html)


# CSVCatalog SQL schema

For data integrity purposes, CSVCatalog stores table metadata in an SQL database. CREATE statements for the necessary tables are in the /sql folder. Furthermore, the database must have a user with name 'dbuser' and password 'dbuser', which can be done using the dbuser.sql file.

# Necessary packages/programs

MySQL\
pymysql

# Some usage examples

![catalog](/test/usage/initialize_catalog.png)


![table](/test/usage/initialize_table.png)


![joins](/test/usage/join.png)


![other functions](/test/usage/having_order_by.png)
