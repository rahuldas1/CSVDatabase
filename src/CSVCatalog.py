import pymysql
import csv
import json
import DataTableExceptions
from collections import defaultdict

# hardcoded table names and columns
schema = "CSVCatalog"
table_table = "CSVTables"
column_table = "CSVColumns"
index_table = "CSVIndexes"

table_cols = ["table_name", "file_path"]
column_cols = ["table_name", "column_name", "column_type", "not_null"]
index_cols = ["table_name", "index_name", "index_type", "columns"]


def append_conditions(q, t):
    for k, v in t.items():
        q += "{}='{}' AND ".format(k, v)
    q = q.rstrip(" AND")

    return q


def run_q(cnx, q, fetch=False):
    # print(q)
    cursor = cnx.cursor()
    cursor.execute(q)

    if fetch:
        result = cursor.fetchall()
    else:
        result = None

    cnx.commit()
    return result


class ColumnDefinition:
    """
    Represents a column definition in the CSV Catalog.
    """

    # Allowed types for a column.
    column_types = ("text", "number")

    def __init__(self, column_name, column_type="text", not_null=False):
        """
        :param column_name: Cannot be None.
        :param column_type: Must be one of valid column_types.
        :param not_null: True or False
        """
        if column_type not in ColumnDefinition.column_types:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Invalid column type for column '{}'".format(column_name))
        if not_null not in [True, False]:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="'not null' must be True or False for column '{}'".format(column_name))

        self.name = column_name
        self.type = column_type
        self.not_null = not_null

    def __str__(self):
        return self.name.ljust(20) + self.type.ljust(20) + str(self.not_null)

    def __eq__(self, other):
        if isinstance(other, ColumnDefinition):
            return self.name == other.name
        return False

    def __hash__(self):
        return hash(self.name)

    def to_json(self):
        d = {column_cols[1]: self.name,
             column_cols[2]: self.type,
             column_cols[3]: self.not_null}

        return d


class IndexDefinition:
    """
    Represents the definition of an index.
    """
    index_types = ("PRIMARY", "UNIQUE", "INDEX")

    def __init__(self, index_name, index_type, columns):
        """
        :param index_name: Name for index. Must be unique name for table.
        :param index_type: Valid index type.
        """
        if index_type not in IndexDefinition.index_types:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Invalid index type for index '{}' on columns {}".format(index_name, ",".join(columns)))
        self.name = index_name
        self.type = index_type
        self.columns = columns

    def __str__(self):
        return self.name.ljust(20) + self.type.ljust(20) + ','.join(self.columns)

    def to_json(self):
        d = {index_cols[1]: self.name,
             index_cols[2]: self.type,
             index_cols[3]: self.columns}

        return d


class TableDefinition:
    """
    Represents the definition of a table in the CSVCatalog.
    """

    def __init__(self, t_name=None, csv_f=None, column_definitions=None, index_definitions=None, cnx=None, init=True):
        """
        :param t_name: Name of the table.
        :param csv_f: Full path to a CSV file holding the data.
        :param column_definitions: List of column definitions to use from file. Cannot contain invalid column name.
        :param index_definitions: List of index definitions. Column names must be valid.
        :param cnx: Database connection to use. If None, create a default connection.
        :param init: False if table definition is being retrieved from catalog, else True
        """
        if cnx:
            self.cnx = cnx
        else:
            self.cnx = pymysql.connect(host='localhost',
                                       user='dbuser',
                                       password='dbuser',
                                       db=schema)  # hardcoded defaults

        if not t_name:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Table name cannot be empty")

        try:  # verify file path
            if not csv_f.lower().endswith('.csv'):  # check file type
                raise Exception()
            with open(csv_f, 'r') as csvfile:  # store column names for integrity checking
                reader = csv.reader(csvfile)
                headers = next(reader)
                self.headers = [header.lower() for header in headers]  # as lowercase for simplicity
        except Exception:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Invalid file path {} for table {}".format(csv_f, t_name))

        self.t_name = t_name
        self.csv_f = csv_f
        self.column_definitions = []
        self.index_definitions = []

        if column_definitions:
            for column in column_definitions:
                self.add_column_definition(column, init=init)
            # for integrity checking when adding index
            self.columns = [col.name.lower() for col in self.column_definitions]

        if index_definitions:
            index_names = [index.name for index in index_definitions]
            if len(index_names) != len(set(index_names)):  # check dup index name
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.invalid_column_definition,
                    message="Duplicate index name in table {}".format(t_name))

            for index in index_definitions:
                cols = index.columns  # column checking in the below functions
                if index.type == "PRIMARY":
                    self.define_primary_key(cols, init=init)
                else:
                    self.define_index(index.name, cols, init=init)

    def __str__(self):
        string = "Table name: " + self.t_name
        string += "\nPath: " + self.csv_f
        string += "\nColumns:\n"

        string += "\t" + "Column".ljust(20) + "Datatype".ljust(20) + "Not Null\n"
        for col in self.column_definitions:
            string += '\t' + str(col) + '\n'

        string += "\nIndexes:\n"
        string += "\t" + "Index".ljust(20) + "Type".ljust(20) + "Index Columns\n"
        for ind in self.index_definitions:
            string += '\t' + str(ind) + '\n'

        return string

    @classmethod
    def load_table_definition(cls, cnx, table_name):
        """
        :param cnx: Connection to use to load definition.
        :param table_name: Name of table to load.
        :return: Table and all sub-data. Read from the database tables holding catalog information.
        """
        q = "SELECT * FROM {} WHERE {}='{}'".format(table_table, table_cols[0], table_name)
        table_res = run_q(cnx, q, fetch=True)
        if not table_res:  # check table exists
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Table '{}' does not exist in CSVCatalog".format(table_name))
        q = "SELECT {} FROM {} WHERE {}='{}'".format(', '.join(column_cols[1:]), column_table, column_cols[0], table_name)
        column_res = run_q(cnx, q, fetch=True)
        q = "SELECT {} FROM {} WHERE {}='{}'".format(', '.join(index_cols[1:]), index_table, index_cols[0], table_name)
        index_res = run_q(cnx, q, fetch=True)

        cds = []
        for col in column_res:
            not_null = True if col[2].lower() == 'true' else False
            args = [col[0], col[1], not_null]
            cds.append(ColumnDefinition(*args))
        ids = []
        for ind in index_res:
            columns = list(ind[2].split(','))
            args = [ind[0], ind[1], columns]
            ids.append(IndexDefinition(*args))
        table_name = table_res[0][0]
        csv_f = table_res[0][1]
        table = TableDefinition(table_name, csv_f, cds, ids, init=False)

        return table

    def add_column_definition(self, c, init=True):
        if c.name.lower() not in self.headers:  # check column in csv
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Cannot add column definition for column '{}' ".format(c.name) +
                        "in table {} as column not present in {}".format(self.t_name, self.csv_f))
        for col in self.column_definitions:  # duplicate column check
            if col.name.lower() == c.name.lower():
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.invalid_column_definition,
                    message="Cannot add column definition for column '{}' ".format(c.name) +
                            "in table {} as column already exists".format(self.t_name))

        self.column_definitions.append(c)
        if init:
            q = "INSERT INTO {} ({}) VALUES ('{}', '{}', '{}', '{}')".format(column_table, ', '.join(column_cols),
                                                                             self.t_name, c.name, c.type, c.not_null)
            run_q(self.cnx, q)

    def drop_column_definition(self, c, from_catalog=True):
        to_drop = None
        for col in self.column_definitions:
            if col.name.lower() == c.lower():
                self.column_definitions.remove(col)  # remove from column defs
                to_drop = col
        if to_drop is None:  # column c does not exist
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Cannot drop column definition for column '{}' ".format(c) +
                        "in table {} as it does not exist".format(self.t_name))

        if from_catalog:
            # now removing from catalog table
            q = "DELETE FROM {} WHERE ".format(column_table)
            t = {column_cols[0]: self.t_name, column_cols[1]: to_drop.name}

            q = append_conditions(q, t)
            run_q(self.cnx, q)

    def to_json(self):
        json_table = defaultdict()
        json_table['definition'] = {'name': self.t_name, 'path': self.csv_f}

        indexes = {}
        pk_cols = []
        for ind in self.index_definitions:
            indexes[ind.name] = ind.to_json()
            if ind.name == "PRIMARY":
                pk_cols = [self.get_column_by_name(col) for col in ind.columns]

        columns = []
        for col in pk_cols:
            columns.append(col.to_json())
        non_pk_cols = set(self.column_definitions) - set(pk_cols)
        for col in non_pk_cols:
            columns.append(col.to_json())

        json_table['columns'] = columns
        json_table['indexes'] = indexes

        return json_table

    def define_primary_key(self, columns, init=True):
        self.define_index("PRIMARY", columns, kind="PRIMARY", init=init)

        # primary key columns can't be null
        set_not_null = []
        for col in self.column_definitions:
            if col.name in columns and col.not_null is False:
                set_not_null.append(col)
        # drop column and re-add with not_null=True
        for col in set_not_null:
            data = col.to_json()
            self.drop_column_definition(data[column_cols[1]])
            self.add_column_definition(ColumnDefinition(data[column_cols[1]],
                                                        data[column_cols[2]],
                                                        not_null=True))

    def define_index(self, index_name, columns, kind="INDEX", init=True):
        """
        Define or replace and index definition.
        :param index_name: Index name, must be unique within a table.
        :param columns: Valid list of columns.
        :param kind: One of the valid index types.
        :param init: if True, inserts index info into Catalog
        """
        # duplicate index check is in __init__, so not required when initializing
        if not init:
            for index in self.index_definitions:  # check for existing, if so delete
                if index.name.lower() == index_name.lower():
                    delete = "DELETE FROM {} WHERE {}='{}' \
                        AND {}='{}';".format(index_table, index_cols[0], self.t_name, index_cols[1], index_name)
                    run_q(self.cnx, delete)
                    self.index_definitions.remove(index)

        if not all(col.lower() in self.columns for col in columns):  # check columns
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Cannot create index on table {} ".format(self.t_name) +
                        "as columns are invalid")

        if init:
            q = "INSERT INTO {} ({}) VALUES ('{}', '{}', '{}', '{}')".format(
                index_table, ', '.join(index_cols),
                self.t_name, index_name, kind, ','.join(columns))
            run_q(self.cnx, q)
        self.index_definitions.append(IndexDefinition(index_name, kind, columns))

    def drop_index(self, index_name, from_catalog=True):
        """
        Remove an index.
        :param index_name: Name of index to remove.
        :param from_catalog: if True, drop index from catalog using sql
        """
        to_drop = None
        for index in self.index_definitions:
            if index.name.lower() == index_name.lower():
                self.index_definitions.remove(index)  # remove from index defs
                to_drop = index

        if to_drop is None:  # index does not exist
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Cannot drop index definition for index '{}' ".format(index_name) +
                        "as it does not exist")

        if from_catalog:
            # now removing from Catalog
            q = "DELETE FROM {} WHERE ".format(index_table)
            t = {index_cols[0]: self.t_name, index_cols[1]: to_drop.name}

            q = append_conditions(q, t)
            run_q(self.cnx, q)

    def get_column_by_name(self, column_name):
        for col in self.column_definitions:
            if col.name.lower() == column_name.lower():
                return col

    def describe_table(self):
        """
        Simply wraps to_json()
        :return: JSON representation.
        """
        return self.to_json()


class CSVCatalog:

    def __init__(self, dbhost='localhost', dbport=None, dbname='CSVCatalog',
                 dbuser='dbuser', dbpw='dbuser', debug_mode=None):
        self.cnx = pymysql.connect(host=dbhost,
                                   port=dbport,
                                   user=dbuser,
                                   password=dbuser,
                                   db=dbname)
        self.table_definitions = []

    def __str__(self):
        string = "CSVCatalog:\n"
        string += "\tTable Name\tFile Path\n"
        for table in self.table_definitions:
            string += "\t{}\t{}\n".format(table.t_name, table.csv_f)

        return string

    def create_table(self, table_name, file_name, column_definitions=None, index_definitions=None):
        q = "SELECT * FROM {} WHERE {}='{}'".format(table_table, table_cols[0], table_name)
        dup_check = run_q(self.cnx, q, fetch=True)
        if dup_check:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.duplicate_table_name,
                message="Cannot create table {} as table already exists. ".format(table_name) +
                        "File path: {}".format(dup_check[0][1]))

        try:
            # add to Catalog
            q = "INSERT INTO {} ({}) VALUES ('{}','{}')".format(table_table, ', '.join(table_cols),
                                                                table_name, file_name)
            run_q(self.cnx, q)
            table = TableDefinition(table_name, file_name,
                                    column_definitions, index_definitions)
            self.table_definitions.append(table)
        except Exception as e:
            self.drop_table(table_name)
            raise Exception(e)

        return table

    def drop_table(self, table_name):
        # delete indexes, columns, and table definition from Catalog
        # more efficient than sending one query for each column/index
        q = "DELETE FROM {} WHERE {}='{}'".format(index_table, index_cols[0], table_name)
        run_q(self.cnx, q)
        q = "DELETE FROM {} WHERE {}='{}'".format(column_table, column_cols[0], table_name)
        run_q(self.cnx, q)
        q = "DELETE FROM {} WHERE {}='{}'".format(table_table, table_cols[0], table_name)
        run_q(self.cnx, q)

        req_table = None
        for table in self.table_definitions:
            if table.t_name.lower() == table_name.lower():
                req_table = table
        # could get_table, but if the table doesn't already exist in memory it
        # doesn't make sense to create the table and then delete it immediately
        if req_table:
            for column in table.column_definitions:  # drop columns
                table.drop_column_definition(column, from_catalog=False)
            for index in table.index_definitions:  # drop indexes
                table.drop_index(index.name, from_catalog=False)
            self.table_definitions.remove(table)

    def get_table(self, table_name):
        """
        Returns a previously created table.
        :param table_name: Name of the table.
        :return: required table
        """
        for table in self.table_definitions:
            if table.t_name.lower() == table_name.lower():
                return table

        table = TableDefinition.load_table_definition(self.cnx, table_name)
        return table
