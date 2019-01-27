import csv
import copy
import sys
import time
import operator
import re
from collections import defaultdict, OrderedDict
import DataTableExceptions
import CSVCatalog

max_rows_to_print = 10
null_sym = '\033[1m' + "NULL" + '\033[0m'


def print_rows(rows, all=False):
    string = ""
    if not rows:
        return

    row_width = 16
    cols = []
    for col in rows[0].keys():
        if col != 'rownum':
            cols.append(col)
            string += col.ljust(row_width)
    string += '\n'

    if not all and len(rows) > 20:
        for row in rows[:10]:
            for col in cols:
                if row[col]:
                    string += str(row[col]).ljust(row_width)
                else:
                    string += null_sym.ljust(row_width + 8)
            string += '\n'

        for _ in range(len(cols)):
            string += "...".ljust(row_width)
        string += '\n'

        for row in rows[-10:]:
            for col in cols:
                if row[col]:
                    string += str(row[col]).ljust(row_width)
                else:
                    string += null_sym.ljust(row_width + 8)
            string += '\n'
    else:
        for row in rows:
            for col in cols:
                if row[col]:
                    string += str(row[col]).ljust(row_width)
                else:
                    string += null_sym.ljust(row_width + 8)
            string += '\n'

    print(string)


class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__table_name__ = t_name
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = None
            self.__load__()  # Load rows from the CSV file.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"

    def __load_info__(self):
        """
        Loads metadata from catalog and sets __description__ to hold the information.
        :return:
        """
        self.__description__ = self.__catalog__.get_table(self.__table_name__).describe_table()

    def __load__(self):

        try:
            self.__deleted_rows__ = []
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')

                column_names = self.__get_column_names__()
                column_types = self.__get_column_types__()
                not_null_columns = self.__get_not_null_columns__()

                for r in reader:
                    projected_r = self.project([r], column_names)[0]

                    for k, v in projected_r.items():  # convert numerical data types
                        if v == '':
                            if k in not_null_columns:
                                raise DataTableExceptions.DataTableException(
                                    code=DataTableExceptions.DataTableException.cannot_be_null,
                                    message="Cannot load table {}. NULL value found in column {}.".format(self.__table_name__, k))
                            else:
                                projected_r[k] = None
                        elif column_types[k] == "number":
                            if '.' in v:
                                projected_r[k] = float(v)
                            else:
                                projected_r[k] = int(v)

                    self.__add_row__(projected_r)

        except IOError as e:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_file,
                message="Could not read file = " + fn)

    def __get_column_names__(self):
        if not hasattr(self, '__column_names__'):
            self.__column_names__ = [col['column_name'] for col in self.__description__['columns']]

        if 'rownum' in self.__column_names__:
            self.__column_names__.remove('rownum')

        return self.__column_names__

    def __get_column_types__(self):
        if not hasattr(self, '__column_types__'):
            self.__column_types__ = {col['column_name']: col['column_type'] for col in self.__description__['columns']}

        return self.__column_types__

    def __get_not_null_columns__(self):
        if not hasattr(self, '__not_null_cols__'):
            self.__not_null_cols__ = [col['column_name'] for col in self.__description__['columns'] if col['not_null']]

        return self.__not_null_cols__

    def __get_file_name__(self):
        if not hasattr(self, '__file_name__'):
            self.__file_name__ = self.__description__['definition']['path']

        return self.__file_name__

    def __len__(self):
        if self.__rows__:
            return len(self.__rows__) - self.__rows__.count(None)  # exclude deleted rows
        else:
            return 0

    def __str__(self, all=False, rownums=False):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        row_width = 16

        string = ""
        string += "Name: {}\tFile: {}\n".format(self.__table_name__, self.__get_file_name__())
        string += "Row count: {}\n".format(len(self))

        columns = self.__get_column_names__()
        if rownums:
            columns.insert(0, 'rownum')
        for col in columns:
            string += col.ljust(row_width)
        string += '\n'

        count = 0
        for r in self.__rows__:
            if not r:
                continue

            for col in columns:
                if r.get(col) is not None:
                    string += str(r[col]).ljust(row_width)
                else:
                    string += null_sym.ljust(row_width + 8)
            string += '\n'

            count += 1
            if not all and count >= max_rows_to_print:
                for _ in range(len(columns)):
                    string += "...".ljust(row_width)
                string += '\n'
                break

        return string

    def __add_row__(self, r):
        if not hasattr(self, '__rownum__'):
            self.__rownum__ = -1
        if not self.__rows__:
            self.__rows__ = []

        self.__rownum__ += 1
        r['rownum'] = self.__rownum__
        self.__rows__.append(r)

        return self.__rownum__

    def __refresh_rownums__(self):
        count = 0
        for row in self.__rows__:
            row['rownum'] = count
            count += 1

    def __show_loading_bar__(self, count, n):
        bar_len = 20

        if count == n == 0:
            sys.stdout.write("\033[s")
            sys.stdout.write("[" + ' ' * bar_len + "]   0%")
            return

        if count < (n - 1):
            n_dots = count // (n // bar_len)
            bar = '[' + ('.' * n_dots).ljust(bar_len) + '] ' + str(count * 100 // n).rjust(3) + '%'
        else:
            bar = '[' + ('.' * bar_len).ljust(bar_len) + '] ' + '100%'

        sys.stdout.write("\033[" + str(len(bar)) + "D\033[u")
        sys.stdout.write(bar)
        sys.stdout.flush()

    def __build_indexes__(self):
        self.indexes = self.__description__['indexes']

        if self.indexes.get("PRIMARY"):  # index optimization for multicolumn primary keys
            columns = self.indexes['PRIMARY']['columns']

            for i in range(len(columns) - 1):
                ind_cols = columns[:i + 1]
                if all(ind_cols != ind['columns'] for ind in self.indexes.values()):  # prevent dup index
                    new_idx = {}
                    ind_name = '_'.join(ind_cols) + '_idx'
                    new_idx['index_name'] = ind_name
                    new_idx['index_type'] = "INDEX"
                    new_idx['columns'] = ind_cols

                    self.indexes[ind_name] = new_idx

        to_drop = []
        for index_name in self.indexes:
            index = defaultdict(list)
            data = self.indexes[index_name]
            columns = data['columns']
            rows = self.__rows__
            n = len(rows)

            print("{}: Building index {} on {}\n\t\t\t\t\t".format(self.__table_name__, index_name, ','.join(columns)), end='')
            self.__show_loading_bar__(0, 0)
            start_time = time.time()

            for row in rows:
                key, _ = self.__create_key_template__(row, columns)

                if len(key) > 0:
                    index[key].append(row['rownum'])

                self.__show_loading_bar__(row['rownum'], n)

            print("  {:.4f}s".format(time.time() - start_time))
            data['selectivity'] = self.__get_index_selectivity__(index)
            if data['selectivity'] < 1.0:
                if data['index_type'] == "UNIQUE":
                    print("Warning: Skipping unique index '{}' as it fails unique constraint.".format(data['index_name']))
                    to_drop.append(data['index_name'])
                    continue

                if data['index_type'] == "PRIMARY":
                    raise DataTableExceptions.DataTableException(
                        code=DataTableExceptions.DataTableException.duplicate_row_pk,
                        message="Aborting; duplicate entry for key 'PRIMARY'."
                    )

            data['index'] = index

        for ind_name in to_drop:
            self.indexes.pop(ind_name)

    def __update_indexes__(self, fields, rownums=None, add=False, remove=False, inserting=False):
        if inserting:
            indexes = [ind_name for ind_name, ind in self.indexes.items() if all(col in fields for col in ind['columns'])]
        else:
            indexes = [ind_name for ind_name, ind in self.indexes.items() if any(col in fields for col in ind['columns'])]

        if rownums is None:
            rownums = range(len(self.__rows__))
        for index_name in indexes:
            for rownum in rownums:
                key, _ = self.__create_key_template__(self.__rows__[rownum], self.indexes[index_name]['columns'])
                if len(key) > 0:
                    if add:
                        self.indexes[index_name]['index'][key].append(rownum)
                    if remove:
                        self.indexes[index_name]['index'][key].remove(rownum)
            self.indexes[index_name]['selectivity'] = self.__get_index_selectivity__(self.indexes[index_name]['index'])

    def __get_index_selectivity__(self, index):
        """
        :param index: Index (dict) with key->rownums pairs
        :return: Index selectivity
        """
        return len(index) / len(self.__rows__)

    def __create_key_template__(self, r, cols):
        key = ""
        t = {}
        for col in cols:
            key += str(r[col]) + '_'
            t[col] = r[col]
        key = key.rstrip('_')

        return key, t

    def __get_access_path__(self, tmp):
        """
        Returns best index matching the set of keys in the template.
        :param tmp: Query template.
        :return: Index or None
        """
        pk_index = self.indexes.get('PRIMARY')
        if pk_index:
            # if primary key exists with necessary columns, it is obviously the most selective
            if all(col in list(tmp.keys()) for col in pk_index['columns']):
                return pk_index

        # compile list of valid indexes
        possible_inds = []
        for index_name, index in self.indexes.items():
            if index_name == "PRIMARY":
                continue
            if all(col in list(tmp.keys()) for col in index['columns']):
                possible_inds.append(index)

        if len(possible_inds) == 0:
            return None
        else:
            # select index with highest selectivity
            ind_selx = [ind['selectivity'] for ind in possible_inds]
            best_ind = possible_inds[ind_selx.index(max(ind_selx))]
            return best_ind

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """
        if t is None:
            return True
        if row is None:
            return False

        try:
            if any(row.get(n) != t[n] for n in t.keys()):
                return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None:  # If there is not project clause, return the base table
                return rows
            else:
                result = []
                for r in rows:  # For every row in the table.
                    tmp = {}
                    for j in range(0, len(fields)):  # Make a new row with just the requested columns/fields.
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  # Insert into new table when done.

                return result

        except KeyError as ke:
            # happens if the requested field not in rows.
            raise DataTableExceptions.DataTableException(-2, "Invalid field in project")

    def __get_sub_where_clause__(self, t):
        sub_t = {}
        columns = self.__get_column_names__()

        for col, val in t.items():
            if col in columns:
                sub_t[col] = val

        return sub_t

    def __get_on_template__(self, r, fields):
        t = {}
        for field in fields:
            t[field] = r.get(field)

        return t

    def __join_rows__(self, l, right_rows, on_fields, project_fields=None):
        result = []
        for r in right_rows:
            row = copy.deepcopy(l)
            for k, v in r.items():
                if k not in on_fields:
                    row[k] = v

            row = self.project([row], project_fields)[0]
            result.append(row)

        return result

    def __find_by_template_scan__(self, t, fields=None, rownums=None):
        """
        Returns rows that match the template and the requested fields if any.
        Returns all rows if template is None and all columns if fields is None.
        """
        if self.__rows__ is not None:

            result = []

            rows = self.__rows__
            if rownums:
                for rownum in rownums:
                    if self.matches_template(rows[rownum], t):
                        result.append(rows[rownum])
            else:
                # Add the rows that match the template to the newly created table.
                for r in rows:
                    if self.matches_template(r, t):
                        result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __find_by_template_index__(self, t, idx, fields=None, rownums=None):
        """
        Find using a selected index
        """
        if self.__rows__ is not None:
            result = []

            index = idx['index']
            idx_cols = idx['columns']
            key, _ = self.__create_key_template__(t, idx_cols)

            if index.get(key) is None:
                return None

            if rownums:
                # when doing equijoin, probe rows have already been found from where clause,
                # so we simply take the intersection of these rows with the on template rows
                rownums = set(index[key]) & set(rownums)
            else:
                rownums = index[key]

            for rownum in rownums:
                r = self.__rows__[rownum]
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def find_by_template(self, t, fields=None, limit=None, offset=None, rownums=None, show_time=True):
        """
        Returns rows which match template
        """
        usage = "Usage: <CSVTable>.find_by_template({where clause}, fields=[], limit=<int>, offset=<int>)"
        if not isinstance(t, (dict, OrderedDict)) or fields and not isinstance(fields, list) \
                or limit and not isinstance(limit, int) or offset and not isinstance(offset, int):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )

        start_time = time.time()
        if not all(col in self.__get_column_names__() for col in t.keys()):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_column_definition,
                message="Invalid columns in where template")

        index = self.__get_access_path__(t)
        if index:
            result = self.__find_by_template_index__(t, index, fields, rownums=rownums)
        else:
            result = self.__find_by_template_scan__(t, fields, rownums=rownums)

        if offset:
            result = result[offset:]
        if limit:
            if len(result) > limit:
                result = result[:limit]

        if show_time:
            print("Fetch time: {:.4f}s".format(time.time() - start_time))
        return result

    def insert(self, r):
        """
        Inserts row into table
        """
        usage = "Usage: <CSVTable>.insert({<column>: <value>, ...})"
        if self.__file_name__ == "DERIVED":
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_operation,
                message="Cannot insert into derived table"
            )
        if not isinstance(r, (dict, OrderedDict)):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )
        for col in r.keys():
            if col not in self.__get_column_names__():
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.unknown_column,
                    message="Unknown column '{}' in field list".format(col)
                )
        for col in self.__get_not_null_columns__():
            if r.get(col) is None:
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.cannot_be_null,
                    message="Insert failed; {} is NULL".format(col)
                )

        # check duplicate
        p_ind = self.indexes.get("PRIMARY")
        if p_ind:
            key, _ = self.__create_key_template__(r, p_ind['columns'])
            if len(p_ind['index'][key]) > 0:
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.duplicate_row_pk,
                    message="Insert failed; duplicate entry for key PRIMARY."
                )

        # __file_name__ = "../data/Pitching2.csv"
        try:
            with open(self.__file_name__, "r") as csvfile:
                reader = csv.DictReader(open(self.__file_name__, "r"), delimiter=",", quotechar='"')
                fields = reader.fieldnames
            with open(self.__file_name__, "a") as csvfile:
                writer = csv.DictWriter(csvfile, fields,
                                        delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
                writer.writerow(r)
        except Exception:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.io_error,
                message="Insert failed; error while writing to file"
            )

        rownum = self.__add_row__(copy.deepcopy(r))
        self.__update_indexes__(r.keys(), [rownum], add=True, inserting=True)

    def delete(self, t):
        """
        Delete rows matching template
        """
        usage = "Usage: <CSVTable>.delete({where clause, ...})"

        if self.__file_name__ == "DERIVED":
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_operation,
                message="Cannot delete from derived table"
            )
        if not isinstance(t, (dict, OrderedDict)):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )
        for col in t.keys():
            if col not in self.__get_column_names__():
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.unknown_column,
                    message="Unknown column '{}' in where clause\n".format(col) + usage
                )

        rows_to_delete = self.find_by_template(t, show_time=False)
        rownums = set([row['rownum'] for row in rows_to_delete])

        try:
            rows = []
            with open(self.__file_name__, "r") as csvfile:
                reader = csv.DictReader(open(self.__file_name__, "r"), delimiter=",", quotechar='"')
                for r in reader:
                    while (reader.line_num - 2) in self.__deleted_rows__:
                        reader.line_num += 1  # account for deleted rows
                    if (reader.line_num - 2) in rownums:
                        continue
                    else:
                        rows.append(r)
                fields = reader.fieldnames
            with open(self.__file_name__, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fields,
                                        delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(rows)
        except Exception:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.io_error,
                message="Delete failed; error while writing to file"
            )

        self.__update_indexes__(t.keys(), rownums, remove=True)
        self.__deleted_rows__.extend(list(rownums))
        for rownum in rownums:
            self.__rows__[rownum] = None

    def update(self, t, change_values):
        """
        Update rows matching template with new values
        """
        usage = "Usage: <CSVTable>.update({where clause, ...}, change_values={...})"

        if self.__file_name__ == "DERIVED":
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_operation,
                message="Cannot update values in derived table"
            )
        if not isinstance(t, (dict, OrderedDict)) or not isinstance(change_values, (dict, OrderedDict)):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )
        for col in t.keys():
            if col not in self.__get_column_names__():
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.unknown_column,
                    message="Unknown column '{}' in where clause\n".format(col) + usage
                )

        rows_to_update = self.find_by_template(t, show_time=False)
        rownums = set([row['rownum'] for row in rows_to_update])

        try:
            rows = []
            with open(self.__file_name__, "r") as csvfile:
                reader = csv.DictReader(open(self.__file_name__, "r"), delimiter=",", quotechar='"')
                for r in reader:
                    while (reader.line_num - 2) in self.__deleted_rows__:
                        reader.line_num += 1  # account for deleted rows
                    if (reader.line_num - 2) in rownums:
                        for k, v in change_values.items():
                            r[k] = v
                    rows.append(r)
                fields = reader.fieldnames
            with open(self.__file_name__, "w") as csvfile:
                writer = csv.DictWriter(csvfile, fields,
                                        delimiter=",", quotechar='"', quoting=csv.QUOTE_ALL)
                writer.writeheader()
                writer.writerows(rows)
        except Exception:
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.io_error,
                message="Update failed; error while writing to file"
            )

        relevant_inds = [ind_name for ind_name, ind in self.indexes.items() if any(col in t.keys() for col in ind['columns'])]
        for index_name in relevant_inds:  # remove old indexes
            for rownum in rownums:
                key, _ = self.__create_key_template__(self.__rows__[rownum], self.indexes[index_name]['columns'])
                if len(key) > 0:
                    self.indexes[index_name]['index'][key].remove(rownum)

        self.__update_indexes__(t.keys(), rownums, remove=True)  # remove old indexes
        for rownum in rownums:  # update internal values
            for k, v in change_values.items():
                self.__rows__[rownum][k] = v
        self.__update_indexes__(t.keys(), rownums, add=True)

    def join(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSVTables.
        :param right_r: The second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: Joined table
        """
        usage = "Usage: <CSVTable>.join(<CSVTable>, on_fields=[...], where_template={...}, project_fields=[...])"
        left_r = self

        if not isinstance(right_r, CSVTable) or not isinstance(on_fields, list) or \
                where_template and not isinstance(where_template, (dict, OrderedDict)) or \
                project_fields and not isinstance(project_fields, list):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )

        # on_fields must be present in both tables
        if not all(on_field in left_r.__get_column_names__() for on_field in on_fields) \
                or not all(on_field in right_r.__get_column_names__() for on_field in on_fields):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.unknown_column,
                message="Could not perform equijoin; invalid on clause\n" + usage)

        # switch scan and probe tables based on index selectivity
        on_template = left_r.__get_on_template__({}, on_fields)
        left_index = left_r.__get_access_path__(on_template)
        right_index = right_r.__get_access_path__(on_template)
        left_sel = left_index['selectivity'] if left_index else 0
        right_sel = right_index['selectivity'] if right_index else 0
        if left_sel > right_sel:
            print("Using index {} on {}".format(left_index['index_name'], self.__table_name__))
            left_r = right_r
            right_r = self
        else:
            print("Using index {} on {}".format(right_index['index_name'], right_r.__table_name__))

        scan_rows = left_r.__rows__
        probe_rows = right_r.__rows__
        probe_rownums = None

        # where
        if where_template:
            scan_rows = left_r.find_by_template(left_r.__get_sub_where_clause__(where_template))
            probe_rows = right_r.find_by_template(right_r.__get_sub_where_clause__(where_template))
            probe_rownums = [row['rownum'] for row in probe_rows]

        join_result = []
        count = 0
        n = len(scan_rows)
        self.__show_loading_bar__(0, 0)
        start_time = time.time()
        # equijoin + project
        for l_r in scan_rows:
            on_template = left_r.__get_on_template__(l_r, on_fields)
            current_right_rows = right_r.find_by_template(on_template, rownums=probe_rownums, show_time=False)

            if current_right_rows is not None and len(current_right_rows) > 0:
                new_rows = self.__join_rows__(l_r, current_right_rows,
                                              on_fields, project_fields)
                join_result.extend(new_rows)

            count += 1
            self.__show_loading_bar__(count, n)

        join_name = left_r.__table_name__ + '_' + right_r.__table_name__ + '_' + '_'.join(on_fields)
        join_table = CSVTable(join_name, load=False)

        join_table.__column_names__ = list(join_result[0].keys())
        join_table.__column_types__ = {}
        for col in join_table.__get_column_names__():
            join_table.__column_types__[col] = left_r.__get_column_types__().get(col) or right_r.__get_column_types__().get(col)

        for field in reversed(on_fields):  # order by on fields
            join_result = sorted(join_result, key=lambda x: x[field])
        join_table.__rows__ = join_result

        # update indexes
        join_table.__refresh_rownums__()
        join_table.indexes = {}
        if left_r.indexes.get('PRIMARY') and right_r.indexes.get('PRIMARY'):  # assign primary index
            if project_fields is None or any(field not in project_fields
                                             for field in set(left_r.indexes['PRIMARY']['columns'] + right_r.indexes['PRIMARY']['columns'])):
                if len(left_r.indexes['PRIMARY']['columns']) > len(right_r.indexes['PRIMARY']['columns']):
                    relevant_table = left_r
                else:
                    relevant_table = right_r
            elif all(field in project_fields for field in left_r.indexes['PRIMARY']['columns']):
                relevant_table = left_r
            elif all(field in project_fields for field in right_r.indexes['PRIMARY']['columns']):
                relevant_table = right_r
            else:
                relevant_table = None

            if relevant_table:
                index = relevant_table.indexes['PRIMARY']
                join_table.indexes['PRIMARY'] = {'index_name': index['index_name'],
                                                 'index_type': index['index_type'],
                                                 'columns': index['columns'],
                                                 'index': defaultdict(list)}
        for index_name, index in list(left_r.indexes.items()) + list(right_r.indexes.items()):  # remaining indexes
            if join_table.indexes.get(index_name) is None:
                if project_fields and any(col not in project_fields for col in index['columns']):
                    continue
                join_table.indexes[index_name] = {'index_name': index['index_name'],
                                                  'index_type': index['index_type'],
                                                  'columns': index['columns'],
                                                  'index': defaultdict(list)}

        join_table.__update_indexes__(join_table.__column_names__, add=True, inserting=True)

        print("  {:.4f}s".format(time.time() - start_time))
        return join_table

    def having(self, *conds):
        """
        Returns derived table with rows satisfying given conditions.
        Supports = < <= > >= !=
        """
        if len(conds) == 0:
            return self

        start_time = time.time()
        usage = "Usage: <CSVTable>.having('<column> = <val>', 'yearID >= 2000', ...)"
        if not all(isinstance(cond, str) for cond in conds):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )

        operators = {'=': operator.eq,
                     '!=': operator.ne,
                     '<': operator.lt,
                     '>': operator.gt,
                     '<=': operator.le,
                     '>=': operator.ge}

        conditions = []
        for c in conds:
            split_cond = re.split(r"([!><=]+)", c.replace(' ', ''))
            if len(split_cond) != 3 or operators.get(split_cond[1]) is None:  # check operator
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.invalid_operation,
                    message="Invalid condition. Supported operators: = < <= > >= !=\n" + usage
                )
            elif split_cond[0] not in self.__get_column_names__():  # check columns
                raise DataTableExceptions.DataTableException(
                    code=DataTableExceptions.DataTableException.unknown_column,
                    message="Unknown column '{}' in condition\n".format(split_cond[0]) + usage
                )

            if self.__get_column_types__().get(split_cond[0]) == "number":
                split_cond[2] = float(split_cond[2])

            split_cond[1] = operators[split_cond[1]]
            conditions.append(tuple(split_cond))

        rows = self.__rows__
        matching_rows = []
        for row in rows:
            valid = True
            for condition in conditions:
                row_val = row.get(condition[0])
                if row_val is None or not condition[1](row_val, condition[2]):
                    valid = False
                    break

            if valid:
                matching_rows.append(copy.deepcopy(row))

        t_name = self.__table_name__ + '_having_' + '_'.join([c[0] for c in conditions])
        new_table = CSVTable(t_name, load=False)

        new_table.__column_names__ = self.__get_column_names__()
        new_table.__column_types__ = self.__get_column_types__()
        new_table.__rows__ = matching_rows

        print("Fetch time: {:.4f}s".format(time.time() - start_time))
        return new_table

    def order_by(self, *cols):
        """
        Returns new table with rows sorted by given columnsß
        """
        if len(cols) == 0:
            return self

        start_time = time.time()
        usage = "Usage: <CSVTable>.order_by('<column> <ASC/DESC>', ...)"
        if not all(isinstance(col, str) for col in cols):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.invalid_method_call,
                message=usage
            )

        sorts = []
        for col in cols:
            args = col.split(' ')
            if len(args) == 2 and args[1].lower() == "desc":
                sorts.append((args[0], True))
            else:
                sorts.append((args[0], False))

        if not all(sort[0] in self.__get_column_names__() for sort in sorts):
            raise DataTableExceptions.DataTableException(
                code=DataTableExceptions.DataTableException.unknown_column,
                message="Unknown column in order_by function call\n" + usage
            )

        rows = copy.deepcopy(self.__rows__)
        for sort in reversed(sorts):
            rows = sorted(rows, key=lambda x: x[sort[0]], reverse=sort[1])

        t_name = self.__table_name__ + '_orderby_' + '_'.join([sort[0] for sort in sorts])
        sorted_table = CSVTable(t_name, load=False)
        sorted_table.__column_names__ = self.__get_column_names__()
        sorted_table.__column_types__ = self.__get_column_types__()
        sorted_table.__rows__ = rows

        print("Sort time: {:.4f}s".format(time.time() - start_time))
        return sorted_table

    def print_all(self, rownums=False):
        print(self.__str__(all=True, rownums=rownums))
