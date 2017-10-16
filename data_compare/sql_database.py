from collections import defaultdict
from sqlalchemy import (
    create_engine,
    inspect,
    MetaData,
)

    # 'description', # I don't care about description right now.
TABLE_KEYS = [
    'fullname',
    'name',
    'primary_key',
]

# I know default is on the SA side, and I suspect onupdate is the same since there is 
# also a server_onupdate. I don't really care about doc for now, and I don't know what
# the rest of these are.
# 'default', 'doc', 'system', 'dispatch', 'onupdate'
# TODO Could we just look at all of the column attributes instead of enumerating them here?

# Omitted keys because they are redundant with the table metadata. TODO confirm the redundancy.
    # 'constraints',
    # 'foreign_keys',
    # 'index',
    # 'primary_key',

COLUMN_KEYS = [
    'autoincrement', 
    'is_literal', 
    'key', 
    'name', 
    'nullable',  
    'server_default', 
    'server_onupdate', 
    'type', 
    'unique', 
]

IGNORE_ITEMS = [
    'PrimaryKeyConstraint',
]


class SQLDatabase (object):

    def __init__(self, conn_string, schema=None):
        # How should we name the class considering it can either refer to
        # a schema or a database?
        # is there any reason to create the engine separately?
        self.schema = schema
        self.engine = create_engine(conn_string)
        self.conn = self.engine.connect()

        # The differences are a nesting of dicts and lists. The first level has table names, then a
        # general entry and column specific entries. If the table data or column list is empty,
        # that means the corresponding database does not have that table or column at all. To illustrate:
        # {
        #     table1:
        #         {
        #             general: [difference1, difference2],
        #             col1: [difference3, difference4],
        #             col2: [difference5]},
        #     table2:
        #         {
        #             general: [],
        #             col3: [difference6],
        #             col4: [difference7, difference8],
        #             col5: [],
        #         },
        #     table3: {},
        # }
        self.differences = defaultdict(dict)
# TODO
# Use insepctor, instead: http://docs.sqlalchemy.org/en/latest/core/reflection.html
        self.metadata = MetaData(bind=self.engine, schema=schema)
        self.metadata.reflect()
        self.inspector = inspect(self.engine)

    @property
    def tables(self):
        return self.metadata.tables

    @property
    def table_names(self):
        return self.inspector.get_table_names(schema=self.schema)

    def table_pk_col_names(self, table):
        return [c.name for c in table.columns if c.primary_key]

    def table_from_name(self, table_name):
        if self.schema:
            table_name = self.schema + table_name
        return self.tables[table_name]

    def column_from_table(self, table, column_name):
        if isinstance(table, str):
            table = self.table_from_name(table)
        return table.columns[column_name]


# has tables with columns with constraints and sequences
# has rows with values

# We want to be able to apply these functions to full databases and
# to tables. Can we make that possible?
# This will require 

# For getting common table names, common columns, etc.
    # def common_elements(db_attr, attr_attr):
    #     return set(getattr(a, attr_attr) for a in getattr(db_a, db_attr) if getattr(db_b, db_attr))

    def compare_schemas(self, comparator):
        """Compare the schemas of the two given databases.
    
        Compare the schema of the database associated with self with the schema of
        the comparator."""
        self.comparator = comparator
        common_table_names = set(self.table_names) & set(comparator.table_names)
        self.dual_set_compare(self.table_names, comparator.table_names)

        # add a and b not found tables to however we end up reporting errors
        # a_not_b_tables = set(db_a.tables) - common_table_names and vice versa
        map(self.compare_table_schemas, common_table_names)

    def dual_set_compare(self, a, b, diff_key=None):
        self.single_set_compare(a, b, 'a', diff_key)
        self.single_set_compare(b, a, 'b', diff_key)

    def single_set_compare(self, a, b, prefix, diff_key):
        for i in remove_ignore(set(a) - set(b)):
            if diff_key:
                self.differences[diff_key]['{}_{}'.format(i, prefix)] = {}
            else:
                self.differences['{}_{}'.format(i, prefix)] = {}
    
    def compare_table_schemas(self, table_name):
        """Compare the general and column specific schemas of each table."""
        ta = self.table_from_name(table_name)
        tb = self.comparator.table_from_name(table_name)

        self.dual_set_compare(ta.constraints, tb.constraints, table_name)
        self.dual_set_compare(ta.foreign_keys, tb.foreign_keys, table_name)
        self.dual_set_compare(ta.indexes, tb.indexes, table_name)

        self.differences[table_name]['general'] = compare(TABLE_KEYS, ta, tb)

        self.compare_table_columns(ta, tb)

    def compare_table_columns(self, ta, tb):
        ta_col_names = set(col.name for col in ta.columns)
        tb_col_names = set(col.name for col in tb.columns)

        for col_name in ta_col_names - tb_col_names:
            self.differences[ta.name]['{}_a'.format(col_name)] = []

        for col_name in tb_col_names - ta_col_names:
            self.differences[tb.name]['{}_b'.format(col_name)] = []

        for col_name in ta_col_names & tb_col_names:
            col_a = self.column_from_table(ta, col_name)
            col_b = self.comparator.column_from_table(tb, col_name)

            self.differences[ta.name][col_name] = compare(COLUMN_KEYS, col_a, col_b)

    def compare_data(self):
        """Compare the data of the two given databases.
    
        Compare the data of database a (db_a) with the data of 
        database b (db_b). Only show the differences unless 
        show_equivalence is True.
        Differences can be different data in one or more columns
        of the same row (as identified by the primary key), missing
        rows (rows db_a has, but db_b does not), or added rows (rows
        that db_b has, but db_a does not).
        A data comparison necessarily includes a schema comparison."""
        pass
    
    def compare_sequences(self):
        pass
    
    def update_schema_b_from_a(self):
        """Update the schema of db_b to that found in db_a."""
        pass
    
    def update_data_b_from_a(self):
        """Update the data in db_b to that found in db_a.
    
        This function assumes that the schemas are equivalent enough
        that this data update will work. If it will not, the function
        rolls back the transaction (if the db_b is of a database type
        that supports rollback) and returns an exception. This function 
        also assumes write privileges on db_b.
        """
        pass
    
    def update_b_sequences_from_a(self):
        pass
    
    # We need type equivalences. 
    # Type + flavor is equivalent to another type + flavor

    def print_differences(self):
        """Print the differences in a (hopefully) human understandable format."""
        print self.differences

def compare(attrs, compare_a, compare_b):
    """Return the unequal attributes between a and b.

    Given a set of attributes and two arbitrary objects, compare the values of
    those attributes of each object. Return the comparisons that were unequal.
    An example use case might be tables as the objects and schema parameters as
    the attributes. """
    errors = {}
    
    for attr in attrs:
        a_val = getattr(compare_a, attr)
        b_val = getattr(compare_b, attr)

        # Allow for string equivalence as otherwise things like VARCHAR() do not match each other
        # TODO investigate if this inequality is because of SQL flavor and possibly use that
        # route to fix.
        if not a_val == b_val and not (str(a_val) == str(b_val)):
            errors['{}_a'.format(attr)] = a_val
            errors['{}_b'.format(attr)] = b_val

    return errors

def remove_ignore(diff_set):
    """Remove items that begin with ignore strings."""
    return_set = set(diff_set)

    for item in diff_set:
        for ignore in IGNORE_ITEMS:
            if ignore in item:
                return_set.remove(item)

    return return_set
