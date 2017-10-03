from sqlalchemy import (
    create_engine,
    inspect,
    MetaData,
)

    # 'description', # I don't care about description right now.
table_keys = [
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

column_keys = [
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
        #             col5: None,
        #         },
        #     table3: None,
        # }
        self.differences = {}
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

# add ability to create different database types. Similar to BF's 
# PGDBConf

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
        print self.differences

    def dual_set_compare(self, a, b):
        self.single_set_compare(a, b, 'a')
        self.single_set_compare(b, a, 'b')

    def single_set_compare(self, a, b, prefix):
        for i in set(a) - set(b):
            self.differences.append({'table_{}'.format(prefix): i})
    
    def compare_table_schemas(self, table_name):
        ta = self.table_from_name(table_name)
        tb = self.comparator.table_from_name(table_name)

        self.dual_set_compare(ta.constraints, tb.constraints)
        self.dual_set_compare(ta.foreign_keys, tb.foreign_keys)
        self.dual_set_compare(ta.indexes, tb.indexes)

        self.differences.extend(compare(table_keys, ta, tb))

        self.compare_table_columns(ta, tb)

    def compare_table_columns(self, ta, tb):
        ta_col_names = set(col.name for col in ta.columns)
        tb_col_names = set(col.name for col in tb.columns)

        for col_name in ta_col_names - tb_col_names:
            self.differences.append({'table_a_{}'.format(ta.name): col_name})

        for col_name in tb_col_names - ta_col_names:
            self.differences.append({'table_b_{}'.format(tb.name): col_name})

        for col_name in ta_col_names & tb_col_names:
            col_a = self.column_from_table(ta, col_name)
            col_b = self.comparator.column_from_table(tb, col_name)
            
            self.differences.extend(compare(column_keys, col_a, col_b))
    
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

def compare(attrs, compare_a, compare_b):
    errors = []
    
    for attr in attrs:
        a_val = getattr(compare_a, attr)
        b_val = getattr(compare_b, attr)
    
        if not a_val == b_val:
            errors.append({
                'a{}'.format(attr): a_val,
                'b{}'.format(attr): b_val,
                })
    
    return errors