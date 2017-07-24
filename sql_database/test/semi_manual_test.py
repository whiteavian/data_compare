from sql_database import (
	DBA_CONN,
	DBB_CONN,
	TableComparison,
	SQLDatabase,
)


sa = SQLDatabase(DBA_CONN)
sb = SQLDatabase(DBB_CONN)

sa.compare_schemas(sb)

table_a = sa.table_from_name('users')
table_b = sb.table_from_name('users')

res_a = sa.conn.execute(table_a.select())
res_b = sb.conn.execute(table_b.select())

a = res_a.fetchall()
b = res_b.fetchall()

headers_a = tuple(c.name for c in table_a.columns)
headers_b = tuple(c.name for c in table_b.columns)

# Add error in "real" case for when the length > 1
pk_a = sa.table_pk_col_names(table_a)[0]
pk_b = sb.table_pk_col_names(table_b)[0]

a.insert(0, headers_a)
b.insert(0, headers_b)

tc = TableComparison(a, pk_a, b, pk_b)
tc.compare_all()
tc.errors