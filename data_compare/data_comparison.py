# Eventual TODO: account for different data types
# Ex: utf8 vs latin1
# Boolean 0 == False, etc
# Eventual TODO: suggest rows are the same based on similarity of contents.
# Is there any reason why we would actually want that?
STOP = "stop"
ERROR = "error"

class DataComparison (object):

    def __init__(self, a, key_a, b, key_b):
        self.a = a
        self.b = b
        # Compute this here to not have to recompute every time we call
        # matching_row_b.
        self.length_b = len(b)

        self.headers_a = a[0]
        self.headers_b = b[0]

        self.shared_headers = set(self.headers_a) & set(self.headers_b)
        self.errors = []

        self.index_b = 1

        self.key_a = key_a
        self.key_b = key_b

    def compare_all(self):
        for row_a in self.a[1:]:
            row_pkey_a = getval(self.headers_a, row_a, self.key_a)
            row_b = self.matching_row_b(row_pkey_a)
            if row_b == STOP:
                break
            elif row_b == ERROR:
                self.errors.append({"a": row_a})
            else:
                self.compare_row(row_a, row_b)
                self.index_b += 1

    def matching_row_b(self, row_pkey_a):
        # We assume here that the primary key columns are integers.
        row_b = self.b[self.index_b]
        row_pkey_b = getval(self.headers_b, row_b, self.key_b)

        if self.index_b == self.length_b:
            return STOP

        if row_pkey_b < row_pkey_a:
            self.errors.append({"b": row_b})
            self.index_b += 1
            return self.matching_row_b(row_pkey_a)
        elif row_pkey_b > row_pkey_a:
            return ERROR
        else:
            return row_b

    def get_pk_val(self, row, a_or_b):
        if a_or_b == 'a':
            return getval(self.headers_a, row, self.key_a)
        elif a_or_b == 'b':
            return getval(self.headers_b, row, self.key_b)

    def compare_row(self, row_a, row_b):
        for header in self.shared_headers:
            val_a = getval(self.headers_a, row_a, header)
            val_b = getval(self.headers_b, row_b, header)

            if val_a != val_b:
                self.errors.append({
                    '{}_{}_a'.format(header, self.get_pk_val(row_a, 'a')): val_a,
                    '{}_{}_b'.format(header, self.get_pk_val(row_b, 'b')): val_b,
                    })

def getval(headers, tup, attr_name):
    i = headers.index(attr_name)
    return tup[i]