class RelationalData (object):
    """This class represents a relational data set.

    This class expects to be handed a list-like object of same-length list-
    like objects. The first entry represents names that correspond to the items
    of the same index. One such name should correspond to a unique entry per each
    row (as compared to that entry of each other row), in other words, the primary
    key.
    
    For example:
    
    [
    ('id', 'type', 'value'),
    (1, 'cat', 5),
    (2, 'dog', 3),
    ]

    One means of achieving such a data set is by querying a SQL database with 
    SQLAlchemy and prepending the column names. We can then use the comparison 
    methods to determine the differences between the data sets (each a 
    RelationalData object).
    """

    def __init__(self, data, pk):
        """Initialization expects data of lists of lists and the key, a string.

        The formats expected are described more in-depth in the class docstring."""
        self.data = data
        self.length = len(data)
        self.pk = pk
        # Make sure the data is sorted asc by pk.

        self.headers = data[0]

    def val(self, row, attr_name):
        """Return the value of the row associated with the given header name."""
        i = self.headers.index(attr_name)
        return row[i]

    def pk_val(self, row):
        """Return the primary key value of the given row."""
        return self.val(self.headers, row, self.key)

    def compare(self, rd):
        """Compare this relational data set to the given relational data set.

        This comparison is currently limited by the following assumptions:

        1) Only columns with identical names will be compared. At some point
           in the future, we may add the ability to specify column name 
           equivalences.
        2) A given row will only be compared to a row that has the same primary
           key value. A row that does not have a corresponding row in the 
           compared data set will be entered entirely into the errors."""
        for row_a in self.data[1:]:
            row_b = rd.matching_row(self.pk_val(row))
            if row_b == STOP:
                break
            elif row_b == ERROR:
                self.errors.append({"a": row_a})
            else:
                self.compare_row(row_a, row_b)
                self.index_b += 1

    def matching_row_b(self, pk_val):
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