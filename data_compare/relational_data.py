HEADER_INDEX = 0
BEGIN_INDEX = 1
STOP = "stop"
ERROR = "error"


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

    def __init__(self, data, pkey):
        """Initialization expects data of lists of lists and the key, a string.

        The formats expected are described more in-depth in the class docstring."""
        self.data = data
        self.length = len(data)
        self.pkey = pkey
        # Make sure the data is sorted asc by pk.

        self.headers = data[HEADER_INDEX]

    def val(self, row, attr_name):
        """Return the value of the row associated with the given header name."""
        i = self.headers.index(attr_name)
        return row[i]

    def pkey_val(self, row):
        """Return the primary key value of the given row."""
        return self.val(self.headers, row, self.key)

    def compare(self, comparator):
        """Compare this relational data set to the given relational data set.

        This comparison is currently limited by the following assumptions:

        1) Only columns with identical names will be compared. At some point
           in the future, we may add the ability to specify column name 
           equivalences.
        2) A given row will only be compared to a row that has the same primary
           key value. A row that does not have a corresponding row in the 
           compared data set will be entered entirely into the errors."""
        # This is the beginning index of the comparator data, starting after the
        # header index.
        self.comparator.begin_index = BEGIN_INDEX
        self.comparator = comparator
        self.shared_headers = set(self.headers) & set(comparator.headers)
        self.errors = []

        for row in self.data[BEGIN_INDEX:]:
            comparator_row = comparator.matching_row(self.pkey_val(row))

            if comparator_row == STOP:
                break
            elif comparator_row == ERROR:
                self.errors['missing_rows'] = row
            else:
                self.compare_row(row, comparator_row)
                self.comparator_index += 1

    def matching_row(self, pkey_to_match):
        """Return the row that has the given primary key value.

        This method relies on the sort by primary key we did at initialization.
        If we reach the end the data before finding a match, return instructions
        to cease comparisons. Add each """
        # We assume here that the primary key columns are integers.
        row = self.data[self.begin_index]
        row_pkey = self.val(self.headers, row, self.pkey)

        if self.begin_index == self.length:
            return STOP

        if row_pkey < pkey_to_match:
            self.errors['missing_rows'] = row
            self.begin_index += 1
            return self.matching_row(pkey_to_match)
        elif row_pkey > pkey_to_match:
            return ERROR
        else:
            return row

    def compare_row(self, row, comparator_row):
        for header in self.shared_headers:
            val = self.val(row, header)
            comparator_val = comparator.val(row, header)

            if val != comparator_val:
                self.errors.append({
                    '{}_{}_original'.format(header, self.pkey_val(row)): val,
                    '{}_{}_comparator'.format(header, self.pkey_val(comparator_row)): comparator_val,
                    })