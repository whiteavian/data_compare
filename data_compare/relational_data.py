HEADER_INDEX = 0
BEGIN_INDEX = 1
STOP = "stop"
ERROR = "error"


class RelationalData (object):
    """This class represents a relational data set and offers comparison to another.

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
        # Compute length once so it does not have to be recomputed every time 
        # matching_row is called.
        self.length = len(data)
        self.pkey = pkey

        self.headers = data[HEADER_INDEX]

        # TODO will this be slow? Would it be (generally) faster to check if it's sorted first?
        self.sort_by_pkey()

    def sort_by_pkey(self):
        """Sort the data by primary key.

        We make a later assumption that our data is in sorted by primary key order.
        Later methods will fail if sorting has not been completed."""
        sorted_no_header = sorted(self.data[BEGIN_INDEX:], key=lambda row: self.pkey_val(row))
        self.data = [self.data[HEADER_INDEX]] + sorted_no_header

    def val(self, row, attr_name):
        """Return the value of the row associated with the given header name."""
        i = self.headers.index(attr_name)
        return row[i]

    def pkey_val(self, row):
        """Return the primary key value of the given row."""
        return self.val(row, self.pkey)

    def compare(self, comparand):
        """Compare this relational data set to the comparand relational data set.

        This comparison is currently limited by the following assumptions:

        1) Only columns with identical names will be compared. At some point
           in the future, we may add the ability to specify column name 
           equivalences.
        2) A given row will only be compared to a row that has the same primary
           key value. A row that does not have a corresponding row in the 
           compared data set will be entered entirely into the errors."""
        # This is the beginning index of the comparand data, starting after the
        # header index.
        comparand_index = BEGIN_INDEX
        self.comparand = comparand
        self.shared_headers = set(self.headers) & set(comparand.headers)

        self.errors = []
        comparand.errors = []

        for row in self.data[BEGIN_INDEX:]:
            # I don't love that we return a tuple here, but I prefer it to setting
            # a variable on the comparand. Can we do better?
            comparand_row, comparand_index = \
                comparand.matching_row(self.pkey_val(row), comparand_index)

            if comparand_row == STOP:
                break
            elif comparand_row == ERROR:
                self.errors.append({'missing_row': row})
            else:
                self.compare_row(row, comparand_row)
                comparand_index += 1

        for row in comparand.data[comparand_index:]:
            self.errors.append({'comparand_missing_row': row})

    def matching_row(self, pkey_to_match, i):
        """Return the row that has the given primary key value.

        This method relies on the sort by primary key we did at initialization.
        If we reach the end the data before finding a match, return instructions
        to cease comparisons. Add each row that has a key less than the given key
        to our errors. If we reach a row that has a key greater than the given key,
        return instructions to add the originating row to the errors."""
        # We assume here that the primary key columns are integers.
        if i == self.length:
            return STOP, i

        row = self.data[i]
        row_pkey = self.val(row, self.pkey)

        if row_pkey < pkey_to_match:
            # As this method is typically called from the comparand, how can we set
            # errors on the original item, and not the comparand?
            self.errors.append({'missing_row': row})
            i += 1
            return self.matching_row(pkey_to_match, i)
        elif row_pkey > pkey_to_match:
            return ERROR, i
        else:
            return row, i

    def compare_row(self, row, comparand_row):
        """Compare the values of each row for each shared header.

        Add both values to our errors if there is a mismatch."""
        for header in self.shared_headers:
            val = self.val(row, header)
            comparand_val = self.comparand.val(comparand_row, header)

            if val != comparand_val:
                self.errors.append({
                    '{}_{}_original'.format(header, self.pkey_val(row)): val,
                    '{}_{}_comparand'.format(header, 
                        self.pkey_val(comparand_row)): comparand_val,
                    })