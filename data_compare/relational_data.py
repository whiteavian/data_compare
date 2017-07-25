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

    def __init__(self, data, key):
        """Initialization expects data of lists of lists and the key, a string.

        The formats expected are described more in-depth in the class docstring."""
        self.data = data
        self.length = len(data)
        self.key = key

        self.headers = data[0]

    def val(self, row, attr_name):
        """Return the value of the row associated with the given header name."""
        i = self.headers.index(attr_name)
        return row[i]

    def pk_val(self, row):
        """Return the primary key value of the given row."""
        return getval(self.headers, row, self.key)
