from data_compare.relational_data import RelationalData, STOP, ERROR
from . import DataCompareTestCase


class TestRelationalData (DataCompareTestCase):

    def setUp(self):
        self.headers = ('id', 'col1', 'col2')
        self.rd = RelationalData(
            [self.headers,
             (1, 'foo', 'loo'),
             (2, 'out', 'ina'),
             (3, 'go', 'return'),
             (4, 'ooo', 'ppp'),
            ],
            'id')

        self.rd_comp = RelationalData(
            [self.headers,
             (1, 'oof', 'loo'),
             (2, 'out', 'eee'),
             (3, 'go', 'return'),
             (3, 'nnn', 'lll'),
            ],
            'id')

        self.rd.errors = []
        self.rd.shared_headers = set(['id', 'col1', 'col2'])
        self.rd.comparand = self.rd_comp

    def test_val(self):
        """Test that an arbitrary value is appropriately extracted from a row."""
        row = (4, 'ooo', 'ppp')
        assert self.rd.val(row, self.headers[1]) == row[1]

    def test_pkey_val(self):
        """Test that the primary key is appropriately extracted from a row."""
        row = (2, 'out', 'ina')
        pkey_index = self.headers.index(self.rd.pkey)
        assert self.rd.pkey_val(row) == row[pkey_index]

    def test_matching_row(self):
        res, i = self.rd.matching_row(1, 1)
        assert res == (1, 'foo', 'loo')

        res, i = self.rd.matching_row(1, 2)
        assert res == ERROR

        res, i = self.rd.matching_row(5, 4)
        assert res == STOP

    def test_compare_row(self):
        self.rd.compare_row(
            (1, 'foo', 'loo'),
            (1, 'oof', 'loo')
            )
        assert self.rd.errors[0]['col1_1_original'] == 'foo'
        assert self.rd.errors[0]['col1_1_comparand'] == 'oof'
