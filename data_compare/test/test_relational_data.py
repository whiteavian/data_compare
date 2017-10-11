from data_compare.relational_data import (
    BEGIN_INDEX,
    COMPARAND_MISSING_ROWS,
    ERROR,
    MISSING_ROWS,
    RelationalData,
    STOP,
)
from unittest import TestCase


class TestRelationalData (TestCase):

    def setUp(self):
        self.headers = ('id', 'col1', 'col2')
        self.rd = RelationalData(
            [self.headers,
             (1, 'foo', 'loo'),
             (2, 'out', 'ina'),
             (3, 'go', 'return'),
            ],
            'id')

        self.records_to_add_to_rd_comp = [(5, 'nnn', 'lll')]
        self.records_to_add_to_rd = [(4, 'ooo', 'ppp')]
        rd_comp_data = self.rd.data + self.records_to_add_to_rd_comp
        self.rd_comp = RelationalData(rd_comp_data, self.rd.pkey)
        self.rd.data.extend(self.records_to_add_to_rd)
        self.records_to_modify_in_rd_comp = [
             (1, 'oof', 'loo'),
             (2, 'out', 'eee'),
            ]

        self.rd.shared_headers = set(['id', 'col1', 'col2'])
        self.rd.comparand = self.rd_comp

    def test_sort_by_pkey(self):
        """Ensure the primary key of each row is strictly less than each succeeding row."""
        rd = RelationalData(
            [self.headers,
             (4, 'ooo', 'ppp'),
             (2, 'out', 'ina'),
             (1, 'foo', 'loo'),
             (3, 'go', 'return'),
            ],
            'id')

        for row in rd.data[BEGIN_INDEX:-1]:
            next_row_index = rd.data.index(row) + 1
            next_row = rd.data[next_row_index]
            assert rd.pkey_val(row) < rd.pkey_val(next_row)

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

    def test_compare(self):
        self.rd.compare(self.rd_comp)
        assert self.rd.errors
        assert self.rd.errors[MISSING_ROWS].keys() == self.records_to_add_to_rd
        assert self.rd.errors[COMPARAND_MISSING_ROWS].keys() == self.records_to_add_to_rd_comp


def modify_relational_data(relational_data, modifiers):
    pass
