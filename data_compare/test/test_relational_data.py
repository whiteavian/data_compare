from data_compare.relational_data import RelationalData, STOP, ERROR
from unittest import TestCase


class TestRelationalData (TestCase):

	def setUp(self):
		self.rd = RelationalData(
			[('id', 'col1', 'col2'),
			 (1, 'foo', 'loo'),
			 (2, 'out', 'ina'),
			 (3, 'go', 'return'),
			 (4, 'ooo', 'ppp'),
			],
			'id')

		self.rd_comp = RelationalData(
			[('id', 'col1', 'col2'),
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
		assert self.rd.val((4, 'ooo', 'ppp'), 'col1') == 'ooo'

	def test_pkey_val(self):
		assert self.rd.pkey_val((2, 'out', 'ina')) == 2

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
