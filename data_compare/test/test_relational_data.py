from data_compare.relational_data import RelationalData, STOP, ERROR
from unittest import TestCase


class TestRelationalData (TestCase):

	def setUp(self):
		self.rd = RelationalData(
			[('id', 'col1', 'col2'),
			 (1, 'foo', 'loo'),
			 (2, 'out', 'ina'),
			],
			'id')


	def test_matching_row(self):
		self.rd.errors = []
		res, i = self.rd.matching_row(1, 1)
		assert res == (1, 'foo', 'loo')

		res, i = self.rd.matching_row(1, 2)
		assert res == ERROR

		res, i = self.rd.matching_row(3, 2)
		assert res == STOP
