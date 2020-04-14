import unittest

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST
from cryptostore.data.timescaledb import TimescaleDB


class MyTestCase(unittest.TestCase):

    def test_connect(self):
        config = {'host': '127.0.0.1', 'user': 'XXXXXXXXX', 'pw': 'XXXXXXXXXXX', 'db': 'XXXXXXXXXXXXX'}
        td = TimescaleDB(config)
        self.assertIsNotNone(td)
        self.assertEqual(td.conn.status, 1)



if __name__ == '__main__':
    unittest.main()
