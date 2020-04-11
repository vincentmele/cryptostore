'''
Copyright (C) 2018-2020  Bryant Moscon - bmoscon@gmail.com

Please see the LICENSE file for the terms and conditions
associated with this software.

Contributed by Vincent Mele <vincentmele@gmail.com>
'''
import logging

from datetime import timezone, datetime as dt

from cryptofeed.defines import TRADES, L2_BOOK, L3_BOOK, TICKER, FUNDING, OPEN_INTEREST

from cryptostore.data.store import Store
#from cryptostore.engines import StorageEngines

import psycopg2
from psycopg2.extras import execute_values

import timeit

LOG = logging.getLogger('cryptostore')


def chunk(iterable, length):
    return (iterable[i : i + length] for i in range(0, len(iterable), length))


class TimescaleDB(Store):
    def __init__(self, config: dict, connection = None):
        self.data = None
        self.host = config.get('host')
        self.user = config.get('user')
        self.pw = config.get('pw')
        self.db = config.get('db')
        self.table = None
        self.table_exists = False
        self.conn = None  # StorageEngines.timescaledb.TimeseriesDB(connection)
        self._connect()

    def _connect(self):
        if self.conn is None:
            try:
                self.conn = psycopg2.connect(user=self.user, password=self.pw, database=self.db, host=self.host)
            except Exception as e:
                print('Error connecting to timescaledb postgres instance. ' + str(e))

    def aggregate(self, data):
        # We're going to set an idx that increments for each item in the data that has the same timestamp
        # Then, make timestamps conform to postgres
        # Finally, set feed and id to None if they don't exist in the data feed
       # ts_indicies = {}
        for entry in data:

            entry['timestamp'] = dt.fromtimestamp(entry['timestamp'], timezone.utc )
            entry['receipt_timestamp'] = dt.fromtimestamp(entry['receipt_timestamp'], timezone.utc )

            entry['feed'] = entry.get('feed')
            entry['id'] = entry.get('id')

        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        if not self.data:
            return

        for entry in self.data: # todo: is this the best place for this?
            entry['pair'] = pair
            entry['feed'] = exchange

        table = self._set_table(exchange, data_type, pair)

        self._connect()

        self._check_if_table_exists(data_type, table)
        self._insert_data(table, data_type)

    def _set_table(self, exchange, data_type, pair = None):
        """Utility function to normalise table name(s)"""
        self.table = f'{data_type}_{exchange}'.lower()
        return self.table

    def _check_if_table_exists(self, data_type, table, schema='public'): #todo: check schema for hypertables
        if self.table_exists:
            return True
        else:
            with self.conn.cursor() as cur:
                cur.execute('SELECT to_regclass(%s);', (table,))
                if cur.fetchone()[0] is not None:
                    self.table_exists = True
                    return True

            self._create_table(data_type, table)
            self.table_exists = True

    def _create_table(self, data_type, table):
        query = ''
        if data_type == TRADES:
            query = f'CREATE TABLE IF NOT EXISTS {table} (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'side VARCHAR, ' \
                    f'id VARCHAR NULL,' \
                    f'amount NUMERIC, ' \
                    f'price NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ ' \
                    f');'
        elif data_type == TICKER:
            query = f'CREATE TABLE IF NOT EXISTS {table} (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'id VARCHAR NULL, ' \
                    f'bid NUMERIC, ' \
                    f'ask NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ ' \
                    f');'
        elif data_type == L3_BOOK or data_type == L2_BOOK:
            query = f'CREATE TABLE IF NOT EXISTS {table} (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'delta BOOLEAN, ' \
                    f'side VARCHAR, ' \
                    f'id VARCHAR NULL, ' \
                    f'size NUMERIC, ' \
                    f'price NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ ' \
                    f');'
        elif data_type == FUNDING:
            return NotImplemented  # TODO: Seems to need to store arbitrary key/value pairs, see influx.py?
        elif data_type == OPEN_INTEREST:
            query = f'CREATE TABLE IF NOT EXISTS {table}  (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'open_interest NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ ' \
                    f');'

        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                self.conn.commit()
                cur.execute(f'CREATE INDEX ON {table} (feed, pair);')
                cur.execute(f'SELECT create_hypertable(%s, %s);', (table, 'timestamp',))
                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                print('error creating database: ' + str(e))

    def _insert_data(self, table, data_type):
        if self.data is None:
            return
        # use copy_from instead of executemany to ease the strain on the database server importing the data

       # tmp_table_query = f'INSERT INTO {table} ' \
       #                   f'SELECT DISTINCT ON (time) * ' \
       #                   f'FROM source ON CONFLICT DO NOTHING'
        insert_sql = ''
        template = ''
        if data_type == TRADES:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, side, id, amount, price, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(side)s, %(id)s, %(amount)s, %(price)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, side, amount, price, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(side)s, %(amount)s, %(price)s, %(receipt_timestamp)s)'

        elif data_type == TICKER:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, id, bid, ask, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(id)s, %(bid)s, %(ask)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, bid, ask, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(bid)s, %(ask)s, %(receipt_timestamp)s)'
        elif data_type == L3_BOOK or data_type == L2_BOOK:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, delta, side, id, size, price, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(delta)s, %(side)s, %(id)s, %(size)s, %(price)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, delta, side, size, price, receipt_timestamp) VALUES  %s "
                template = '(%(timestamp)s,  %(feed)s, %(pair)s, %(delta)s, %(side)s, %(size)s, %(price)s, %(receipt_timestamp)s)'

        elif data_type == FUNDING:
            return NotImplemented
        elif data_type == OPEN_INTEREST:
            insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, open_interest, receipt_timestamp) VALUES  %s "
            template = '(%(timestamp)s, %(feed)s, %(pair)s, %(open_interest)s, %(receipt_timestamp)s)'

        LOG.info("Writing %d documents to TimescaleDB", len(self.data))
        starttime = timeit.default_timer()
        try:
            with self.conn.cursor() as cur:
                #for c in chunk(self.data, 5000):
                    # For each chunk,
                    # create a csv in memory,
                    # copy csv to temp table,
                    # copy temp table to main table.
                psycopg2.extras.execute_values(cur, insert_sql, self.data, template=template, page_size=5000)
                self.conn.commit()
                LOG.info("Took %f to write %d documents to TimescaleDB.",
                         round(timeit.default_timer() - starttime, 2), len(self.data))
        except Exception as e:
            print(e)
            self.conn.rollback()

        self.data = None

    def get_start_date(self, exchange: str, data_type: str, pair: str) -> float:
        try:
            self._set_table(exchange, data_type, pair)
            msg = f'SELECT time FROM {self.table} WHERE pair = %s ORDER BY time ASC FETCH FIRST ROW ONLY '

            with self.conn.cursor() as cur:
                cur.execute(msg, (pair,))
                time = cur.fetchone()[0]
                return time
        except psycopg2.DatabaseError as e:
            print(f'Error get_start_date: {e}')
        except Exception:
            return None


"""            columns = list(self.data[0].keys())
                with StringIO(newline='') as sio:
                    dict_writer = csv.DictWriter(sio, columns)
                    dict_writer.writeheader()
                    dict_writer.writerows(c)
                    sio.seek(0)

                    cur.execute('CREATE TEMP TABLE source(LIKE %s INCLUDING ALL) ON COMMIT DROP;' % table);

                    copy_sql = "COPY source FROM stdin WITH CSV HEADER DELIMITER as ','"
                    cur.copy_expert(sql=copy_sql, file=sio)

                    cur.execute(tmp_table_query)
                    cur.execute('DROP TABLE source')
                    self.conn.commit()
"""
