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
# from cryptostore.engines import StorageEngines

import psycopg2
from psycopg2.extras import execute_values

import timeit

LOG = logging.getLogger('cryptostore')


def chunk(iterable, length):
    return (iterable[i: i + length] for i in range(0, len(iterable), length))


class TimescaleDB(Store):
    def __init__(self, config: dict, connection=None):
        self.data = None
        self.host = config.get('host')
        self.user = config.get('user')
        self.pw = config.get('pw')
        self.db = config.get('db')
        self.page_size = config.get('page_size', 5000)
        self.chunk_interval = config.get('chunk_interval', 12)  # in hours
        self.table = None
        self.table_exists = False
        self.conn = None  # StorageEngines.timescaledb.TimeseriesDB(connection)
        self._connect()

    def _connect(self):
        if self.conn is None or not self._is_alive():
            try:
                self.conn = psycopg2.connect(user=self.user, password=self.pw, database=self.db, host=self.host)
            except Exception as e:
                print('Error connecting to timescaledb postgres instance. ' + str(e))

    def _is_alive(self):
        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT 1')
            return True
        except psycopg2.OperationalError:
            return False

    def aggregate(self, data):
        self.data = data

    def write(self, exchange, data_type, pair, timestamp):
        if not self.data:
            return

        used_ts = {}
        for entry in self.data:

            if data_type == L2_BOOK or data_type == L3_BOOK or data_type == TRADES:
                idx = used_ts.get(entry['timestamp'], 0)
                entry['idx'] = None if idx == 0 else idx
                used_ts[entry['timestamp']] = idx + 1

            entry['timestamp'] = dt.fromtimestamp(entry['timestamp'], timezone.utc)
            entry['receipt_timestamp'] = dt.fromtimestamp(entry['receipt_timestamp'], timezone.utc)
            entry['feed'] = entry.get('feed', exchange)
            entry['id'] = entry.get('id')
            entry['pair'] = pair

        table = self._set_table(exchange, data_type, pair)

        self._connect()
        self._create_table_if_not_exists(data_type, table)
        self._insert_data(table, data_type)

    def _set_table(self, exchange, data_type, pair=None):
        """Utility function to normalise table name(s)"""
        self.table = f'{data_type}_{exchange}'.lower()
        return self.table

    def _create_table_if_not_exists(self, data_type, table):
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
                    f'idx INTEGER, ' \
                    f'side VARCHAR, ' \
                    f'id VARCHAR NULL,' \
                    f'amount NUMERIC, ' \
                    f'price NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ, ' \
                    f'PRIMARY KEY (timestamp, feed, pair), ' \
                    f'CONSTRAINT unique_trades_const UNIQUE (timestamp, feed, pair, idx)' \
                    f');'
        elif data_type == TICKER:
            query = f'CREATE TABLE IF NOT EXISTS {table} (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'id VARCHAR NULL, ' \
                    f'bid NUMERIC, ' \
                    f'ask NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ, ' \
                    f'PRIMARY KEY (timestamp, feed, pair)' \
                    f');'
        elif data_type == L3_BOOK or data_type == L2_BOOK:
            query = f'CREATE TABLE IF NOT EXISTS {table} (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'idx INTEGER, ' \
                    f'delta BOOLEAN, ' \
                    f'side VARCHAR, ' \
                    f'id VARCHAR NULL, ' \
                    f'size NUMERIC, ' \
                    f'price NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ, ' \
                    f'CONSTRAINT unique_{data_type}_const UNIQUE (timestamp, feed, pair, idx)' \
                    f');'
        elif data_type == FUNDING:
            return NotImplemented  # TODO: Seems to need to store arbitrary key/value pairs, see influx.py?
        elif data_type == OPEN_INTEREST:
            query = f'CREATE TABLE IF NOT EXISTS {table}  (' \
                    f'timestamp TIMESTAMPTZ NOT NULL, ' \
                    f'feed VARCHAR, ' \
                    f'pair VARCHAR, ' \
                    f'open_interest NUMERIC, ' \
                    f'receipt_timestamp TIMESTAMPTZ, ' \
                    f'PRIMARY KEY (timestamp, feed, pair)' \
                    f');'

        with self.conn.cursor() as cur:
            try:
                cur.execute(query)
                self.conn.commit()
                cur.execute(f'CREATE INDEX ON {table} (feed, pair);')
                cur.execute(f'SELECT create_hypertable(%s, %s);', (table, 'timestamp',))
                if data_type == L2_BOOK or data_type == L3_BOOK or data_type == TRADES:
                    cur.execute(f"SELECT set_chunk_time_interval(%s, %s * interval '1 hours');",
                                (table, self.chunk_interval))
                    compress_sql = f'ALTER TABLE {table} SET (' \
                                   f' timescaledb.compress, ' \
                                   f'timescaledb.compress_segmentby = "feed, pair, idx"' \
                                   f');'
                else:
                    cur.execute(f"SELECT set_chunk_time_interval(%s, %s * interval '1 hours');",
                                (table, self.chunk_interval * 2))
                    compress_sql = f'ALTER TABLE {table} SET (' \
                                   f' timescaledb.compress, ' \
                                   f'timescaledb.compress_segmentby = "feed, pair"' \
                                   f');'

                cur.execute(compress_sql)
                cur.execute(f"SELECT add_compress_chunks_policy(%s, %s * interval '1 hour');",
                            (table, self.chunk_interval * 3))

                self.conn.commit()
            except Exception as e:
                self.conn.rollback()
                with self.conn.cursor as cur:
                    cur.execute("DROP TABLE {table};", table)
                self.conn.commit()
                print('error creating database: ' + str(e))

    def _insert_data(self, table, data_type):
        if self.data is None:
            return

        insert_sql = ''
        template = ''
        if data_type == TRADES:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, idx, side, id, amount, price, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(idx), %(side)s, %(id)s, %(amount)s, %(price)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, idx, side, amount, price, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(idx)s, %(side)s, %(amount)s, %(price)s, %(receipt_timestamp)s)'

        elif data_type == TICKER:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, id, bid, ask, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(id)s, %(bid)s, %(ask)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, bid, ask, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(bid)s, %(ask)s, %(receipt_timestamp)s)'
        elif data_type == L3_BOOK or data_type == L2_BOOK:
            if id in self.data:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, idx, delta, side, id, size, price, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s, %(feed)s, %(pair)s, %(idx)s, %(delta)s, %(side)s, %(id)s, %(size)s, %(price)s, %(receipt_timestamp)s)'
            else:
                insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, idx, delta, side, size, price, receipt_timestamp) VALUES  %s " \
                             f"ON CONFLICT DO NOTHING"
                template = '(%(timestamp)s,  %(feed)s, %(pair)s, %(idx)s, %(delta)s, %(side)s, %(size)s, %(price)s, %(receipt_timestamp)s)'

        elif data_type == FUNDING:
            return NotImplemented
        elif data_type == OPEN_INTEREST:
            insert_sql = f"INSERT INTO {table} (timestamp, feed, pair, open_interest, receipt_timestamp) VALUES  %s " \
                         f"ON CONFLICT DO NOTHING"
            template = '(%(timestamp)s, %(feed)s, %(pair)s, %(open_interest)s, %(receipt_timestamp)s)'

        LOG.info("Writing %d documents to TimescaleDB", len(self.data))
        starttime = timeit.default_timer()
        try:
            with self.conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, insert_sql, self.data, template=template, page_size=self.page_size)
                self.conn.commit()
                LOG.info("Took %fs to write %d documents to TimescaleDB. %f doc/s.",
                         round(timeit.default_timer() - starttime, 6),
                         len(self.data),
                         round(len(self.data) / (timeit.default_timer() - starttime), 6))
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


"""            # old code to possibly use csv import for bulk loading, probably should use asuncpg instead
                
                # use copy_from instead of executemany to ease the strain on the database server importing the data

               # tmp_table_query = f'INSERT INTO {table} ' \
               #                   f'SELECT DISTINCT ON (time) * ' \
               #                   f'FROM source ON CONFLICT DO NOTHING'
                
                columns = list(self.data[0].keys())
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
