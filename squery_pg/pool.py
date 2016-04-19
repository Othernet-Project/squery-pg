"""
pool.py: Connection Pool for PostgreSQL

Based on https://github.com/gevent/gevent/blob/master/examples/psycopg2_pool.py
"""

from __future__ import print_function
import sys
import contextlib

import gevent
from gevent.queue import Queue
from gevent.socket import wait_read, wait_write
from psycopg2 import extensions, OperationalError, connect


if sys.version_info[0] >= 3:
    integer_types = int,
else:
    import __builtin__
    integer_types = int, __builtin__.long


def gevent_wait_callback(conn, timeout=None):
    """A wait callback useful to allow gevent to work with Psycopg."""
    while 1:
        state = conn.poll()
        if state == extensions.POLL_OK:
            break
        elif state == extensions.POLL_READ:
            wait_read(conn.fileno(), timeout=timeout)
        elif state == extensions.POLL_WRITE:
            wait_write(conn.fileno(), timeout=timeout)
        else:
            raise OperationalError(
                "Bad result from poll: %r" % state)


extensions.set_wait_callback(gevent_wait_callback)


class DatabaseConnectionPool(object):

    def __new__(cls, *args, **kwargs):
        cls = super(DatabaseConnectionPool, cls).__new__(cls, *args, **kwargs)
        # Depending on the maxsize argument, we will set up the class to be
        # either a fake connection pool that uses only one connection object,
        # or a real connection pool powered by gevent.
        if kwargs.get('maxsize') == 1:
            cls.get = cls.single_get
            cls.put = cls.single_put
            cls.closeall = cls.single_closeall
        else:
            cls.get = cls.multi_get
            cls.put = cls.multi_put
            cls.closeall = cls.multi_closeall
        return cls

    def __init__(self, maxsize=100):
        if not isinstance(maxsize, integer_types):
            raise TypeError('Expected integer, got %r' % (maxsize, ))
        self.maxsize = maxsize
        if self.maxsize == 1:
            self._conn = None
            self.pool = None
        else:
            self.pool = Queue()
        self.size = 0

    # The following methods are used for single-connection mode.

    def single_get(self):
        if not self._conn or self._conn.closed:
            self._conn = self.create_connection()
        return self._conn

    def single_put(self, item):
        self._conn = item

    def single_closeall(self):
        try:
            self._conn.close()
        except Exception:
            pass

    # The following methods are used for real connection pool.

    def multi_get(self):
        pool = self.pool
        if self.size >= self.maxsize or pool.qsize():
            return pool.get()
        else:
            self.size += 1
            try:
                new_item = self.create_connection()
            except:
                self.size -= 1
                raise
            return new_item

    def multi_put(self, item):
        self.pool.put(item)

    def multi_closeall(self):
        while not self.pool.empty():
            conn = self.pool.get_nowait()
            try:
                conn.close()
            except Exception:
                pass

    @contextlib.contextmanager
    def connection(self, isolation_level=None):
        conn = self.get()
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
            yield conn
        except:
            if conn.closed:
                conn = None
                self.closeall()
            else:
                conn = self._rollback(conn)
            raise
        else:
            if conn.closed:
                raise OperationalError("Cannot commit because connection "
                                       "was closed: %r" % (conn, ))
            conn.commit()
        finally:
            if conn is not None and not conn.closed:
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.put(conn)

    @contextlib.contextmanager
    def cursor(self, *args, **kwargs):
        isolation_level = kwargs.pop('isolation_level', None)
        with self.connection(isolation_level) as conn:
            yield conn.cursor(*args, **kwargs)

    def _rollback(self, conn):
        try:
            conn.rollback()
        except:
            gevent.get_hub().handle_error(conn, *sys.exc_info())
            return
        return conn

    def execute(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.rowcount

    def executemany(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.executemany(*args)
            return cursor.rowcount

    def fetchone(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchone()

    def fetchall(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            return cursor.fetchall()

    def fetchiter(self, *args, **kwargs):
        with self.cursor(**kwargs) as cursor:
            cursor.execute(*args)
            while True:
                items = cursor.fetchmany()
                if not items:
                    break
                for item in items:
                    yield item


class PostgresConnectionPool(DatabaseConnectionPool):

    def __init__(self, *args, **kwargs):
        # separate the pool's constructor parameters from the connection's
        self.connect = kwargs.pop('connect', connect)
        maxsize = kwargs.pop('maxsize', None)
        self.args = args
        self.kwargs = kwargs
        # initialize pool
        pool_kwargs = dict()
        if maxsize is not None:
            pool_kwargs.update(maxsize=maxsize)

        super(PostgresConnectionPool, self).__init__(**pool_kwargs)

    def create_connection(self):
        return self.connect(*self.args, **self.kwargs)
