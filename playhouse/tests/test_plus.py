# encoding=utf-8

import sys
import threading
try:
    from Queue import Queue
except ImportError:
    from queue import Queue

from peewee import OperationalError
from peewee import SqliteDatabase
from playhouse.tests.base import compiler
from playhouse.tests.base import database_class
from playhouse.tests.base import database_initializer
from playhouse.tests.base import ModelTestCase
from playhouse.tests.base import PeeweeTestCase
from playhouse.tests.base import query_db
from playhouse.tests.base import skip_unless
from playhouse.tests.base import test_db
from playhouse.tests.base import ulit
from playhouse.tests.models import *


class DatabaseQueryCounter(object):
    
    def __init__(self, underlying):
    
        orig_execute_sql = underlying.execute_sql
        def new_execute_sql(*args, **kwargs):
            self.__dict__['sql_count'] += 1
            return orig_execute_sql(*args, **kwargs)
        underlying.execute_sql = new_execute_sql
        
        self.underlying = underlying
        self.sql_count = 0
    
    def __getattr__(self, name):
        if name in ('sql_count', 'underlying'):
            return self.__dict__[name]
        if name == 'recent_sql_count':
            c = self.__dict__['sql_count']
            self.__dict__['sql_count'] = 0
            return c
        else:
            return getattr(self.__dict__['underlying'], name)

    def __setattr__(self, name, value):
        if name in ('sql_count', 'underlying'):
            self.__dict__[name] = value
        else:
            setattr(self.underlying, name, value)
        

class TestPlusQueries(ModelTestCase):
    requires = [User, Blog]

    def setUp(self):
        self._orig_db = test_db
        kwargs = {}
        try:  # Some engines need the extra kwargs.
            kwargs.update(test_db.connect_kwargs)
        except:
            pass
        if isinstance(test_db, SqliteDatabase):
            # Put a very large timeout in place to avoid `database is locked`
            # when using SQLite (default is 5).
            kwargs['timeout'] = 30

        self.db = DatabaseQueryCounter(self.new_connection())
        self._orig_dbs = {}
        for tbl in self.requires:
            self._orig_dbs[tbl] = tbl._meta.database
            tbl._meta.database = self.db
        
        super(TestPlusQueries, self).setUp()

    def tearDown(self):
        for tbl, orig_db in self._orig_dbs.items():
          tbl._meta.database = orig_db
        test_db.close()
        super(TestPlusQueries, self).tearDown()

    def test_plus_queries(self):
        self.db.sql_count = 0

        self.assertEqual(User.ALL.count(), 0)
        user = User.create(username='username')
        self.assertEqual(User.ALL.count(), 1)
        self.assertEqual(self.db.recent_sql_count, 3)

        blog = Blog.create(user=user, title='title')
        self.assertEqual(self.db.recent_sql_count, 1)
        
        blog = Blog.ALL.plus(Blog.user).get()
        self.assertEqual(blog.title, 'title')
        self.assertEqual(blog.user.username, 'username')
        self.assertEqual(self.db.recent_sql_count, 1)
        
        

