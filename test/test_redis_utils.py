import json
import time
import datetime
from dcicutils.redis_utils import RedisBase


class TestRedisBase:
    """ Uses redisdb, function scope fixture that automatically destroys DB on every run """

    def test_redis_simple(self, redisdb):
        """ Tests getting/setting simple string keys. """
        rd = RedisBase(redisdb)
        assert rd.set('hello', 'world')
        assert rd.get('hello') == 'world'
        assert rd.get('world') is None
        assert rd.dbsize() == 1

    def test_redis_hset_hgetall(self, redisdb):
        """ Builds a simple object and tests using it. """
        rd = RedisBase(redisdb)
        my_key = 'foobar'
        assert rd.hset(my_key, 'foo', 'bar')
        assert 'foo' in rd.hgetall(my_key)
        assert rd.dbsize() == 1
        rd.delete(my_key)
        assert rd.hgetall(my_key) == {}

    def test_redis_hset_multiple(self, redisdb):
        """ Builds a record with multiple hset entries in a single call """
        rd = RedisBase(redisdb)
        my_key = 'foobar'
        n_set = rd.hset_multiple(my_key, {
            'foo': 'bar',
            'bar': 'foo',
            'bazz': 5
        })
        assert n_set == 3
        res = rd.hgetall(my_key)
        assert 'foo' in res
        assert 'bar' in res
        assert 'bazz' in res
        n_set = rd.hset_multiple(my_key, {
            'hello': 'world'
        })
        assert n_set == 1
        res = rd.hgetall(my_key)
        assert res['hello'] == 'world'
        assert rd.hget(my_key, 'hello') == 'world'
        assert rd.hget(my_key, 'blah') is None
        assert rd.hget('notakey', 'notavalue') is None

    def test_redis_hset_hgetall_complex(self, redisdb):
        """ Builds a complex object resembling our structure """
        rd = RedisBase(redisdb)
        my_key_meta = 'snovault:items:uuid:meta'
        obj = {
            'this is': 'an object',
            'with multiple': 'fields'
        }

        def build_item_metadata():
            rd.hset(my_key_meta, 'dirty', 0)
            rd.hset(my_key_meta, 'item_type', 'Sample')
            rd.hset(my_key_meta, 'properties', json.dumps(obj))
        build_item_metadata()

        res = rd.hgetall(my_key_meta)
        assert res['dirty'] == '0'
        assert res['item_type'] == 'Sample'
        assert json.loads(res['properties']) == obj

    def test_redis_set(self, redisdb):
        """ Tests some simple sets with expiration """
        rd = RedisBase(redisdb)
        my_key_meta = 'snovault:items:uuid:meta'
        rd.set(my_key_meta, 'hello')
        assert rd.get(my_key_meta) == 'hello'
        rd.delete(my_key_meta)
        # test exp with datetime
        exp = datetime.timedelta(seconds=2)
        rd.set(my_key_meta, 'hello', exp=exp)
        assert rd.get(my_key_meta) == 'hello'
        time.sleep(3)
        assert not rd.get(my_key_meta)
        # test exp with integer
        exp = 2
        rd.set(my_key_meta, 'hello', exp=exp)
        assert rd.get(my_key_meta) == 'hello'
        time.sleep(3)
        assert not rd.get(my_key_meta)
