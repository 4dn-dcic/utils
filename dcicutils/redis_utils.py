import redis
# Low level utilities for working with Redis


def create_redis_client(*, url, ping=True):
    """ Creates a Redis client connecting from a host:port or a URL
        Because this creation pings Redis should re-use this as much as possible, but
        you can skip the ping if you're confident it will come up
    """
    r = redis.from_url(url)
    if ping:
        r.ping()
    return r


class RedisException(Exception):
    pass


class RedisBase(object):
    """ This class contains low level methods meant to implement useful Redis APIs. The idea is these functions
        are used to implement the methods needed in Redis Tools.

        One important note about Redis - the database wants bytes like objects in the
        place of strings, so some automatic encoding/decoding from utf-8 is implemented
        here and thus should NOT be done by the caller. If the caller does do it though
        it is assumed it has been done correctly, or is of a type (ie: float) that does
        not require encoding/decoding.

        TODO: implement more APIs as needed - Redis is very complex and offers a lot of
              potentially useful functionality, and it may be necessary to split off into
              sub classes later on to isolate in a reasonable manner, but for now all here
    """

    def __init__(self, redis_handle):
        self.redis = redis_handle

    @staticmethod
    def _encode_value(value):
        """ Function used to preprocess all values that are strings and encode them as utf-8 bytes """
        return value.encode('utf-8') if (value and isinstance(value, str)) else value

    @staticmethod
    def _decode_value(value):
        """ Function used to post process all values that are bytes and decode them into utf-8 strings """
        return value.decode('utf-8') if (value and isinstance(value, bytes)) else value

    def info(self):
        """ Returns info about the Redis server """
        return self.redis.info()

    def set(self, key, value):
        """ Sets the given key to the given value. """
        return self.redis.set(self._encode_value(key), self._encode_value(value))

    def get(self, key: str):
        """ Gets the given key from Redis. """
        val = self.redis.get(self._encode_value(key))
        if val is not None:
            val = self._decode_value(val)
        return val

    def delete(self, key):
        """ Deletes the given key from Redis. """
        self.redis.delete(self._encode_value(key))

    def hget(self, key, field):
        """ Gets the value of field from hash key """
        return self._decode_value(self.redis.hget(self._encode_value(key), self._encode_value(field)))

    def hgetall(self, key):
        """ Gets all values of the given hash. """
        encoded_vals = self.redis.hgetall(self._encode_value(key))
        if encoded_vals:
            encoded_vals = {self._decode_value(k): self._decode_value(v) for k, v in encoded_vals.items()}
        return encoded_vals

    def hset(self, key, field, value):
        """ Sets a single field on a hash key. """
        return self.redis.hset(self._encode_value(key), self._encode_value(field), self._encode_value(value))

    def hset_multiple(self, key, items):
        """ Sets all k,v pairs in items on hash key. """
        encoded_dict = {self._encode_value(k): self._encode_value(v) for k, v in items.items()}
        return self.redis.hset(key, mapping=encoded_dict)

    def dbsize(self):
        """ Returns number of keys in redis """
        return self.redis.dbsize()
