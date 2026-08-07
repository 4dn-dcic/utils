import functools
import redis
import redis.exceptions
import datetime
from typing import Union
# Low level utilities for working with Redis


class RedisException(Exception):
    """ Canonical exception raised by this library for any operational failure reported by the
        underlying Redis driver.

        This is the public error contract: callers of create_redis_client, RedisBase and
        dcicutils.redis_tools should catch this exception and never need to import
        redis.exceptions themselves. The originating driver exception is always attached as
        __cause__ for callers that want to inspect it.
    """
    pass


# Driver exceptions that represent an operational failure talking to Redis (connection refused,
# socket timeout, protocol/response error, cluster failure, ...). redis.exceptions.RedisError is
# the root of nearly all of them; RedisClusterException deliberately sits outside that tree, so
# it is named explicitly. Python-level programmer errors (TypeError, AttributeError, ValueError,
# ...) are intentionally NOT listed and propagate unchanged.
REDIS_DRIVER_EXCEPTIONS = (redis.exceptions.RedisError, redis.exceptions.RedisClusterException)


def translate_redis_exceptions(fn):
    """ Decorator translating Redis driver failures into the canonical RedisException.

        Applied to every public operation in this module and in dcicutils.redis_tools so that
        consumers have a single exception type to handle. Exceptions that are not driver
        failures - including RedisException itself, so nesting decorated calls is harmless -
        are re-raised untouched.

    :param fn: function issuing one or more Redis commands
    :return: wrapped function raising RedisException in place of driver exceptions
    """
    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except REDIS_DRIVER_EXCEPTIONS as e:
            raise RedisException(f'Redis operation {fn.__name__} failed:'
                                 f' {e.__class__.__name__}: {e}') from e
    return wrapper


@translate_redis_exceptions
def create_redis_client(*, url: str, ping=True) -> redis.Redis:
    """ Creates a Redis client connecting from a host:port or a URL
        Because this creation pings Redis should re-use this as much as possible, but
        you can skip the ping if you're confident it will come up

        Raises RedisException if the driver cannot connect (or the ping fails).
    """
    r = redis.from_url(url)
    if ping:
        r.ping()
    return r


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
    def _encode_value(value: Union[str, int, float]) -> Union[bytes, int, float]:
        """ Function used to preprocess all values that are strings and encode them as utf-8 bytes
        :param value: any value that can be represented as bytes either directly (numbers, floats)
                      or through encoding (string)
        :return: encoded value
        """
        return value.encode('utf-8') if (value and isinstance(value, str)) else value

    @staticmethod
    def _decode_value(value: Union[bytes, int, float]) -> str:
        """ Function used to post process all values that are bytes and decode them into utf-8 strings
        :param value: any of bytes or other single valued entities (floats, integers)
        :return: decoded value
        """
        return value.decode('utf-8') if (value and isinstance(value, bytes)) else value

    @translate_redis_exceptions
    def info(self) -> dict:
        """ Returns info about the Redis server https://redis.io/commands/info/
        :return: a dictionary of information about the redis server
        """
        return self.redis.info()

    @translate_redis_exceptions
    def set(self, key: str, value: Union[str, int, float], exp: Union[int, datetime.timedelta] = None) -> str:
        """ Sets the given key to the given value https://redis.io/commands/set/
        :param key: string to store value under
        :param value: value mapped to from key
        :param exp: expiration time in seconds as int or datetime.timedelta (optional)
        :return: a "true" string <OK>
        """
        kwargs = {}
        if exp:
            kwargs['ex'] = exp
        return self.redis.set(self._encode_value(key), self._encode_value(value), **kwargs)

    @translate_redis_exceptions
    def get(self, key: str) -> str:
        """ Gets the given key from Redis https://redis.io/commands/get/
        :param key: key to check for a value store in Redis
        :return: that value if mapped or None if key does not exist
        """
        val = self.redis.get(self._encode_value(key))
        if val is not None:
            val = self._decode_value(val)
        return val

    @translate_redis_exceptions
    def set_expiration(self, key: str, t: Union[int, datetime.time]) -> bool:
        """ Sets the TTL of the given key manually
        :param key: key to set TTL
        :param t: time in seconds until expiration (datetime.timedelta or int)
        :returns: True if successful
        """
        return self.redis.expire(key, t, gt=True)

    @translate_redis_exceptions
    def ttl(self, key: str) -> datetime.time:
        """ Gets the TTL of the given key
        :param key: key to get TTL for
        :return: datetime value in seconds
        """
        return self.redis.ttl(key)

    @translate_redis_exceptions
    def delete(self, key: str) -> int:
        """ Deletes the given key from Redis https://redis.io/commands/del/
        :param key: key to delete
        :return: number of keys removed
        """
        return self.redis.delete(self._encode_value(key))

    @translate_redis_exceptions
    def hget(self, key: str, field: str) -> str:
        """ Gets the value of field from hash key https://redis.io/commands/hget/
        :param key: hash key to retrieve field value from
        :param field: sub key within the hash key whose value we'd like to retrieve
        :return: the value key -> field -> value mapped, if it exists
        """
        return self._decode_value(self.redis.hget(self._encode_value(key), self._encode_value(field)))

    @translate_redis_exceptions
    def hgetall(self, key: str) -> dict:
        """ Gets all values of the given hash https://redis.io/commands/hgetall/
        :param key: hash key to grab all values from
        :return: dictionary of values mapped within the given hash key
        """
        encoded_vals = self.redis.hgetall(self._encode_value(key))
        if encoded_vals:
            encoded_vals = {self._decode_value(k): self._decode_value(v) for k, v in encoded_vals.items()}
        return encoded_vals

    @translate_redis_exceptions
    def hset(self, key: str, field: str, value: Union[str, int, float]) -> int:
        """ Sets a single field on a hash key https://redis.io/commands/hset/
        :param key: hash key to set field -> value mapping on
        :param field: field to map under the hash key
        :param value: value to map from field
        :return: number of fields set (1 if successful)
        """
        return self.redis.hset(self._encode_value(key), self._encode_value(field), self._encode_value(value))

    @translate_redis_exceptions
    def hset_multiple(self, key: str, items: dict) -> int:
        """ Sets all k,v pairs in items on hash key https://redis.io/commands/hset/ (variadic form)
        :param key: hash key to store items under
        :param items: k, v pairs to store under the hash key
        :return: number of k, v pairs set (== len(items.items()) if successful)
        """
        encoded_dict = {self._encode_value(k): self._encode_value(v) for k, v in items.items()}
        return self.redis.hset(key, mapping=encoded_dict)

    @translate_redis_exceptions
    def dbsize(self) -> int:
        """ Returns number of keys in redis https://redis.io/commands/dbsize/ """
        return self.redis.dbsize()
