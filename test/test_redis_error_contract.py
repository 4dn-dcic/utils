import datetime
import pytest
import redis.exceptions
from unittest import mock

from dcicutils.redis_utils import RedisBase, RedisException, create_redis_client, translate_redis_exceptions
from dcicutils.redis_tools import RedisSessionToken
from dcicutils import redis_tools


pytestmark = [pytest.mark.unit]


# Representative operational failures the redis driver raises. ConnectionError/TimeoutError cover
# an unreachable or slow server, ResponseError covers a server-side protocol failure,
# BusyLoadingError covers a server that is up but not yet serving, and RedisClusterException is
# deliberately included because it does NOT descend from redis.exceptions.RedisError.
DRIVER_FAILURES = [
    redis.exceptions.ConnectionError('Connection refused'),
    redis.exceptions.TimeoutError('Timeout reading from socket'),
    redis.exceptions.ResponseError('WRONGTYPE Operation against a key holding the wrong kind of value'),
    redis.exceptions.BusyLoadingError('Redis is loading the dataset in memory'),
    redis.exceptions.RedisClusterException('Redis Cluster cannot be connected'),
]

# Errors that indicate a bug in the caller rather than a Redis failure. These must NOT be
# translated - swallowing them into RedisException would hide real defects from consumers.
PROGRAMMER_ERRORS = [
    TypeError('takes 2 positional arguments but 3 were given'),
    AttributeError("'NoneType' object has no attribute 'get'"),
    ValueError('not a valid value'),
    KeyError('missing'),
]

# Every public RedisBase operation, with arguments that succeed against a healthy handle.
REDIS_BASE_OPERATIONS = [
    ('info', ()),
    ('set', ('key', 'value')),
    ('get', ('key',)),
    ('set_expiration', ('key', 60)),
    ('ttl', ('key',)),
    ('delete', ('key',)),
    ('hget', ('key', 'field')),
    ('hgetall', ('key',)),
    ('hset', ('key', 'field', 'value')),
    ('hset_multiple', ('key', {'field': 'value'})),
    ('dbsize', ()),
]


class FailingRedisHandle:
    """ Stand-in for a redis.Redis client where every command raises the given exception.
        Used to inject driver failures without needing a live (or dead) Redis server.
    """

    def __init__(self, error):
        self.error = error

    def __getattr__(self, item):
        def raise_error(*args, **kwargs):
            raise self.error
        return raise_error


def failing_redis_base(error) -> RedisBase:
    """ Builds a RedisBase whose underlying driver always fails with the given exception """
    return RedisBase(FailingRedisHandle(error))


def new_session_token() -> RedisSessionToken:
    return RedisSessionToken(namespace='dcicutils-unit-test', jwt='example-jwt', email='test@example.com')


class TestRedisBaseErrorContract:
    """ Every public RedisBase operation must surface driver failures as RedisException. """

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    @pytest.mark.parametrize('operation,args', REDIS_BASE_OPERATIONS, ids=lambda v: v if isinstance(v, str) else '')
    def test_driver_failures_become_redis_exception(self, operation, args, error):
        rd = failing_redis_base(error)
        with pytest.raises(RedisException):
            getattr(rd, operation)(*args)

    @pytest.mark.parametrize('error', PROGRAMMER_ERRORS, ids=lambda e: type(e).__name__)
    @pytest.mark.parametrize('operation,args', REDIS_BASE_OPERATIONS, ids=lambda v: v if isinstance(v, str) else '')
    def test_programmer_errors_are_not_translated(self, operation, args, error):
        rd = failing_redis_base(error)
        with pytest.raises(type(error)):
            getattr(rd, operation)(*args)

    def test_original_driver_exception_is_chained(self):
        """ Consumers that want the underlying cause can still reach it via __cause__ """
        original = redis.exceptions.ConnectionError('Connection refused')
        rd = failing_redis_base(original)
        with pytest.raises(RedisException) as exc:
            rd.get('key')
        assert exc.value.__cause__ is original
        assert 'ConnectionError' in str(exc.value)

    def test_successful_operations_are_unchanged(self):
        """ The translation layer must be transparent when Redis is healthy """
        handle = mock.MagicMock()
        handle.get.return_value = b'hello'
        handle.set.return_value = True
        handle.delete.return_value = 1
        handle.ttl.return_value = 300
        handle.hgetall.return_value = {b'foo': b'bar'}
        handle.hget.return_value = b'bar'
        handle.dbsize.return_value = 7
        rd = RedisBase(handle)
        assert rd.set('key', 'value') is True
        assert rd.get('key') == 'hello'
        assert rd.delete('key') == 1
        assert rd.ttl('key') == 300
        assert rd.hgetall('key') == {'foo': 'bar'}
        assert rd.hget('key', 'foo') == 'bar'
        assert rd.dbsize() == 7

    def test_get_of_missing_key_still_returns_none(self):
        handle = mock.MagicMock()
        handle.get.return_value = None
        assert RedisBase(handle).get('nope') is None

    def test_docstrings_survive_decoration(self):
        """ The decorator uses functools.wraps, so introspection is preserved """
        assert RedisBase.get.__name__ == 'get'
        assert 'https://redis.io/commands/get/' in RedisBase.get.__doc__


class TestCreateRedisClientErrorContract:

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_ping_failure_becomes_redis_exception(self, error):
        handle = mock.MagicMock()
        handle.ping.side_effect = error
        with mock.patch.object(redis, 'from_url', return_value=handle):
            with pytest.raises(RedisException):
                create_redis_client(url='redis://localhost:6379')

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_connect_failure_becomes_redis_exception(self, error):
        with mock.patch.object(redis, 'from_url', side_effect=error):
            with pytest.raises(RedisException):
                create_redis_client(url='redis://localhost:6379')

    def test_successful_creation_is_unchanged(self):
        handle = mock.MagicMock()
        with mock.patch.object(redis, 'from_url', return_value=handle):
            assert create_redis_client(url='redis://localhost:6379') is handle
            handle.ping.assert_called_once()


class TestSessionTokenErrorContract:
    """ Every session operation - creation, lookup, validation, update and revocation - must
        raise RedisException when the driver fails, so consumers such as Snovault never need
        to import redis.exceptions.
    """

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_store_session_token(self, error):
        with pytest.raises(RedisException):
            new_session_token().store_session_token(redis_handler=failing_redis_base(error))

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_validate_session_token(self, error):
        with pytest.raises(RedisException):
            new_session_token().validate_session_token(redis_handler=failing_redis_base(error))

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_update_session_token(self, error):
        with pytest.raises(RedisException):
            new_session_token().update_session_token(redis_handler=failing_redis_base(error),
                                                     jwt='new-jwt', email='test@example.com')

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_delete_session_token(self, error):
        with pytest.raises(RedisException):
            new_session_token().delete_session_token(redis_handler=failing_redis_base(error))

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_from_redis_get_failure(self, error):
        with pytest.raises(RedisException):
            RedisSessionToken.from_redis(redis_handler=failing_redis_base(error),
                                         namespace='dcicutils-unit-test', token='some-token')

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_from_redis_ttl_failure(self, error):
        """ The record is found but reading its TTL fails - still a RedisException, not a
            half-built session object.
        """
        handle = mock.MagicMock()
        handle.get.return_value = b'example-jwt:test@example.com'
        handle.ttl.side_effect = error
        with pytest.raises(RedisException):
            RedisSessionToken.from_redis(redis_handler=RedisBase(handle),
                                         namespace='dcicutils-unit-test', token='some-token')

    @pytest.mark.parametrize('error', DRIVER_FAILURES, ids=lambda e: type(e).__name__)
    def test_session_ops_translate_driver_errors_from_a_raw_handler(self, error):
        """ Session operations are decorated in their own right, so a driver exception raised by
            a handler that is not a RedisBase is translated too.
        """
        token = new_session_token()
        raw = FailingRedisHandle(error)
        for call in [lambda: token.store_session_token(redis_handler=raw),
                     lambda: token.validate_session_token(redis_handler=raw),
                     lambda: token.delete_session_token(redis_handler=raw),
                     lambda: token.update_session_token(redis_handler=raw, jwt='j', email='e')]:
            with pytest.raises(RedisException):
                call()

    @pytest.mark.parametrize('error', PROGRAMMER_ERRORS, ids=lambda e: type(e).__name__)
    def test_session_ops_do_not_translate_programmer_errors(self, error):
        token = new_session_token()
        rd = failing_redis_base(error)
        for call in [lambda: token.store_session_token(redis_handler=rd),
                     lambda: token.validate_session_token(redis_handler=rd),
                     lambda: token.delete_session_token(redis_handler=rd),
                     lambda: token.update_session_token(redis_handler=rd, jwt='j', email='e'),
                     lambda: RedisSessionToken.from_redis(redis_handler=rd, namespace='n', token='t')]:
            with pytest.raises(type(error)):
                call()

    def test_validate_distinguishes_absence_from_failure(self):
        """ A missing token is False; an unreachable Redis raises. These must not collapse. """
        handle = mock.MagicMock()
        handle.get.return_value = None
        assert new_session_token().validate_session_token(redis_handler=RedisBase(handle)) is False
        with pytest.raises(RedisException):
            new_session_token().validate_session_token(
                redis_handler=failing_redis_base(redis.exceptions.ConnectionError('nope')))

    def test_from_redis_missing_entry_still_returns_none(self):
        handle = mock.MagicMock()
        handle.get.return_value = None
        assert RedisSessionToken.from_redis(redis_handler=RedisBase(handle),
                                            namespace='dcicutils-unit-test', token='some-token') is None

    def test_successful_session_lifecycle_is_unchanged(self):
        """ Store/validate/update/delete against a healthy handle behave exactly as before """
        handle = mock.MagicMock()
        handle.set.return_value = True
        handle.get.return_value = b'example-jwt:test@example.com'
        handle.delete.return_value = 1
        handle.ttl.return_value = 300
        rd = RedisBase(handle)
        token = new_session_token()
        assert token.store_session_token(redis_handler=rd) is True
        assert token.validate_session_token(redis_handler=rd) is True
        old_key = token.get_redis_key()
        assert token.update_session_token(redis_handler=rd, jwt='new-jwt', email='test@example.com') is True
        assert token.get_redis_key() != old_key
        assert token.get_jwt() == 'new-jwt'
        assert token.delete_session_token(redis_handler=rd) is True
        handle.delete.return_value = 0
        assert token.delete_session_token(redis_handler=rd) is False

    def test_from_redis_round_trip_is_unchanged(self):
        handle = mock.MagicMock()
        handle.get.return_value = b'example-jwt:test@example.com'
        handle.ttl.return_value = 300
        restored = RedisSessionToken.from_redis(redis_handler=RedisBase(handle),
                                                namespace='dcicutils-unit-test', token='some-token')
        assert restored.get_jwt() == 'example-jwt'
        assert restored.get_email() == 'test@example.com'
        assert restored.get_expiration() == 300


class TestRedisExceptionContractSurface:
    """ Public API compatibility guarantees consumers (notably Snovault) rely on. """

    def test_redis_exception_is_importable_from_both_modules(self):
        assert redis_tools.RedisException is RedisException

    def test_redis_exception_is_not_a_driver_exception(self):
        """ RedisException must be catchable without importing redis.exceptions """
        assert issubclass(RedisException, Exception)
        assert not issubclass(RedisException, redis.exceptions.RedisError)

    def test_redis_exception_can_be_raised_without_arguments(self):
        """ Backwards compatibility - older code constructs RedisException() bare """
        with pytest.raises(RedisException):
            raise RedisException()

    def test_translation_is_idempotent(self):
        """ Nesting decorated calls must not re-wrap an already-canonical RedisException """
        original = RedisException('already translated')

        @translate_redis_exceptions
        def inner():
            raise original

        @translate_redis_exceptions
        def outer():
            return inner()

        with pytest.raises(RedisException) as exc:
            outer()
        assert exc.value is original

    def test_translation_preserves_return_value(self):
        @translate_redis_exceptions
        def op(a, b=2):
            """ some docstring """
            return a + b

        assert op(1) == 3
        assert op(1, b=10) == 11
        assert op.__name__ == 'op'
        assert 'some docstring' in op.__doc__

    def test_every_public_redis_base_operation_is_covered(self):
        """ Guards against a new RedisBase method being added without error translation """
        public_methods = {name for name in vars(RedisBase)
                          if not name.startswith('_') and callable(vars(RedisBase)[name])}
        assert public_methods == {name for name, _ in REDIS_BASE_OPERATIONS}

    def test_datetime_expiration_still_accepted(self):
        """ Sanity check that decoration did not disturb keyword handling """
        handle = mock.MagicMock()
        handle.set.return_value = True
        rd = RedisBase(handle)
        exp = datetime.timedelta(seconds=30)
        assert rd.set('key', 'value', exp=exp) is True
        assert handle.set.call_args.kwargs['ex'] == exp
