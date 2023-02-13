import datetime
from unittest import mock
from dcicutils.redis_utils import RedisBase
from dcicutils.redis_tools import RedisSessionToken


class TestRedisSession:
    """ Uses redisdb and Session abstraction on top of RedisBase to implement some APIs for managing sessions
        using Redis """
    NAMESPACE = 'snovault-unit-test'
    DUMMY_EMAIL = 'snovault@test.com'
    DUMMY_JWT = 'example'

    # normal method to fit the mock structure
    def mock_build_session_expiration(self):  # noQA
        """ Simulate an expired datetime when validating """
        return str(datetime.datetime.utcnow() - datetime.timedelta(minutes=1))

    def test_redis_session_basic(self, redisdb):
        """ Generate a session, validate it, test validation failure cases """
        rd = RedisBase(redisdb)
        session_token = RedisSessionToken(
            namespace=self.NAMESPACE,
            email=self.DUMMY_EMAIL,
            jwt=self.DUMMY_JWT
        )
        session_token.store_session_token(redis_handler=rd)
        # passing token just built should validate
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)
        # invalid token should fail
        assert not session_token.validate_session_token(redis_handler=rd,
                                                        token='blah')
        # update with a new token and expiration
        old_token = session_token.session_token
        session_token.update_session_token(redis_handler=rd, jwt=self.DUMMY_JWT)
        assert not session_token.validate_session_token(redis_handler=rd,
                                                        token=old_token)
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)

    def test_redis_session_expired_token(self, redisdb):
        """ Tests that when patching in a function that will generate an expired timestamp
            session token validation will fail.
        """
        rd = RedisBase(redisdb)
        with mock.patch.object(RedisSessionToken, '_build_session_expiration', self.mock_build_session_expiration):
            session_token = RedisSessionToken(
                namespace=self.NAMESPACE,
                email=self.DUMMY_EMAIL,
                jwt=self.DUMMY_JWT
            )
            session_token.store_session_token(redis_handler=rd)
            assert not session_token.validate_session_token(redis_handler=rd,
                                                            token=session_token.session_token)
        # update then should validate
        session_token.update_session_token(redis_handler=rd, jwt=self.DUMMY_JWT)
        assert session_token.validate_session_token(redis_handler=rd,
                                                    token=session_token.session_token)

    def test_redis_session_many_sessions(self, redisdb):
        """ Tests generating and pushing many session objects into Redis and checking
            that they do not validate against one another.
        """
        rd = RedisBase(redisdb)
        sessions = []
        emails = [f'snovault{n}@test.com' for n in range(5)]
        for email in emails:
            session_token = RedisSessionToken(
                namespace=self.NAMESPACE,
                email=email,
                jwt=self.DUMMY_JWT
            )
            session_token.store_session_token(redis_handler=rd)
            sessions.append(session_token)
        assert rd.dbsize() == 5
        # check all sessions work
        tokens = []
        for session in sessions:
            assert session.validate_session_token(redis_handler=rd, token=session.session_token)
            tokens.append(session.session_token)
        # check tokens don't work with wrong session
        for session, token in zip(sessions, tokens[::-1]):
            if session.session_token != token:  # all but middle should fail
                assert not session.validate_session_token(redis_handler=rd, token=token)
            else:
                assert session.validate_session_token(redis_handler=rd, token=token)
        # invalidate some tokens, check that they don't work while others still do
        for idx, session in enumerate(sessions):
            if idx % 2:
                session.delete_session_token(redis_handler=rd)
        for idx, session in enumerate(sessions):
            if idx % 2:
                assert not session.validate_session_token(redis_handler=rd, token=session.session_token)
            else:
                assert session.validate_session_token(redis_handler=rd, token=session.session_token)

    def test_redis_session_from_redis_equality(self, redisdb):
        """ Tests generating a session then grabbing that same session from Redis, assuring
            that they result in the same downstream session object.
        """
        rd = RedisBase(redisdb)
        session_token_local = RedisSessionToken(
            namespace=self.NAMESPACE,
            email=self.DUMMY_EMAIL,
            jwt=self.DUMMY_JWT
        )
        session_token_local.store_session_token(redis_handler=rd)
        session_token_remote = RedisSessionToken.from_redis(redis_handler=rd, namespace=self.NAMESPACE,
                                                            email=self.DUMMY_EMAIL)
        assert rd.dbsize() == 1
        assert session_token_remote == session_token_local
        session_token_remote.delete_session_token(redis_handler=rd)
        assert rd.dbsize() == 0
