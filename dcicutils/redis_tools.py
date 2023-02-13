import secrets
import datetime
import structlog
from dcicutils.redis_utils import RedisBase, RedisException


log = structlog.getLogger(__name__)


def make_session_token(n_bytes=32):
    """ Uses the secrets module to create a cryptographically secure and URL safe string """
    return secrets.token_urlsafe(n_bytes)


class RedisSessionToken:
    """
    Model used by Redis to store session tokens
    Keystore structure:
        <env_namespace>:session:email
            -> Redis hset containing the associated JWT, session token and expiration time (3 hours)
    """
    JWT = 'jwt'
    SESSION = 'session_token'
    EXPIRATION = 'expiration'

    @staticmethod
    def _build_session_expiration():
        """ Builds a session expiration date 3 hours after generation """
        return str(datetime.datetime.utcnow() + datetime.timedelta(hours=3))

    def _build_session_hset(self, jwt, token, expiration=None):
        """ Builds Redis hset record for the session token """
        return {
            self.JWT: jwt,
            self.SESSION: token,
            self.EXPIRATION: self._build_session_expiration() if not expiration else expiration
        }

    def __init__(self, *, namespace, email, jwt, token=None, expiration=None):
        """ Creates a Redis Session object, storing a hash of the JWT into Redis and returning this
            value as the session token.
        """
        self.redis_key = f'{namespace}:session:{email}'
        self.email = email
        self.jwt = jwt
        if token:
            self.session_token = token
        else:
            self.session_token = make_session_token()
        self.session_hset = self._build_session_hset(self.jwt, self.session_token, expiration=expiration)

    def __eq__(self, other):
        """ Evaluates equality of two session objects based on the value of the session hset """
        return self.session_hset == other.session_hset

    @classmethod
    def from_redis(cls, *, redis_handler, namespace, email):
        """ Builds a RedisSessionToken from an existing record """
        redis_key = f'{namespace}:session:{email}'
        redis_token = redis_handler.hgetall(redis_key)
        return cls(namespace=namespace, email=email, jwt=redis_token[cls.JWT],
                   token=redis_token[cls.SESSION],
                   expiration=redis_token[cls.EXPIRATION])

    def store_session_token(self, *, redis_handler: RedisBase) -> bool:
        """ Stores the created session token object as an hset in Redis """
        try:
            redis_handler.hset_multiple(self.redis_key, self.session_hset)
        except Exception as e:
            log.error(str(e))
            raise RedisException()
        return True

    def validate_session_token(self, *, redis_handler: RedisBase, token) -> bool:
        """ Validates the given session token against that stored in redis """
        redis_token = redis_handler.hgetall(self.redis_key)
        if not redis_token:
            return False
        token_is_valid = (redis_token[self.SESSION] == token)
        timestamp_is_valid = (datetime.datetime.fromisoformat(redis_token[self.EXPIRATION]) > datetime.datetime.utcnow())
        return token_is_valid and timestamp_is_valid

    def update_session_token(self, *, redis_handler: RedisBase, jwt) -> bool:
        """ Refreshes the session token, jwt (if different) and expiration stored in Redis """
        self.session_token = make_session_token()
        self.jwt = jwt
        self.session_hset = self._build_session_hset(jwt, self.session_token)
        return self.store_session_token(redis_handler=redis_handler)

    def delete_session_token(self, *, redis_handler) -> bool:
        """ Deletes the session token from redis, effectively logging out """
        return redis_handler.delete(self.redis_key)
