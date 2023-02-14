import secrets
import datetime
import structlog
from dcicutils.redis_utils import RedisBase, RedisException


log = structlog.getLogger(__name__)


def make_session_token(n_bytes: int = 32) -> str:
    """ Uses the secrets module to create a cryptographically secure and URL safe string
    :param n_bytes: number of bytes to use, default 32
    :return: cryptographically secure url safe token for use as a session token
    """
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
    def _build_session_expiration() -> str:
        """ Builds a session expiration date 3 hours after generation
        :return: datetime string 3 hours from creation time
        """
        return str(datetime.datetime.utcnow() + datetime.timedelta(hours=3))

    def _build_session_hset(self, jwt: str, token: str, expiration=None) -> dict:
        """ Builds Redis hset record for the session token
        :param jwt: encoded jwt value
        :param token: session token
        :param expiration: expiration if using an existing one
        :return: dictionary to be stored as a hash in Redis
        """
        return {
            self.JWT: jwt,
            self.SESSION: token,
            self.EXPIRATION: self._build_session_expiration() if not expiration else expiration
        }

    @staticmethod
    def _build_redis_key(namespace: str, email: str) -> str:
        """ Builds the hash key used by Redis
        :param namespace: namespace to build keys under, for example the env name
        :param email: email this session token is associated with
        :return: redis hash key to store values under
        """
        return f'{namespace}:session:{email}'

    def __init__(self, *, namespace: str, email: str, jwt: str, token=None, expiration=None):
        """ Creates a Redis Session object, storing a hash of the JWT into Redis and returning this
            value as the session token.
            :param namespace: namespace to build key under, for example the env name
            :param email: email this session token is associated with
            :param jwt: jwt generated for this user
            :param token: value of token if passed, if not one will be generated
            :param expiration: expiration of token if passed, if not new expiration will be generated
        """
        self.redis_key = self._build_redis_key(namespace, email)
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
        """ Builds a RedisSessionToken from an existing record
        :param redis_handler: handle to Redis API
        :param namespace: namespace to search under
        :param email: email the token is associated with
        :return: A RedisSessionToken object built from an existing record in Redis
        """
        redis_key = f'{namespace}:session:{email}'
        redis_token = redis_handler.hgetall(redis_key)
        return cls(namespace=namespace, email=email, jwt=redis_token[cls.JWT],
                   token=redis_token[cls.SESSION],
                   expiration=redis_token[cls.EXPIRATION])

    def store_session_token(self, *, redis_handler: RedisBase) -> bool:
        """ Stores the created session token object as an hset in Redis
        :param redis_handler: handle to Redis API
        :return: True if successful, raise Exception otherwise
        """
        try:
            redis_handler.hset_multiple(self.redis_key, self.session_hset)
        except Exception as e:
            log.error(str(e))
            raise RedisException()
        return True

    def validate_session_token(self, *, redis_handler: RedisBase, token) -> bool:
        """ Validates the given session token against that stored in redis
        :param redis_handler: handle to Redis API
        :param token: token to validate
        :return: True if token matches that in Redis and is not expired
        """
        redis_token = redis_handler.hgetall(self.redis_key)
        if not redis_token:
            return False
        token_is_valid = (redis_token[self.SESSION] == token)
        timestamp_is_valid = (datetime.datetime.fromisoformat(
            redis_token[self.EXPIRATION]) > datetime.datetime.utcnow()
        )
        return token_is_valid and timestamp_is_valid

    def update_session_token(self, *, redis_handler: RedisBase, jwt) -> bool:
        """ Refreshes the session token, jwt (if different) and expiration stored in Redis
        :param redis_handler: handle to Redis API
        :param jwt: jwt of user
        :return: True if successful, raise Exception otherwise
        """
        self.session_token = make_session_token()
        self.jwt = jwt
        self.session_hset = self._build_session_hset(jwt, self.session_token)
        return self.store_session_token(redis_handler=redis_handler)

    def delete_session_token(self, *, redis_handler) -> bool:
        """ Deletes the session token from redis, effectively logging out
        :param redis_handler: handle to Redis API
        :return: True if successful, False otherwise
        """
        return redis_handler.delete(self.redis_key)
