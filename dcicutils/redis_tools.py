import secrets
import datetime
import structlog
import jwt
from dcicutils.redis_utils import RedisBase, RedisException


log = structlog.getLogger(__name__)


SESSION_TOKEN_COOKIE = 'c4_st'


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
        <env_namespace>:session:<token>
            -> Redis hset containing the associated JWT and expiration time (3 hours)
    """
    JWT = 'jwt'
    EXPIRATION = 'expiration'

    @staticmethod
    def _build_session_expiration(n_hours: int = 3) -> str:
        """ Builds a session expiration date 3 hours after generation
        :param n_hours: timestamp after which the session token is invalid
        :return: datetime string 3 hours from creation time
        """
        return str(datetime.datetime.utcnow() + datetime.timedelta(hours=n_hours))

    def _build_session_hset(self, jwt: str, expiration=None) -> dict:
        """ Builds Redis hset record for the session token
        :param jwt: encoded jwt value
        :param expiration: expiration if using an existing one
        :return: dictionary to be stored as a hash in Redis
        """
        return {
            self.JWT: jwt,
            self.EXPIRATION: self._build_session_expiration() if not expiration else expiration
        }

    @staticmethod
    def _build_redis_key(namespace: str, token: str) -> str:
        """ Builds the hash key used by Redis
        :param namespace: namespace to build keys under, for example the env name
        :param token: value of the token
        :return: redis hash key to store values under
        """
        return f'{namespace}:session:{token}'

    def __init__(self, *, namespace: str, jwt: str, token=None, expiration=None):
        """ Creates a Redis Session object, storing a hash of the JWT into Redis and returning this
            value as the session token.
            :param namespace: namespace to build key under, for example the env name
            :param jwt: jwt generated for this user
            :param token: value of token if passed, if not one will be generated
            :param expiration: expiration of token if passed, if not new expiration will be generated
        """
        if token:
            self.session_token = token
        else:
            self.session_token = make_session_token()
        self.namespace = namespace
        self.redis_key = self._build_redis_key(self.namespace, self.session_token)
        self.jwt = jwt
        self.session_hset = self._build_session_hset(self.jwt, expiration=expiration)

    def __eq__(self, other):
        """ Evaluates equality of two session objects based on the value of the session hset """
        return self.session_hset == other.session_hset

    def get_session_token(self) -> str:
        """ Extracts the session token stored on this object """
        return self.session_token

    def get_redis_key(self) -> str:
        """ Returns the key under which the Redis hset is stored - note that this contains the token! """
        return self.redis_key

    def get_expiration(self) -> str:
        """ Returns the expiration date stored in the session hset locally """
        return self.session_hset[self.EXPIRATION]

    @classmethod
    def from_redis(cls, *, redis_handler: RedisBase, namespace: str, token: str):
        """ Builds a RedisSessionToken from an existing record - allows extracting JWT
            given a session token internally.
        :param redis_handler: handle to Redis API
        :param namespace: namespace to search under
        :param token: value of the token
        :return: A RedisSessionToken object built from an existing record in Redis
        """
        redis_key = f'{namespace}:session:{token}'
        redis_entry = redis_handler.hgetall(redis_key)
        if redis_entry:
            return cls(namespace=namespace, jwt=redis_entry[cls.JWT],
                       token=token, expiration=redis_entry[cls.EXPIRATION])

    def decode_jwt(self, audience: str, secret: str, leeway: int = 30) -> dict:
        """ Decodes JWT to grab info such as the email
        :param audience: audience under which to decode, typically Auth0Client
        :param secret: secret to decrypt using, typically Auth0Secret
        :param leeway: numerical value in seconds to account for clock drift
        :return: a decoded JWT in dictionary format
        """
        return jwt.decode(self.jwt, secret, audience=audience, leeway=leeway,
                          options={'verify_signature': True}, algorithms=['HS256'])

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

    def validate_session_token(self, *, redis_handler: RedisBase) -> bool:
        """ Validates the given session token against that stored in redis
        :param redis_handler: handle to Redis API
        :return: True if token matches that in Redis and is not expired
        """
        redis_token = redis_handler.hgetall(self.redis_key)
        if not redis_token:
            return False  # if it doesn't exist it's not valid
        timestamp_is_valid = (datetime.datetime.fromisoformat(
            redis_token[self.EXPIRATION]) > datetime.datetime.utcnow()
        )
        return timestamp_is_valid

    def update_session_token(self, *, redis_handler: RedisBase, jwt) -> bool:
        """ Refreshes the session token, jwt (if different) and expiration stored in Redis
        :param redis_handler: handle to Redis API
        :param jwt: jwt of user
        :return: True if successful, raise Exception otherwise
        """
        # remove old token
        self.delete_session_token(redis_handler=redis_handler)
        # build new one
        self.session_token = make_session_token()
        self.redis_key = self._build_redis_key(self.namespace, self.session_token)
        self.jwt = jwt
        self.session_hset = self._build_session_hset(jwt)
        return self.store_session_token(redis_handler=redis_handler)

    def delete_session_token(self, *, redis_handler) -> bool:
        """ Deletes the session token from redis, effectively logging out
        :param redis_handler: handle to Redis API
        :return: True if successful, False otherwise
        """
        return redis_handler.delete(self.redis_key)
