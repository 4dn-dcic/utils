import csv
import os
import re

from tempfile import TemporaryFile
from typing import Dict, Union, Tuple, List, Any
from typing_extensions import Literal


# ===== Useful constants =====

REGION = 'us-east-1'

APP_CGAP = 'cgap'
APP_FOURFRONT = 'fourfront'
APP_SMAHT = 'smaht'

LEGACY_GLOBAL_ENV_BUCKET = 'foursight-test-envs'
LEGACY_CGAP_GLOBAL_ENV_BUCKET = 'foursight-cgap-envs'

DEFAULT_ECOSYSTEM = 'main'

ORCHESTRATED_APPS = [APP_CGAP, APP_FOURFRONT, APP_SMAHT]

CHALICE_STAGE_DEV = 'dev'
CHALICE_STAGE_PROD = 'prod'

CHALICE_STAGES = [CHALICE_STAGE_DEV, CHALICE_STAGE_PROD]

# ===== Type hinting names =====

EnvName = str

# Nicknames for enumerated sets of symbols. Note that these values must be syntactic literals,
# so they can't use the variables defined above.

ChaliceStage = Literal['dev', 'prod']
OrchestratedApp = Literal['cgap', 'fourfront', 'smaht']

LIBRARY_DIR = os.path.dirname(__file__)

# ===== Auth Data =====

AuthStr = str

SimpleAuthDict = Dict[Literal['key', 'secret'], str]
ServerAuthDict = Dict[Literal['key', 'secret', 'server'], str]
AuthDict = Union[SimpleAuthDict, ServerAuthDict]

LegacyAuthDict = Dict[Literal['default'], AuthDict]
AnyAuthDict = Union[LegacyAuthDict, AuthDict]

SimpleAuthPair = Tuple[str, str]  # key, secret

AuthData = Union[AuthDict, SimpleAuthPair]
AnyAuthData = Union[LegacyAuthDict, AuthData]

# ===== JSON Data =====

JsonSchema = Dict

AnyJsonData = Union[Dict[str, 'AnyJsonData'], List['AnyJsonData'], str, bool, int, float, None]

KeyValueDict = Dict[Literal['Key', 'Value'], Any]
KeyValueDictList = List[KeyValueDict]

KeyValuestringDict = Dict[Literal['Key', 'Value'], str]
KeyValuestringDictList = List[KeyValuestringDict]

# ===== Miscellaneous Data =====

UrlString = str

PortalEnvName = str

Regexp = type(re.compile("sample"))

CsvReader = type(csv.reader(TemporaryFile()))

# ===== AWS Data =====

S3KeyName = str
S3BucketName = str

# Refs:
#  * https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
#  * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html
#
# We single out 'available' storage classes as the ones someone would expect to be readily
# available in short time and not subject to catastrophe, so that an I/O error probably
# isn't related to the storage class. In practice that's the two standard storage classes
# plus the intelligent tiering.  Most of the others have a latency issue or are otherwise
# fragile. In practice, we just want to not overly warn about normal kinds of storage.

ALL_S3_STORAGE_CLASSES = [
    'STANDARD', 'REDUCED_REDUNDANCY', 'STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING',
    'GLACIER', 'DEEP_ARCHIVE', 'OUTPOSTS', 'GLACIER_IR',
]

AVAILABLE_S3_STORAGE_CLASSES = [
    'STANDARD', 'STANDARD_IA', 'INTELLIGENT_TIERING'
]
# Refs:
#  * https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
#
# See boto3 docs for info on possible values, but these 3 are the current ones used for
# glacier (that require restore calls) - Will 7 Apr 2023
S3_GLACIER_CLASSES = [
    'GLACIER_IR',  # Glacier Instant Retrieval
    'GLACIER',  # Glacier Flexible Retrieval
    'DEEP_ARCHIVE'  # Glacier Deep Archive
]
S3GlacierClass = Union[
    Literal['GLACIER_IR'],
    Literal['GLACIER'],
    Literal['DEEP_ARCHIVE'],
]
S3StorageClass = Union[
    Literal['STANDARD'],
    Literal['REDUCED_REDUNDANCY'],
    Literal['STANDARD_IA'],
    Literal['ONEZONE_IA'],
    Literal['INTELLIGENT_TIERING'],
    Literal['GLACIER'],
    Literal['DEEP_ARCHIVE'],
    Literal['OUTPOSTS'],
    Literal['GLACIER_IR'],
]


# This constant is used in our Lifecycle management system to automatically transition objects
ENCODED_LIFECYCLE_TAG_KEY = 'Lifecycle'


# These numbers come from AWS and is the max size that can be copied with a single request
# Any larger than this requires a multipart upload - Will 24 April 2023
MAX_STANDARD_COPY_SIZE = 5368709120
MAX_MULTIPART_CHUNKS = 10000
