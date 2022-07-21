from typing_extensions import Literal


# ===== Useful constants =====

REGION = 'us-east-1'

APP_CGAP = 'cgap'
APP_FOURFRONT = 'fourfront'

LEGACY_GLOBAL_ENV_BUCKET = 'foursight-test-envs'
LEGACY_CGAP_GLOBAL_ENV_BUCKET = 'foursight-cgap-envs'

DEFAULT_ECOSYSTEM = 'main'

ORCHESTRATED_APPS = [APP_CGAP, APP_FOURFRONT]

CHALICE_STAGE_DEV = 'dev'
CHALICE_STAGE_PROD = 'prod'

CHALICE_STAGES = [CHALICE_STAGE_DEV, CHALICE_STAGE_PROD]

# ===== Type hinting names =====

EnvName = str

# Nicknames for enumerated sets of symbols. Note that these values must be syntactic literals,
# so they can't use the variables defined above.

ChaliceStage = Literal['dev', 'prod']
OrchestratedApp = Literal['cgap', 'fourfront']
