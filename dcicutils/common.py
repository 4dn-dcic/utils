from typing_extensions import Literal


REGION = 'us-east-1'

APP_CGAP = 'cgap'
APP_FOURFRONT = 'fourfront'

ORCHESTRATED_APPS = [APP_CGAP, APP_FOURFRONT]

# Type hinting names
EnvName = str
OrchestratedApp = Literal['cgap', 'fourfront']   # Note: these values must be syntactic literals, can't use vars above
