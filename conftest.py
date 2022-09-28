import os

from dcicutils.env_utils import EnvUtils
from dcicutils.misc_utils import environ_bool, PRINT


USE_SAMPLE_ENVUTILS = environ_bool("USE_SAMPLE_ENVUTILS")


_account_number = os.environ.get('ACCOUNT_NUMBER')

if _account_number and _account_number != '643366669028':
    raise Exception(f"These tests must be run with legacy credentials."
                    f"Your credentials are set to account {_account_number}")

if USE_SAMPLE_ENVUTILS:
    PRINT(f"EnvUtils using sample configuration template.")
    EnvUtils.set_declared_data(data=EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
