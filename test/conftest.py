import os
import pytest
import requests

from dcicutils.common import LEGACY_GLOBAL_ENV_BUCKET
from dcicutils.env_utils import LegacyController, EnvUtils
from dcicutils.ff_mocks import IntegratedFixture
from .conftest_settings import TEST_DIR, INTEGRATED_ENV


IntegratedFixture.initialize_class()
IntegratedFixture.verify_portal_access(IntegratedFixture.PORTAL_ACCESS_KEY)


def _portal_health_get(namespace, portal_url, key):
    healh_json_url = f"{portal_url}/health?format=json"
    response = requests.get(healh_json_url)
    health_json = response.json()
    assert health_json['namespace'] == namespace  # check we're talking to proper host
    return health_json[key]


# When we've rewritten these tests:
# we can
# (a) disable tests in test_env_utils_legacy.py and test_env_utils_orchestrated_legacy_compatibility.py
#     perhaps by just renaming those to .py.DISABLED
# (b) fix these tests:
#         test/test_ff_utils.py::test_unified_authentication_unit FAILED
#         test/test_ff_utils.py::test_get_authentication_with_server_unit FAILED
#         test/test_ff_utils.py::test_are_counts_even_unit[True-sample_counts0] FAILED
#         test/test_ff_utils.py::test_are_counts_even_unit[False-sample_counts1] FAILED
#         test/test_s3_utils.py::test_read_s3_unit FAILED
#         test/test_s3_utils.py::test_get_file_size_unit FAILED
#         test/test_s3_utils.py::test_size_unit FAILED
#         test/test_s3_utils.py::test_get_file_size_in_gb_unit FAILED
#         test/test_s3_utils.py::test_read_s3_zip_unit FAILED
#         test/test_s3_utils.py::test_unzip_s3_to_s3_unit[-fastqc_report.html] FAILED
#         test/test_s3_utils.py::test_unzip_s3_to_s3_unit[2-qc_report.html] FAILED
#         test/test_s3_utils.py::test_unzip_s3_to_s3_store_results_unit FAILED
#         test/test_s3_utils.py::test_s3_utils_buckets_modern FAILED
#         test/test_s3_utils.py::test_env_manager FAILED
#         test/test_s3_utils.py::test_env_manager_verify_and_get_env_config FAILED
#         test/test_s3_utils.py::test_env_manager_compose FAILED
# (c) comment out the next line:
LegacyController.LEGACY_DISPATCH_ENABLED = True

os.environ['GLOBAL_ENV_BUCKET'] = LEGACY_GLOBAL_ENV_BUCKET
os.environ['ENV_NAME'] = INTEGRATED_ENV

EnvUtils.init()  # This would be a good time to get EnvUtils to synchronize with an environment if it hasn't


@pytest.fixture(scope='session')
def integrated_ff():
    """
    Object that contains keys and ff_env for integrated environment
    """
    return IntegratedFixture('integrated_ff')


@pytest.fixture(scope='session')
def integrated_names():

    test_filename = '__test_data/test_file.txt'

    zip_filename = '__test_data/fastqc_report.zip'
    zip_filename2 = '__test_data/madqc_report.zip'

    zip_path = os.path.join(TEST_DIR, 'data_files', os.path.basename(zip_filename))
    zip_path2 = os.path.join(TEST_DIR, 'data_files', os.path.basename(zip_filename2))

    return {
        'ffenv': INTEGRATED_ENV,
        'filename': test_filename,
        # short filenames or s3 key names (informally, s3 filenames)
        'zip_filename': zip_filename,
        'zip_filename2': zip_filename2,
        # actual local filenames where the data should be
        'zip_path': zip_path,
        'zip_path2': zip_path2,
    }


@pytest.fixture(scope='session')
def integrated_s3_info(integrated_names):
    """
    Ensure the test files are present in the s3 sys bucket of the integrated
    environment (probably 'fourfront-mastertest') and return some info on them
    """

    test_filename = integrated_names['filename']

    s3_obj = IntegratedFixture.S3_CLIENT
    # for now, always upload these files
    s3_obj.s3.put_object(Bucket=s3_obj.outfile_bucket, Key=test_filename,
                         Body=str.encode('thisisatest'))
    s3_obj.s3.upload_file(Filename=integrated_names['zip_path'],
                          Bucket=s3_obj.outfile_bucket,
                          Key=integrated_names['zip_filename'])
    s3_obj.s3.upload_file(Filename=integrated_names['zip_path2'],
                          Bucket=s3_obj.outfile_bucket,
                          Key=integrated_names['zip_filename2'])

    return {
        'ffenv': INTEGRATED_ENV,
        's3Obj': s3_obj,
        'bucket': s3_obj.outfile_bucket,
        'filename': test_filename,
        'zip_filename': integrated_names['zip_filename'],
        'zip_filename2': integrated_names['zip_filename2'],
    }
