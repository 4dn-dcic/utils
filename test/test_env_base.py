import os

from dcicutils.env_base import EnvBase


# TODO: Some tests of EnvManager (test_env_manager_xxx) need to move here from test_s3_utils.py


def test_set_global_env_bucket():

    sample_bucket_name1 = "my-global-env-bucket-one"
    sample_bucket_name2 = "my-global-env-bucket-two"

    with EnvBase.global_env_bucket_named(sample_bucket_name1):

        assert os.environ['GLOBAL_BUCKET_ENV'] == sample_bucket_name1
        assert os.environ['GLOBAL_ENV_BUCKET'] == sample_bucket_name1

        EnvBase.set_global_env_bucket(sample_bucket_name2)

        assert os.environ['GLOBAL_BUCKET_ENV'] == sample_bucket_name2
        assert os.environ['GLOBAL_ENV_BUCKET'] == sample_bucket_name2
