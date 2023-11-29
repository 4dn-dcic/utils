import pytest
from contextlib import contextmanager
from dcicutils import transfer_utils
from dcicutils.transfer_utils import TransferUtils
from unittest import mock


# TODO: centralize below utilities elsewhere
def create_dummy_keydict():
    return {'cgap-dummy': {
        'key': 'dummy', 'secret': 'dummy',
        'server': 'cgap-test.com'
    }}


class CGAPKeyManager:
    def get_keydict_for_env(self, keys_file=None):
        return create_dummy_keydict()['cgap-dummy']


@contextmanager
def mock_key_manager():
    with mock.patch.object(transfer_utils, 'CGAPKeyManager', new=CGAPKeyManager):
        yield


class TestTransferUtils:
    """ Tests some basic functionality with mocks for TransferUtils """

    def test_transfer_utils_basic(self):
        """ Tests that we can instantiate an object with defaults """
        with mock_key_manager():
            ts = TransferUtils(ff_env='cgap-dummy')
            assert ts.downloader == 'curl'
            assert ts.download_path == './'
