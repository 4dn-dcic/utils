# This file is set up with minimal tests for now
# Add more following 4DN Annual Meeting 2018
import os
import datetime
import pytest
import re

from dcicutils import s3_utils, ff_utils
from dcicutils.qa_utils import override_environ, MockFileSystem
from unittest import mock

pytestmark = pytest.mark.working


def initialize_jh_env(server):
    keys = s3_utils.s3Utils(env='fourfront-mastertest').get_access_keys()
    os.environ['FF_ACCESS_KEY'] = keys['key']
    os.environ['FF_ACCESS_SECRET'] = keys['secret']
    os.environ['_JH_FF_SERVER'] = server


def test_import_fails_without_initialization():

    # Loading this library fails if proper env variables were not set up.
    # TODO: I'm not sure I think that's a good idea. Functions should fail, imports should not. -kmp 14-Aug-2020

    with override_environ(FF_ACCESS_KEY=None):
        with pytest.raises(Exception) as exec_info:
            from dcicutils import jh_utils  # NOQA
        assert 'ERROR USING JUPYTERHUB_UTILS!' in str(exec_info.value)

    with override_environ(FF_ACCESS_SECRET=None):
        with pytest.raises(Exception) as exec_info:
            from dcicutils import jh_utils  # NOQA
        assert 'ERROR USING JUPYTERHUB_UTILS!' in str(exec_info.value)


@pytest.mark.integrated
def test_proper_initialization(integrated_ff):
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    assert os.environ['FF_ACCESS_KEY'] == integrated_ff['ff_key']['key']
    assert os.environ['FF_ACCESS_SECRET'] == integrated_ff['ff_key']['secret']
    # eliminate 'http://' from server name. This is just how urllib store passwords...
    auth_key = ((test_server[7:], '/'),)
    basic_auth = jh_utils.AUTH_HANDLER.passwd.passwd[None][auth_key]
    assert basic_auth == (os.environ['FF_ACCESS_KEY'], os.environ['FF_ACCESS_SECRET'])


@pytest.mark.integrated
def test_some_decorated_methods_work(integrated_ff):
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    search_res = jh_utils.search_metadata('search/?status=in+review+by+lab')
    assert len(search_res) > 0
    meta_res = jh_utils.get_metadata(search_res[0]['uuid'])
    assert 'uuid' in meta_res
    post_res = jh_utils.post_metadata({'tracking_type': 'other'}, 'tracking-items')
    assert 'uuid' in post_res['@graph'][0]
    patch_res = jh_utils.patch_metadata({'other_tracking': {'test_field': 'test_value'}},
                                        post_res['@graph'][0]['uuid'])
    assert patch_res['@graph'][0]['other_tracking']['test_field'] == 'test_value'
    faceted_search_res = jh_utils.faceted_search(**{'Project': '4DN'})
    assert len(faceted_search_res) == 8


NOT_AN_ID = 'not_an_id'
NOT_AN_ID_ERROR_MESSAGE = "Could not open file: not_an_id"
NOT_VALID_FILE_ERROR_MESSAGE = 'not a valid file object'

BIOSAMPLE_SEARCH_STRING = 'search/?type=Biosample'
BIOSAMPLE_SEARCH_RESULT = [
    {  # This is actually only a subset of the fields we'd find.
        "modifications_summary": "None",
        "@type": ["Biosample", "Item"],
        "display_title": "4DNBS1235556",
        "description": "H1 test prep",
        "biosample_type": "in vitro differentiated cells",
        "accession": "4DNBS1235556",
        "uuid": "111112bc-1111-4448-903e-854af460b123",
        "schema_version": "2",
        "@id": "/biosamples/4DNBS1235556/"
    }
]
BIOSAMPLE_ITEM_0 = BIOSAMPLE_SEARCH_RESULT[0]
BIOSAMPLE_ITEM_0_UUID = BIOSAMPLE_ITEM_0['uuid']

FILE_SEARCH_RESULT = [
    {  # This is actually only a subset of the fields we'd find.
        "display_title": "4DNFIIEB6GJB.bed.gz",
        "title": "4DNFIIEB6GJB",
        "@type": ["FileProcessed", "File", "Item"],
        "uuid": "09d6427d-c253-47d1-8535-f9345b3f7771",
        "@id": "/files-processed/4DNFIIEB6GJB/",
        "status": "uploaded",
        "href": "/files-processed/4DNFIIEB6GJB/@@download/4DNFIIEB6GJB.bed.gz",
        "upload_key": "09d6427d-c253-47d1-8535-f9345b3f7771/4DNFIIEB6GJB.bed.gz",
        "md5sum": "146e3b87f0eac872280c9e2c7c684d43",
        "file_type": "domain calls",
        "file_format": {
            "uuid": "69f6d609-f2ac-4c82-9472-1a13331b5ce9",
            "@id": "/file-formats/bed/",
            "display_title": "bed",
            "file_format": "bed",
            "@type": ["FileFormat", "Item"],
            "principals_allowed": {"view": [], "edit": []}
        },
        "schema_version": "3",
        "extra_files": [
            {
                "filename": "4DNFIIEB6GJB",
                "md5sum": "ad348286f605c227c55b5741c292e967",
                "upload_key": "09d6427d-c253-47d1-8535-f9345b3f7771/4DNFIIEB6GJB.beddb",
                "href": "/files-processed/4DNFIIEB6GJB/@@download/4DNFIIEB6GJB.beddb",
                "filesize": 37888,
                "accession": "4DNFIIEB6GJB",
                "uuid": "09d6427d-c253-47d1-8535-f9345b3f7771",
                "file_format": {
                    "principals_allowed": {},
                    "@type": [],
                    "display_title": "beddb",
                    "@id": "/file-formats/beddb/",
                    "uuid": "76dc8c06-67d8-487b-8fc8-d841752a0b60"
                },
                "status": "uploaded"
            }
        ]
    }
]
FILE_ITEM_0 = FILE_SEARCH_RESULT[0]
FILE_ITEM_0_UUID = FILE_ITEM_0['uuid']
[FILE_ITEM_0_EXTRA_FILE] = FILE_ITEM_0['extra_files']


class MockResponse:
    def __init__(self, status_code, json):
        self.status_code = status_code
        self._json = json

    def json(self):
        return self._json


@pytest.mark.unit
def test_find_valid_file_or_extra_file(integrated_ff):

    # This setup isn't good for a unit test, but right now we can't load jh_utils otherwise. -kmp 15-Feb-2021
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils

    def mocked_get_metadata(obj_id):
        if obj_id == NOT_AN_ID:
            raise Exception(NOT_AN_ID_ERROR_MESSAGE)
        elif obj_id == BIOSAMPLE_ITEM_0_UUID:
            return BIOSAMPLE_ITEM_0
        elif obj_id == FILE_ITEM_0_UUID:
            return FILE_ITEM_0
        else:
            raise NotImplementedError("mocked_authorized_request mock shortfall.")

    def mocked_search_metadata(*args, **kwargs):
        raise NotImplementedError()

    def mocked_authorized_request(url, *args, **kwargs):
        if url.endswith("/" + NOT_AN_ID):
            return MockResponse(404, json={'message': NOT_AN_ID_ERROR_MESSAGE})
        elif url.endswith("/" + BIOSAMPLE_ITEM_0_UUID):
            return MockResponse(200, json=BIOSAMPLE_ITEM_0)
        elif url.endswith("/" + FILE_ITEM_0_UUID):
            return MockResponse(200, json=FILE_ITEM_0)
        else:
            raise NotImplementedError("mocked_authorized_request mock shortfall.")

    mfs = MockFileSystem()
    with mock.patch("builtins.open", mfs.open):
        with mock.patch("os.path.exists", mfs.exists):
            with mock.patch.object(ff_utils, "get_metadata") as mock_get_metadata:
                with mock.patch.object(ff_utils, "authorized_request") as mock_authorized_request:
                    with mock.patch.object(jh_utils, "search_metadata") as mock_search_metadata:

                        mock_get_metadata.side_effect = mocked_get_metadata
                        mock_authorized_request.side_effect = mocked_authorized_request
                        mock_search_metadata.side_effect = mocked_search_metadata

                        with pytest.raises(Exception, match=re.escape(NOT_AN_ID_ERROR_MESSAGE)):
                            jh_utils.find_valid_file_or_extra_file(NOT_AN_ID, None)

                        with pytest.raises(Exception, match=re.escape(NOT_VALID_FILE_ERROR_MESSAGE)):
                            jh_utils.find_valid_file_or_extra_file(BIOSAMPLE_ITEM_0_UUID, None)

                        result = jh_utils.find_valid_file_or_extra_file(FILE_ITEM_0_UUID, None)
                        assert sorted(result.keys()) == ['full_href', 'full_path', 'metadata']
                        assert result['metadata'] == FILE_ITEM_0
                        assert result['full_href'].startswith('http')  # either http or https, depending...
                        assert result['full_href'].endswith(FILE_ITEM_0['href'])
                        assert result['full_path'].startswith('/home')  # the dirname is a wired constant of jh_utils :(
                        assert result['full_href'].endswith(FILE_ITEM_0['href'])

                        extra_format = FILE_ITEM_0_EXTRA_FILE['file_format']['display_title']
                        result = jh_utils.find_valid_file_or_extra_file(FILE_ITEM_0_UUID, format=extra_format)
                        assert sorted(result.keys()) == ['full_href', 'full_path', 'metadata']
                        assert result['metadata'] == FILE_ITEM_0
                        assert sorted(result.keys()) == ['full_href', 'full_path', 'metadata']
                        assert result['metadata'] == FILE_ITEM_0
                        assert result['full_href'].startswith('http')  # either http or https, depending...
                        assert result['full_href'].endswith(FILE_ITEM_0_EXTRA_FILE['href'])
                        assert result['full_path'].startswith('/home')  # the dirname is a wired constant of jh_utils :(
                        assert result['full_href'].endswith(FILE_ITEM_0_EXTRA_FILE['href'])

                        with pytest.raises(Exception, match="invalid file format"):
                            jh_utils.find_valid_file_or_extra_file(FILE_ITEM_0_UUID, 'not_a_format')


@pytest.mark.unit
def test_jh_open_4dn_file_unit(integrated_ff):

    # This setup isn't good for a unit test, but right now we can't load jh_utils otherwise. -kmp 15-Feb-2021
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils

    def mocked_get_metadata(obj_id):
        if obj_id == NOT_AN_ID:
            raise Exception(NOT_AN_ID_ERROR_MESSAGE)
        elif obj_id == BIOSAMPLE_ITEM_0_UUID:
            return BIOSAMPLE_ITEM_0
        elif obj_id == FILE_ITEM_0_UUID:
            return FILE_ITEM_0
        else:
            raise NotImplementedError("mocked_authorized_request mock shortfall.")

    def mocked_search_metadata(*args, **kwargs):
        raise NotImplementedError()

    def mocked_authorized_request(url, *args, **kwargs):
        if url.endswith("/" + NOT_AN_ID):
            return MockResponse(404, json={'message': NOT_AN_ID_ERROR_MESSAGE})
        elif url.endswith("/" + BIOSAMPLE_ITEM_0_UUID):
            return MockResponse(200, json=BIOSAMPLE_ITEM_0)
        elif url.endswith("/" + FILE_ITEM_0_UUID):
            return MockResponse(200, json=FILE_ITEM_0)
        else:
            raise NotImplementedError("mocked_authorized_request mock shortfall.")

    mfs = MockFileSystem()
    with mock.patch("builtins.open", mfs.open):
        with mock.patch("os.path.exists", mfs.exists):
            with mock.patch.object(ff_utils, "get_metadata") as mock_get_metadata:
                with mock.patch.object(ff_utils, "authorized_request") as mock_authorized_request:
                    with mock.patch.object(jh_utils, "search_metadata") as mock_search_metadata:

                        mock_get_metadata.side_effect = mocked_get_metadata
                        mock_authorized_request.side_effect = mocked_authorized_request
                        mock_search_metadata.side_effect = mocked_search_metadata

                        with pytest.raises(Exception, match=re.escape(NOT_AN_ID_ERROR_MESSAGE)):
                            with jh_utils.open_4dn_file(NOT_AN_ID):
                                pass

                        with pytest.raises(Exception, match=re.escape(NOT_VALID_FILE_ERROR_MESSAGE)):
                            with jh_utils.open_4dn_file(BIOSAMPLE_ITEM_0_UUID):
                                pass

                        with pytest.raises(Exception, match="404: Not Found"):
                            with jh_utils.open_4dn_file(FILE_ITEM_0_UUID, local=False):
                                pass

                        with pytest.raises(Exception, match="404: Not Found"):
                            extra_format = FILE_ITEM_0_EXTRA_FILE['file_format']['display_title']
                            with jh_utils.open_4dn_file(FILE_ITEM_0_UUID, format=extra_format, local=False):
                                pass

                        with pytest.raises(Exception, match="invalid file format"):
                            with jh_utils.open_4dn_file(FILE_ITEM_0_UUID, format='not_a_format', local=False):
                                pass

                        with pytest.raises(Exception, match="No such file or directory"):
                            with jh_utils.open_4dn_file(FILE_ITEM_0_UUID, local=True):
                                pass


@pytest.mark.integratedx
def test_jh_open_4dn_file_integrated(integrated_ff):
    # this is tough because uploaded files don't actually exist on mastertest s3
    # so, this test pretty much assumes urllib will work for actually present
    # files and will just test the exceptions for now
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    with pytest.raises(Exception) as exec_info:
        with jh_utils.open_4dn_file(NOT_AN_ID, local=False) as f:  # NOQA
            pass
    assert NOT_AN_ID_ERROR_MESSAGE in str(exec_info.value)
    # use non-file metadata
    search_bios_res = jh_utils.search_metadata('search/?type=Biosample')
    assert len(search_bios_res) > 0
    with pytest.raises(Exception) as exec_info2:
        with jh_utils.open_4dn_file(search_bios_res[0]['uuid'], local=False) as f:  # NOQA
            pass
    assert NOT_VALID_FILE_ERROR_MESSAGE in str(exec_info2.value)
    # get real file metadata
    search_file_res = jh_utils.search_metadata('search/?status=uploaded&type=File&extra_files.href!=No+value')
    assert len(search_file_res) > 0
    file_id = search_file_res[0]['uuid']
    extra_ff = search_file_res[0]['extra_files'][0]['file_format']['display_title']
    with pytest.raises(Exception) as exec_info3:
        with jh_utils.open_4dn_file(file_id, local=False) as f:  # NOQA
            pass
    assert '404: Not Found' in str(exec_info3.value)
    # now use the extra file format
    with pytest.raises(Exception) as exec_info4:
        with jh_utils.open_4dn_file(file_id, format=extra_ff, local=False) as f:  # NOQA
            pass
    assert '404: Not Found' in str(exec_info4.value)
    # now use a bogus extra file format
    with pytest.raises(Exception) as exec_info5:
        with jh_utils.open_4dn_file(file_id, format='not_a_format', local=False) as f:  # NOQA
            pass
    assert 'invalid file format' in str(exec_info5.value)
    # finally, use local=True
    with pytest.raises(Exception) as exec_info6:
        with jh_utils.open_4dn_file(file_id, local=True) as f:  # NOQA
            pass
    # different error, since it attempts to find the file locally
    assert 'No such file or directory' in str(exec_info6.value)


@pytest.mark.skip(reason="we no longer have tracking items")
def test_add_mounted_file_to_session(integrated_ff):
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    # this should fail silently if FF_TRACKING_ID not in environ
    jh_utils.add_mounted_file_to_session('test')
    # now make sure that it works for a real item
    session_body = {'date_initialized': datetime.datetime.utcnow().isoformat() + '+00:00'}
    res = jh_utils.post_metadata({'tracking_type': 'jupyterhub_session',
                                  'jupyterhub_session': session_body}, 'tracking-items')
    res_uuid = res['@graph'][0]['uuid']
    os.environ['FF_TRACKING_ID'] = res_uuid
    jh_utils.add_mounted_file_to_session('test')
    res2 = jh_utils.get_metadata(res_uuid, add_on='datastore=database')
    assert 'test' in res2.get('jupyterhub_session', {}).get('files_mounted', [])


def test_mount_4dn_file(integrated_ff):
    """ Tests getting full filepath of test file on JH
        Needs an additional test (how to?)
    """
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    with pytest.raises(Exception) as exec_info:
        jh_utils.mount_4dn_file('not_an_id')
    assert "Bad status code" in str(exec_info.value)
