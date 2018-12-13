# This file is set up with minimal tests for now
# Add more following 4DN Annual Meeting 2018
import os
import datetime
import pytest
from dcicutils import s3_utils
pytestmark = pytest.mark.working


def initialize_jh_env(server):
    keys = s3_utils.s3Utils(env='fourfront-mastertest').get_access_keys()
    os.environ['FF_ACCESS_KEY'] = keys['key']
    os.environ['FF_ACCESS_SECRET'] = keys['secret']
    os.environ['_JH_FF_SERVER'] = server


def test_import_fails_without_initialization():
    # this fails because proper env variables were not set up
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


@pytest.mark.integrated
def test_jh_open_4dn_file(integrated_ff):
    # this is tough because uploaded files don't actually exist on mastertest s3
    # so, this test pretty much assumes urllib will work for actually present
    # files and will just test the exceptions for now
    test_server = integrated_ff['ff_key']['server']
    initialize_jh_env(test_server)
    from dcicutils import jh_utils
    with pytest.raises(Exception) as exec_info:
        with jh_utils.open_4dn_file('not_an_id', local=False) as f:  # NOQA
            pass
    assert 'Could not open file: not_an_id' in str(exec_info.value)
    # use non-file metadata
    search_bios_res = jh_utils.search_metadata('search/?type=Biosample')
    assert len(search_bios_res) > 0
    with pytest.raises(Exception) as exec_info2:
        with jh_utils.open_4dn_file(search_bios_res[0]['uuid'], local=False) as f:  # NOQA
            pass
    assert 'not a valid file object' in str(exec_info2.value)
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
