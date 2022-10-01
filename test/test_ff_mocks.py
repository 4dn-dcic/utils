import pytest

from dcicutils import ff_mocks as ff_mocks_module
from dcicutils.ff_mocks import AbstractIntegratedFixture, AbstractTestRecorder
from dcicutils.misc_utils import ignored, local_attrs
from dcicutils.qa_utils import MockResponse, MockFileSystem, ControlledTime
# from dcicutils.s3_utils import s3Utils
from unittest import mock


def test_abstract_integrated_fixture_no_server_fixtures():

    with mock.patch.object(ff_mocks_module, "NO_SERVER_FIXTURES"):  # too late to set env variable, but this'll do.
        assert AbstractIntegratedFixture._initialize_class() == 'NO_SERVER_FIXTURES'  # noQA - yes, it's protected
        assert AbstractIntegratedFixture.verify_portal_access('not-a-dictionary') == 'NO_SERVER_FIXTURES'


def test_abstract_integrated_fixture_misc():

    with mock.patch.object(AbstractIntegratedFixture, "_initialize_class"):
        fixture = AbstractIntegratedFixture(name='foo')
        fixture.S3_CLIENT = mock.MagicMock()

        sample_portal_access_key = {'key': 'abc', 'secret': 'shazam', 'server': 'http://genes.example.com/'}
        sample_higlass_access_key = {'key': 'xyz', 'secret': 'bingo', 'server': 'http://higlass.genes.example.com/'}

        fixture.S3_CLIENT.get_access_keys.return_value = sample_portal_access_key
        assert fixture.portal_access_key() == sample_portal_access_key

        env_name = 'fourfront-foobar'
        fixture.__class__.ENV_NAME = env_name
        fixture.S3_CLIENT.get_higlass_key.return_value = sample_higlass_access_key
        assert fixture.higlass_access_key() == sample_higlass_access_key

        fixture.INTEGRATED_FF_ITEMS = {'alpha': 'a', 'beta': 'b', 'some_key': '99999'}
        assert fixture['alpha'] == 'a'
        assert fixture['beta'] == 'b'
        with pytest.raises(Exception):
            ignored(fixture['gamma'])
        assert fixture['self'] == fixture

        with mock.patch.object(ff_mocks_module, "id", lambda _: "1234"):
            assert str(fixture) == ("{'self': <AbstractIntegratedFixture 'foo' 1234>,"
                                    " 'alpha': 'a',"
                                    " 'beta': 'b',"
                                    " 'some_key': <redacted>"
                                    "}")

        assert repr(fixture) == "AbstractIntegratedFixture(name='foo')"

        with mock.patch.object(ff_mocks_module, "authorized_request") as mock_authorized_request:
            # Code 418 = "I'm a teapot", which is at least good for testing.
            mock_authorized_request.return_value = MockResponse(status_code=418)
            with pytest.raises(Exception) as exc:
                assert fixture.verify_portal_access({'server': 'http://server.not.available'})
            assert str(exc.value) == (f'Environment {env_name} is not ready for integrated status.'
                                      f' Requesting the homepage gave status of: 418')

        class MyIntegratedFixture(AbstractIntegratedFixture):
            pass

        assert repr(MyIntegratedFixture('bar')) == "MyIntegratedFixture(name='bar')"


def test_abstract_test_recorder_context_managers():

    r = AbstractTestRecorder()

    with pytest.raises(NotImplementedError):
        with r.recorded_requests('foo', None):
            pass

    with pytest.raises(NotImplementedError):
        with r.replayed_requests('foo', None):
            pass


def test_abstract_test_recorder_recording_enabled_and_recording_level():

    r = AbstractTestRecorder()

    assert r.recording_level == 0
    assert not r.recording_enabled
    with r.creating_record():
        assert r.recording_level == 1
        assert not r.recording_enabled
    assert r.recording_level == 0
    assert not r.recording_enabled

    with local_attrs(r, recording_enabled=True):
        assert r.recording_level == 0
        assert r.recording_enabled
        with r.creating_record():
            assert r.recording_level == 1
            assert not r.recording_enabled
        assert r.recording_level == 0
        assert r.recording_enabled

    assert r.recording_level == 0
    assert not r.recording_enabled


def test_abstract_test_recorder_playback():

    r = AbstractTestRecorder('foo')
    r.dt = ControlledTime()

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with mock.patch.object(r, "get_next_json") as mock_get_next_json:
            datum3, datum2, datum1 = data_stack = [
                {'verb': 'GET', 'url': 'http://baz', 'duration': 15, 'status': 400,
                 'error_type': 'Exception', 'error_message': 'ouch'},
                {'verb': 'GET', 'url': 'http://foo', 'duration': 10, 'status': 200, 'result': "omega"},
                {'verb': 'GET', 'url': 'http://bar', 'duration': 20, 'status': 200, 'result': 'alpha'},
            ]
            mock_get_next_json.side_effect = lambda: data_stack.pop()

            response = r.do_mocked_replay(datum1['verb'], datum1['url'])
            assert response.status_code == 200
            assert response.json() == 'alpha'

            response = r.do_mocked_replay(datum2['verb'], datum2['url'])
            assert response.status_code == 200
            assert response.json() == 'omega'

            with pytest.raises(Exception) as exc:
                r.do_mocked_replay(datum3['verb'], datum3['url'])
                raise AssertionError("Should not get here.")
            assert str(exc.value) == 'ouch'

        assert mfs.files == {}
