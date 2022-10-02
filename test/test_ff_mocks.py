import io
import json
import pytest

from dcicutils import ff_mocks as ff_mocks_module, ff_utils as ff_utils_module
from dcicutils.ff_mocks import AbstractIntegratedFixture, AbstractTestRecorder
from dcicutils.misc_utils import ignored, local_attrs, PRINT
from dcicutils.qa_utils import MockResponse, MockFileSystem, ControlledTime, printed_output
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


@pytest.mark.parametrize("recording_enabled", [False, True])
def test_abstract_test_recorder_recording(recording_enabled):

    recordings_dir = 'my_recordings'
    test_name = 'foo'
    r = AbstractTestRecorder(recordings_dir=recordings_dir)
    output_stream = io.StringIO()
    r.recording_fp = output_stream
    r.recording_enabled = recording_enabled
    r.recording_level = 0

    dt = ControlledTime()

    initial_data = {'initial': 'data'}

    mfs = MockFileSystem()

    with printed_output() as printed:
        with mfs.mock_exists_open_remove():
            with r.setup_recording(test_name, initial_data):
                with mock.patch.object(ff_mocks_module, "datetime", dt):

                    datum4, datum3, datum2, datum1 = data_server_stack = [
                        {'verb': 'GET', 'url': 'http://any', 'data': None, 'duration': 17.0,
                         'error_type': RuntimeError, 'error_message': 'yikes'},
                        {'verb': 'GET', 'url': 'http://baz', 'data': None, 'duration': 15.0, 'status': 400,
                         'result': 'sorry'},
                        {'verb': 'GET', 'url': 'http://bar', 'data': None, 'duration': 20.0, 'status': 200,
                         'result': 'omega'},
                        {'verb': 'GET', 'url': 'http://foo', 'data': None, 'duration': 10.0, 'status': 200,
                         'result': "alpha"},
                    ]

                    def simulate_actual_server():
                        start_time = dt.just_now() - dt._tick_timedelta
                        info = data_server_stack.pop()
                        duration = info.get('duration')
                        if duration:
                            # We subtract 1 from the duration because 'just_now()' occurs
                            # after the first measuring of time that will already have been done
                            dt.set_datetime(start_time + dt._tick_timedelta * duration)
                        if info.get('error_message'):
                            error_type = info.get('error_type')
                            raise error_type(info.get('error_message'))
                        return MockResponse(status_code=info['status'], json=info['result'])

                    response = r.do_mocked_record(action=simulate_actual_server, verb=datum1['verb'], url=datum1['url'])
                    assert response.status_code == 200
                    assert response.json() == datum1['result']  # 'alpha'

                    response = r.do_mocked_record(action=simulate_actual_server, verb=datum2['verb'], url=datum2['url'])
                    assert response.status_code == 200
                    assert response.json() == datum2['result']  # 'omega'

                    response = r.do_mocked_record(action=simulate_actual_server, verb=datum3['verb'], url=datum3['url'])
                    assert response.status_code == 400
                    assert response.json() == datum3['result']  # 'sorry'

                    with pytest.raises(RuntimeError) as exc:
                        r.do_mocked_record(action=simulate_actual_server, verb=datum4['verb'], url=datum4['url'])
                        raise AssertionError("Should not get here.")
                    assert str(exc.value) == datum4['error_message']  # 'yikes'

            if recording_enabled:

                expected = {
                    f"{recordings_dir}/{test_name}":
                        f'{json.dumps(initial_data)}\n'
                        f'{json.dumps(datum1)}\n'
                        f'{json.dumps(datum2)}\n'
                        f'{json.dumps(datum3)}\n'
                        f'{json.dumps(datum4, default=lambda x: x.__name__)}\n'.encode('utf-8')
                }
            else:
                expected = {f"{recordings_dir}/{test_name}": f"{json.dumps(initial_data)}\n".encode('utf-8')}

            assert mfs.files == expected

        recording = "Recording" if recording_enabled else "NOT recording"
        assert printed.lines == [
            f'{recording} GET http://foo normal result',
            f'{recording} GET http://bar normal result',
            f'{recording} GET http://baz normal result',
            f'{recording} GET http://any error result'
        ]


def test_abstract_test_recorder_playback():

    r = AbstractTestRecorder('foo')
    r.dt = ControlledTime()

    mfs = MockFileSystem()

    with printed_output() as printed:
        with mfs.mock_exists_open_remove():

            with mock.patch.object(r, "get_next_json") as mock_get_next_json:
                datum4, datum3, datum2, datum1 = data_stack = [
                    {'verb': 'GET', 'url': 'http://any', 'data': None, 'duration': 17.0,
                     'error_type': RuntimeError, 'error_message': 'yikes'},
                    {'verb': 'GET', 'url': 'http://baz', 'data': None, 'duration': 15.0, 'status': 400,
                     'result': 'sorry'},
                    {'verb': 'GET', 'url': 'http://bar', 'data': None, 'duration': 20.0, 'status': 200,
                     'result': 'omega'},
                    {'verb': 'GET', 'url': 'http://foo', 'data': None, 'duration': 10.0, 'status': 200,
                     'result': "alpha"},
                ]
                mock_get_next_json.side_effect = lambda: data_stack.pop()

                response = r.do_mocked_replay(datum1['verb'], datum1['url'])
                assert response.status_code == 200
                assert response.json() == datum1['result']  # 'alpha'

                response = r.do_mocked_replay(datum2['verb'], datum2['url'])
                assert response.status_code == 200
                assert response.json() == datum2['result']  # 'omega'

                response = r.do_mocked_replay(datum3['verb'], datum3['url'])
                assert response.status_code == 400
                assert response.json() == datum3['result']  # 'sorry'

                with pytest.raises(Exception) as exc:
                    r.do_mocked_replay(datum4['verb'], datum4['url'])
                    raise AssertionError("Should not get here.")
                assert str(exc.value) == datum4['error_message']  # 'yikes'

            assert mfs.files == {}  # no files created on playback

        assert printed.lines == [
            f"Replaying GET {datum1['url']}",  # http://foo
            f" from recording of normal result for GET {datum1['url']}",
            f"Replaying GET {datum2['url']}",  # http://bar
            f" from recording of normal result for GET {datum2['url']}",
            f"Replaying GET {datum3['url']}",  # http://baz
            f" from recording of normal result for GET {datum3['url']}",
            f"Replaying GET {datum4['url']}",  # http://any
            f" from recording of error result for GET {datum4['url']}",
        ]


@pytest.mark.parametrize("recording_enabled", [False, True])
@pytest.mark.parametrize("sample_result", [False, True])
@pytest.mark.parametrize("check_secondary", [False, True])
def test_mocked_recording_stuff_in_queues(recording_enabled, sample_result, check_secondary):

    dt = ControlledTime()
    r = AbstractTestRecorder('foo')
    r.recording_enabled = recording_enabled
    r.dt = dt
    output_stream = io.StringIO()
    r.recording_fp = output_stream
    namespace = "some_namespace"
    with mock.patch.object(ff_mocks_module, "datetime", dt):
        with mock.patch.object(ff_utils_module, "internal_compute_stuff_in_queues") as mock_compute_stuff_in_queues:
            mock_compute_stuff_in_queues.return_value = sample_result
            r.mocked_recording_stuff_in_queues(ff_env_index_namespace='some_namespace', check_secondary=check_secondary)
            if recording_enabled:
                expected_result = {
                    'verb': 'stuff-in-queues',
                    'url': None,
                    'data': {'ff_env_index_namespace': namespace, "check_secondary": check_secondary},
                    'duration': 1.0,
                    'result': sample_result
                }
                assert output_stream.getvalue() == json.dumps(expected_result) + '\n'
            else:
                assert output_stream.getvalue() == ""


def test_get_next_json():

    item1, item2 = [{"item": 1}, {"item": 2}]

    stream = io.StringIO()
    PRINT(json.dumps(item1), file=stream)
    PRINT(json.dumps(item2), file=stream)
    stream.seek(0)

    r = AbstractTestRecorder('foo')
    r.recording_fp = stream

    assert r.get_next_json() == item1
    assert r.get_next_json() == item2
    with pytest.raises(AssertionError) as exc:
        r.get_next_json()
    assert str(exc.value) == "Out of replayable records."