from dcicutils import log_utils, ff_utils, es_utils
import pytest
import structlog
import time
import logging
pytestmark = pytest.mark.working


def test_set_logging_not_prod(caplog):
    # setting in_prod to False will invalidate es_server setting
    log_utils.set_logging(es_server='not_a_real_server', in_prod=False)
    log = structlog.getLogger(__name__)
    log.error('bleh', foo='baz')
    assert len(caplog.records) == 1
    log_record = caplog.records[0]
    # make sure there are no handlers and the message is non-dictionary
    assert len(log_record._logger.handlers) == 0  # noQA - PyCharm doesn't like the reference to ._logger
    assert 'baz' in log_record.__dict__['msg']
    assert 'error' in log_record.__dict__['msg']
    assert 'log_uuid' in log_record.__dict__['msg']
    assert not isinstance(log_record.__dict__['msg'], dict)


def test_set_logging_prod_but_no_es(caplog):
    # omitting es_server while using in_prod=True will cause dictionary
    # log messages but no additional Elasticsearch logging handlers
    log_utils.set_logging(es_server=None, in_prod=True)
    log = structlog.getLogger(__name__)
    log.error('bleh', foo='bean')
    assert len(caplog.records) == 1
    log_record = caplog.records[0]
    assert isinstance(log_record.__dict__['msg'], dict)
    assert 'log_uuid' in log_record.__dict__['msg']
    assert len(log_record._logger.handlers) == 0  # noQA - PyCharm doesn't like the reference to ._logger


@pytest.mark.integrated
def test_set_logging_level(caplog, integrated_ff):
    """ Provides log_dir, log_name and level args to set_logging """
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    log_utils.set_logging(es_server=es_url, level=logging.ERROR, log_name='Errors', log_dir='.')
    log = structlog.getLogger('Errors')
    log.error('oh no an error!', foo='faux')
    assert len(caplog.records) == 1


@pytest.mark.direct_es_query
@pytest.mark.integrated
def test_set_logging_in_prod(caplog, integrated_ff):
    # get es_client info from the health page
    health = ff_utils.get_health_page(key=integrated_ff['ff_key'])
    es_url = health['elasticsearch']
    log_utils.set_logging(env=integrated_ff['ff_env'], es_server=es_url, in_prod=True)
    log = structlog.getLogger(__name__)
    log.warning('meh', foo='bar')

    # There quite a number of records more than one, but they seem unrelated to this test.
    # Banking that nothing else will warn seems poor test strategy, so I've written a search
    # for the record of interest instead. -kmp 10-May-2022
    #
    # assert len(caplog.records) == 1
    # log_record = caplog.records[0]
    # # make sure the ES handler is present
    # assert len(log_record._logger.handlers) == 1
    # assert 'log_uuid' in caplog.records[0].__dict__['msg']
    # assert log_record.__dict__['msg']['event'] == 'meh'
    # assert log_record.__dict__['msg']['foo'] == 'bar'
    # assert log_record.__dict__['msg']['level'] == 'warning'
    # log_uuid = log_record.__dict__['msg']['log_uuid']
    #
    # This is the rewrite:

    assert caplog.records, "There are no log records."
    for log_record in caplog.records:
        msg = log_record.__dict__['msg']  # gets around a method for .msg that does something we don't want.
        if not isinstance(msg, str) and msg['event'] == 'meh':
            assert msg['foo'] == 'bar'
            assert msg['level'] == 'warning'
            log_uuid = msg['log_uuid']
            break
    else:
        raise AssertionError("Expected test record event='meh' not found.")

    # make sure the log was written successfully to mastertest ES
    time.sleep(1)
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_res = ff_utils.get_es_metadata([log_uuid], es_client=es_client,
                                      key=integrated_ff['ff_key'])
    assert len(es_res) == 1
    assert es_res[0]['event'] == 'meh'
    assert es_res[0]['foo'] == 'bar'
    assert es_res[0]['log_uuid'] == log_uuid
    assert es_res[0]['level'] == 'warning'

    # setting _skip_es = True will cause the log not to be shipped to ES
    log.warning('test_skip', _skip_es=True)
    assert len(caplog.records) == 2  # two logs now
    log_record2 = caplog.records[1]
    # make sure the ES handler is present
    assert len(log_record2._logger.handlers) == 1  # noQA - PyCharm doesn't like the reference to ._logger
    assert 'log_uuid' in log_record2.__dict__['msg']
    assert log_record2.__dict__['msg']['event'] == 'test_skip'
    log_uuid = log_record2.__dict__['msg']['log_uuid']
    time.sleep(1)
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_res = ff_utils.get_es_metadata([log_uuid], es_client=es_client,
                                      key=integrated_ff['ff_key'])
    assert len(es_res) == 0  # log is not in ES, as anticipated


@pytest.mark.direct_es_query
@pytest.mark.integrated
def test_logging_retry(caplog, integrated_ff):
    # get es_client info from the health page
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    log_utils.set_logging(env=integrated_ff['ff_env'], es_server=es_url, in_prod=True)
    log = structlog.getLogger(__name__)
    log.warning('test_retry', _test_log_utils=True)

    # There quite a number of records more than one, but they seem unrelated to this test.
    # Banking that nothing else will warn seems poor test strategy, so I've written a search
    # for the record of interest instead. -kmp 10-May-2022
    #
    # assert len(caplog.records) == 1
    # assert caplog.records[0].__dict__['msg']['event'] == 'test_retry'
    # log_uuid = caplog.records[0].__dict__['msg']['log_uuid']
    #
    # This is the rewrite:

    assert caplog.records, "There are no log records."
    for log_record in caplog.records:
        msg = log_record.__dict__['msg']  # gets around a method for .msg that does something we don't want.
        if not isinstance(msg, str) and msg['event'] == 'test_retry':
            assert msg['_test_log_utils'] is True
            log_uuid = msg['log_uuid']
            break
    else:
        raise AssertionError("Expected test record event='test_retry' not found.")

    # retrying will take 5 sec, so log shoudldn't be in ES yet
    time.sleep(1)
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_res = ff_utils.get_es_metadata([log_uuid], es_client=es_client,
                                      key=integrated_ff['ff_key'])
    assert len(es_res) == 0
    # wait to allow logs to retry
    time.sleep(7)
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_res = ff_utils.get_es_metadata([log_uuid], es_client=es_client,
                                      key=integrated_ff['ff_key'])
    assert len(es_res) == 1
    assert es_res[0]['log_uuid'] == log_uuid
    assert es_res[0]['event'] == 'test_retry'
