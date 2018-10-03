from dcicutils import log_utils, ff_utils, es_utils
import pytest
import structlog
import time
pytestmark = pytest.mark.working


def test_es_log_idx():
    es_log_idx = log_utils.calculate_log_index()
    assert 'logs-' in es_log_idx


def test_set_logging_not_prod(caplog):
    # setting in_prod to False will invalidate es_server setting
    log_utils.set_logging(es_server='not_a_real_server', in_prod=False)
    log = structlog.getLogger(__name__)
    log.error('bleh', foo='baz')
    assert len(caplog.records) == 1
    log_record = caplog.records[0]
    # make sure there are no handlers and the message is non-dictionary
    assert len(log_record._logger.handlers) == 0
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
    assert len(log_record._logger.handlers) == 0


@pytest.mark.integrated
def test_set_logging_in_prod(caplog, integrated_ff):
    # get es_client info from the health page
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    log_utils.set_logging(es_server=es_url, in_prod=True)
    log = structlog.getLogger(__name__)
    log.warning('meh', foo='bar')
    assert len(caplog.records) == 1
    log_record = caplog.records[0]
    # make sure the ES handler is present
    assert len(log_record._logger.handlers) == 1
    assert 'log_uuid' in caplog.records[0].__dict__['msg']
    assert log_record.__dict__['msg']['event'] == 'meh'
    assert log_record.__dict__['msg']['foo'] == 'bar'
    assert log_record.__dict__['msg']['level'] == 'warning'
    log_uuid = log_record.__dict__['msg']['log_uuid']
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
    assert len(log_record2._logger.handlers) == 1
    assert 'log_uuid' in log_record2.__dict__['msg']
    assert log_record2.__dict__['msg']['event'] == 'test_skip'
    log_uuid = log_record2.__dict__['msg']['log_uuid']
    time.sleep(1)
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_res = ff_utils.get_es_metadata([log_uuid], es_client=es_client,
                                      key=integrated_ff['ff_key'])
    assert len(es_res) == 0  # log is not in ES, as anticipated


@pytest.mark.integrated
def test_logging_retry(caplog, integrated_ff):
    # get es_client info from the health page
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    log_utils.set_logging(es_server=es_url, in_prod=True)
    log = structlog.getLogger(__name__)
    log.warning('test_retry', _test_log_utils=True)
    assert len(caplog.records) == 1
    assert caplog.records[0].__dict__['msg']['event'] == 'test_retry'
    log_uuid = caplog.records[0].__dict__['msg']['log_uuid']
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
