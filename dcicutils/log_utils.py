import logging
import structlog
import time
import datetime
import uuid
from threading import Timer, Lock
from dcicutils import es_utils
from structlog.threadlocal import wrap_dict
from elasticsearch import eshelpers


class ElasticsearchHandler(logging.Handler):
    """
    Custom handler to post logs to Elasticsearch.
    Needed to sign ES requests with the AWS V4 Signature
    Loosely based off of code here:
    https://github.com/cmanaha/python-elasticsearch-logger
    """
    def __init__(self, es_server):
        """
        Must be given a string es_server url to work.
        Calls __init__ of parent Handler
        """
        print('\n\n\nINIT LOG WITH SERVER: %s\n\n\n' % es_server)
        self.resend_timer = None
        self.records_to_resend = []
        self.es_client = es_utils.create_es_client(es_server, use_aws_auth=True)
        logging.Handler.__init__(self)


    def test_es_server(self):
        """
        Simple ping against the given ES server; will return True if successful
        """
        return self.es_client.ping()


    def schedule_resend(self):
        """
        Create a threading Timer as self.resend_timer to schedule resending any
        records in self.resend_records after 5 seconds.
        If already resending, do nothing
        """
        if self.resend_timer is None:
            self.resend_timer = Timer(5, self.resend_records)
            self.resend_timer.daemon = True
            self.resend_timer.start()


    def resend_records(self):
        """
        Send all records held in self.records_to_resend in batch
        """
        # clean up the timer
        if self.resend_timer is not None and self.resend_timer.is_alive():
            self.resend_timer.cancel()
        self.resend_timer = None
        if self.records_to_resend:
            records_copy = self.records_to_resend[:]
            self.records_to_resend = []
            idx_name = calculate_log_index()
            actions = (
                {
                    '_index': idx_name,
                    '_type': log,
                    '_source': record
                }
                for record in records_copy
            )
            for ok, resp in eshelpers.streaming_bulk(self.es_client, actions):
                print('RESEND RESP: %s' % resp)
                if not ok:
                    self.records_to_resend.append(resp)
        # trigger resending logs if any failed
        if self.records_to_resend:
            self.schedule_resend()


    def emit(self, record):
        """
        Overload the emit method to post logs to ES
        """
        # required?
        # entry = self.format(record)
        import pdb; pdb.set_trace()
        idx_name = calculate_log_index()
        log_id = str(uuid.uuid4())
        try:
            self.es_client.index(index=idx_name, doc_type='log', body=record, id=log_id)
        except Exception as e:
            print('ERROR in logging to ES! %s' % str(e))
            self.records_to_resend.append(record)
            self.schedule_resend()


def calculate_log_index():
    """
    Simple function to name the ES log index by month
    Convention is: filebeat-<yyyy>-<mm>
    * Uses UTC *
    """
    now = datetime.datetime.utcnow()
    idx_suffix = datetime.datetime.strftime(now, '%Y-%m')
    return 'filebeat-' + idx_suffix


def convert_ts_to_at_ts(logger, log_method, event_dict):
    '''
    this function is used to ensure filebeats
    uses our own timestamp if we logged one
    '''
    if 'timestamp' in event_dict:
        event_dict['@timestamp'] = event_dict['timestamp']
        del event_dict['timestamp']
        return event_dict


# configure structlog to use its formats for stdlib logging and / or structlog logging
def set_logging(es_server=None, in_prod=False, level=logging.INFO, log_name=None, log_dir=None):
    '''
    Set logging is a function to be used everywhere, to encourage all subsytems
    to generate structured JSON logs, for easy insertion into ES for searching and
    visualizing later.

    Providing an Elasticsearch server name (es_server) will cause the logs to
    be automatically written to that server.

    Currently this only JSONifies our own logs, and the bit at the very bottom
    would JSONify other logs, like botocore and stuff but that's  probably more
    than we want to store.

    Also sets some standard handlers for the following:
    add_logger_name - python module generating the log
    timestamper - timestamps... big surprise there
    convert_ts_ta_at_ts - takes our timestamp and overwrides
    the @timestamp key used by filebeats, so queries in ES will
    be against our times, which in things like indexing can differ
    a fair amount from the timestamp inserted by filebeats.
    StackInfoRenderer - capture stack trace and insert into JSON
    format_exc_info - capture exception and insert into JSON
    '''
    timestamper = structlog.processors.TimeStamper(fmt="iso")

    logging.basicConfig(format='')

    # structlog is basically wrapping pythons stdlib logging framework and
    # implementing a custom list of processors that take the log message
    # and do fun stuff with it.  Processors are executed in order of this list
    processors = [
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        convert_ts_to_at_ts,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    if in_prod:
        # should be on beanstalk
        level = logging.INFO
        processors.append(structlog.processors.JSONRenderer())
    else:
        # pretty color logs
        processors.append(structlog.dev.ConsoleRenderer())

    # need this guy to go last
    processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

    # context_class guy sets context to thread local so we can set logging context from
    # stats-tween and have that included in each subsequent log entry.
    structlog.configure(
        processors=processors,
        context_class=wrap_dict(dict),
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # add the handler responsible for posting the logs to ES
    import pdb; pdb.set_trace()
    if es_server:
        es_handler = ElasticsearchHandler(es_server)
        logger.addHandler(es_handler)

    # define format and processors for stdlib logging, in case someone hasn't switched
    # yet to using structlog.  Switched off for now as botocore and es are both
    # very chatty, and we should probably turn them to like WARNING level before doing the below
    '''
    pre_chain = [
        # Add the log level and a timestamp to the event_dict if the log entry
        # is not from structlog.
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        timestamper,
    ]
    format_processor = structlog.dev.ConsoleRenderer()
    if in_prod:
        format_processor = structlog.processors.JSONRenderer()

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=format_processor,
        foreign_pre_chain=pre_chain,
    )

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    root_logger.setLevel(level)
    '''

    # below could be used ot redirect logging to a file if desired
    if log_name is None:
        log_name = __name__
    if log_dir and log_name:
        import os
        log_file = os.path.join(log_dir, log_name + ".log")
        logger = logging.getLogger(log_name)
        hdlr = logging.FileHandler(log_file)
        formatter = logging.Formatter('')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(level)
