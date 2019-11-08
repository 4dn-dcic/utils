import logging
import structlog
import datetime
import uuid
from threading import Timer
from dcicutils import es_utils
from structlog.threadlocal import wrap_dict
from elasticsearch import helpers


class ElasticsearchHandler(logging.Handler):
    """
    Custom handler to post logs to Elasticsearch.
    Needed to sign ES requests with the AWS V4 Signature
    Loosely based off of code here:
    https://github.com/cmanaha/python-elasticsearch-logger
    """
    def __init__(self, env, es_server):
        """
        Must be given a string es_server url to work.
        Calls __init__ of parent Handler
        """
        self.resend_timer = None
        self.messages_to_resend = []
        self.retry_limit = 2
        self.namespace = self.get_namespace(env)
        self.es_client = es_utils.create_es_client(es_server, use_aws_auth=True)
        logging.Handler.__init__(self)

    @staticmethod
    def get_namespace(env):
        """ Grabs ES namespace from health page """
        from .ff_utils import get_health_page
        health = get_health_page(ff_env=env)
        return health.get('namespace', '')

    def schedule_resend(self):
        """
        Create a threading Timer as self.resend_timer to schedule resending any
        records in self.resend_messages after 5 seconds.
        If already resending, do nothing
        """
        if self.resend_timer is None:
            self.resend_timer = Timer(5, self.resend_messages)
            self.resend_timer.daemon = True
            self.resend_timer.start()

    def resend_messages(self):
        """
        Send all records held in self.messages_to_resend in Elasticsearch in bulk.
        Keep track of subsequent errors and retry them, if they have been
        retried fewer times than self.retry_limit
        """
        # clean up the timer
        if self.resend_timer is not None and self.resend_timer.is_alive():
            self.resend_timer.cancel()
        self.resend_timer = None
        if self.messages_to_resend:
            messages_copy = self.messages_to_resend[:]
            self.messages_to_resend = []
            idx_name = self.calculate_log_index()
            actions = (
                {
                    '_index': idx_name,
                    '_type': 'log',
                    '_id': message[0],
                    '_source': message[1]
                }
                for message in messages_copy
            )
            errors = []
            for ok, resp in helpers.streaming_bulk(self.es_client, actions):
                if not ok:
                    errors.append(resp['index']['_id'])
            for sent_message in messages_copy:
                if sent_message[0] in errors and sent_message[2] < self.retry_limit:
                    sent_message[2] += 1  # increment retries
                    self.messages_to_resend.append(sent_message)
        # trigger resending logs if any failed
        if self.messages_to_resend:
            self.schedule_resend()

    def emit(self, record):
        """
        Overload the emit method to post logs to ES
        """
        # required?
        # entry = self.format(record)
        idx_name = self.calculate_log_index()
        # get the message from the record
        message = record.__dict__.get('msg')
        # adds the ability to manually skip logging the message to ES
        if not message or message.get('_skip_es', False) is True:
            return
        # make a uuid; use the inherent log_uuid if provided
        log_id = message.get('log_uuid', str(uuid.uuid4()))
        # for testing purposes. trigger the retry mechanism
        if message.get('_test_log_utils', False) is True:
            self.messages_to_resend.append([log_id, message, 0])
            self.schedule_resend()
            return
        try:
            self.es_client.index(index=idx_name, doc_type='log', body=message, id=log_id)
        except Exception:
            # append resend messages as a list: [<uuid>, <dict message>, <int retries>]
            self.messages_to_resend.append([log_id, message, 0])
            self.schedule_resend()

    def calculate_log_index(self):
        """
        Simple function to name the ES log index by month
        Convention is: logs-<yyyy>-<mm>
        * Uses UTC *
        """
        now = datetime.datetime.utcnow()
        idx_suffix = datetime.datetime.strftime(now, '%Y-%m')
        return self.namespace + 'logs-' + idx_suffix


class ElasticsearchLoggerFactory(structlog.stdlib.LoggerFactory):
    """
    Needed to bind the ElasticsearchHandler to the structlog logger.
    Use for logger_factory arg in structlog.configure function
    See: https://github.com/hynek/structlog/blob/master/src/structlog/stdlib.py
    """
    def __init__(self, env=None, ignore_frame_names=None, es_server=None, in_prod=False):
        """
        Set self.es_server and call __init__ of parent.
        If not in prod, always set the es_server to None (dev mode)
        """
        self.env = None if not env else env
        self.es_server = es_server if in_prod else None
        structlog.stdlib.LoggerFactory.__init__(self, ignore_frame_names)

    def __call__(self, *args):
        """
        Overload the original __call__ function and add the custom handler
        The args used to structlog.getLogger should be <logger name> and
        <es server>, in that order
        """
        if args:
            name = args[0]
        else:
            _, name = structlog._frames._find_first_app_frame_and_name(self._ignore)
        logger = logging.getLogger(name)
        if self.es_server:
            es_handler = ElasticsearchHandler(self.env, self.es_server)
            logger.addHandler(es_handler)
            # also set level to info
            logger.setLevel(logging.INFO)
        return logger


def convert_ts_to_at_ts(logger, log_method, event_dict):
    '''
    this function is used to ensure filebeats
    uses our own timestamp if we logged one
    '''
    if 'timestamp' in event_dict:
        event_dict['@timestamp'] = event_dict['timestamp']
        del event_dict['timestamp']
        return event_dict


def add_log_uuid(logger, log_method, event_dict):
    '''
    this function adds a uuid to the log
    '''
    event_dict['log_uuid'] = str(uuid.uuid4())
    return event_dict


# configure structlog to use its formats for stdlib logging and / or structlog logging
def set_logging(env=None, es_server=None, in_prod=False, level=logging.WARN, log_name=None, log_dir=None):
    '''
    Set logging is a function to be used everywhere, to encourage all subsytems
    to generate structured JSON logs, for easy insertion into ES for searching and
    visualizing later.

    Providing an Elasticsearch server name (es_server) will cause the logs to
    be automatically written to that server. Setting 'skip_es' to True for any
    individual logging statement will cause it not to be written.

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
        add_log_uuid,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # if we are in_prod, do not do any prettifying of the log messages and
    # instead send them to ES. Also set the logging level lower, to INFO
    # in_prod affects the logging handlers used in ElasticsearchLoggerFactory
    if not in_prod:
        # pretty color logs
        processors.append(structlog.dev.ConsoleRenderer())

    # need this guy to go last
    processors.append(structlog.stdlib.ProcessorFormatter.wrap_for_formatter)

    # context_class guy sets context to thread local so we can set logging context from
    # stats-tween and have that included in each subsequent log entry.
    structlog.configure(
        processors=processors,
        context_class=wrap_dict(dict),
        logger_factory=ElasticsearchLoggerFactory(env=env, es_server=es_server, in_prod=in_prod),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

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

    # set logging level
    # below could be used ot redirect logging to a file if desired
    if log_name is None:
        log_name = __name__

    if log_dir and log_name and level:
        import os
        log_file = os.path.join(log_dir, log_name + ".log")
        logger = structlog.get_logger(log_name)
        hdlr = logging.FileHandler(log_file)
        formatter = logging.Formatter('')
        hdlr.setFormatter(formatter)
        logger.addHandler(hdlr)
        logger.setLevel(level)
    elif level:
        logger = structlog.get_logger(log_name)
        logger.setLevel(level)
