import logging
import structlog
from structlog.threadlocal import wrap_dict


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
def set_logging(in_prod=False, level=logging.INFO, log_name=None, log_dir=None):
    '''
    Set logging is a function to be used everywhere, to encourage all subsytems
    to generate structured JSON logs, for easy insertion into ES for searching and
    visualizing later.
    Currently this only JSONifies our own logs, and the bit at the very bottom
    would JSONify otehr logs, like botocore and stuff but that's  probably more
    than we want to store.

    Also sets some standard handlers for the following:
    add_logger_name - python module generating the log
    timestamper - timestamps... big suprise there
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
