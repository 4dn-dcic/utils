from datetime import datetime
from enum import Enum

# Constants for progress tracking for smaht-submitr.
# Here only to share between smaht-portal, snovault, and smaht-submitr.


class _Enum(Enum):
    # Automatically make enumerators within the enumeration resolve to its value property.
    def __get__(self, instance, owner):
        return self.value
    # But doing the above does not take when iterating; so make provide a values method.
    @classmethod  # noqa
    def values(cls):
        return [enumerator.value for enumerator in cls]


class PROGRESS_INGESTER(_Enum):
    VALIDATION = "ingester_validation"
    QUEUED = "ingester_queued"
    QUEUE_CLEANUP = "ingester_queue_cleanup"
    INITIATE = "ingester_initiate"
    CLEANUP = "ingester_cleanup"
    DONE = "ingester_done"
    OUTCOME = "ingester_outcome"
    PARSE_LOAD_INITIATE = "ingester_parse_initiate"
    PARSE_LOAD_DONE = "ingester_parse_done"
    VALIDATE_LOAD_INITIATE = "ingester_validate_initiate"
    VALIDATE_LOAD_DONE = "ingester_validate_done"
    LOADXL_INITIATE = "ingester_loadxl_initiate"
    LOADXL_DONE = "ingester_loadxl_done"
    MESSAGE = "ingester_message"
    MESSAGE_VERBOSE = "ingester_message_verbose"
    MESSAGE_DEBUG = "ingester_message_debug"
    NOW = lambda: _NOW()  # noqa


class PROGRESS_PARSE(_Enum):
    LOAD_START = "parse_start"
    LOAD_ITEM = "parse_item"
    LOAD_DONE = "parse_done"
    LOAD_COUNT_SHEETS = "parse_sheets"
    LOAD_COUNT_ROWS = "parse_rows"
    LOAD_COUNT_REFS = "parse_refs"
    LOAD_COUNT_REFS_FOUND = "parse_refs_found"
    LOAD_COUNT_REFS_NOT_FOUND = "parse_refs_not_found"
    LOAD_COUNT_REFS_LOOKUP = "parse_refs_lookup"
    LOAD_COUNT_REFS_LOOKUP_CACHE_HIT = "parse_refs_lookup_cache_hit"
    LOAD_COUNT_REFS_EXISTS_CACHE_HIT = "parse_refs_exists_cache_hit"
    LOAD_COUNT_REFS_INVALID = "parse_refs_invalid"
    ANALYZE_START = "parse_analyze_start"
    ANALYZE_COUNT_TYPES = "parse_analyze_types"
    ANALYZE_COUNT_ITEMS = "parse_analyze_objects"
    ANALYZE_CREATE = "parse_analyze_create"
    ANALYZE_UPDATE = "parse_analyze_update"
    ANALYZE_LOOKUPS = "parse_analyze_lookups"
    ANALYZE_DONE = "parse_analyze_done"
    MESSAGE = "parse_message"
    MESSAGE_VERBOSE = "parse_message_verbose"
    MESSAGE_DEBUG = "parse_message_debug"
    NOW = lambda: _NOW()  # noqa


class PROGRESS_LOADXL(_Enum):
    START = "loadxl_start"
    START_SECOND_ROUND = "loadxl_start_second_round"
    ITEM = "loadxl_item"
    ITEM_SECOND_ROUND = "loadxl_item_second_round"
    GET = "loadxl_lookup"
    POST = "loadxl_post"
    PATCH = "loadxl_patch"
    ERROR = "loadxl_error"
    DONE = "loadxl_done"
    TOTAL = "loadxl_total"
    MESSAGE = "loadxl_message"
    MESSAGE_VERBOSE = "loadxl_message_verbose"
    MESSAGE_DEBUG = "loadxl_message_debug"
    NOW = lambda: _NOW()  # noqa


def _NOW() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
