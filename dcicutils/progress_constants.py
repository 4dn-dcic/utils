from enum import Enum


# Constants for progress tracking for smaht-submitr.
# Here only to share between smaht-portal, snovault, and smaht-submitr.

class PROGRESS_INGESTER(Enum):
    VALIDATION = "ingester_validation"
    INITIATE = "ingester_initiate"
    PARSE_LOAD_INITIATE = "ingester_parse_initiate"
    PARSE_LOAD_DONE = "ingester_parse_done"
    VALIDATE_LOAD_INITIATE = "ingester_validate_initiate"
    VALIDATE_LOAD_DONE = "ingester_validate_done"
    LOADXL_INITIATE = "ingester_loadxl_initiate"
    LOADXL_DONE = "ingester_loadxl_done"
    MESSAGE = "ingester_message"
    MESSAGE_VERBOSE = "ingester_message_verbose"
    MESSAGE_DEBUG = "ingester_message_debug"


class PROGRESS_PARSE(Enum):
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
    ANALYZE_COUNT_LOOKUP = "parse_analyze_lookups"
    ANALYZE_CREATE = "parse_analyze_create"
    ANALYZE_UPDATE = "parse_analyze_update"
    ANALYZE_DONE = "parse_analyze_done"
    MESSAGE = "parse_message"
    MESSAGE_VERBOSE = "parse_message_verbose"
    MESSAGE_DEBUG = "parse_message_debug"


class PROGRESS_LOADXL(Enum):
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
