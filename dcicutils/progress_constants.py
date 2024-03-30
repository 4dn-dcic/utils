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


class PROGRESS_PARSE(Enum):
    LOAD_START = "start"
    LOAD_ITEM = "parse"
    LOAD_DONE = "finish"
    LOAD_COUNT_SHEETS = "sheets"
    LOAD_COUNT_ROWS = "rows"
    LOAD_COUNT_REFS = "refs"
    LOAD_COUNT_REFS_FOUND = "refs_found"
    LOAD_COUNT_REFS_NOT_FOUND = "refs_not_found"
    LOAD_COUNT_REFS_LOOKUP = "refs_lookup"
    LOAD_COUNT_REFS_LOOKUP_CACHE_HIT = "refs_lookup_cache_hit"
    LOAD_COUNT_REFS_EXISTS_CACHE_HIT = "refs_exists_cache_hit"
    LOAD_COUNT_REFS_INVALID = "refs_invalid"
    ANALYZE_START = "start"
    ANALYZE_COUNT_TYPES = "types"
    ANALYZE_COUNT_ITEMS = "objects"
    ANALYZE_CREATE = "create"
    ANALYZE_COUNT_LOOKUP = "lookups"
    ANALYZE_UPDATE = "update"
    ANALYZE_DONE = "finish"


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
