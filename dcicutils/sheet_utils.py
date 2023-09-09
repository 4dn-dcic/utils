import chardet
import contextlib
import copy
import csv
import glob
import io
import json
import openpyxl
import os
import re
import uuid
import yaml

from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook.workbook import Workbook
from tempfile import TemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Tuple, Type, Union
from .common import AnyJsonData
from .env_utils import public_env_name, EnvUtils
from .ff_utils import get_schema
from .lang_utils import conjoined_list, disjoined_list, maybe_pluralize, there_are
from .misc_utils import ignored, PRINT, pad_to, JsonLinesReader, AbstractVirtualApp
from .task_utils import pmap


Header = str
Headers = List[str]
ParsedHeader = List[Union[str, int]]
ParsedHeaders = List[ParsedHeader]
SheetCellValue = Union[int, float, str]
SheetRow = List[SheetCellValue]
CsvReader = type(csv.reader(TemporaryFile()))
SheetData = List[dict]
TabbedSheetData = Dict[str, SheetData]
Regexp = type(re.compile("sample"))


class LoadFailure(Exception):
    """
    In general, we'd prefer to load up the spreadsheet with clumsy data that can then be validated in detail,
    but some errors are so confusing or so problematic that we need to just fail the load right away.
    """
    pass


class LoadArgumentsError(LoadFailure):
    """
    Errors of this class represent situations where we can't get started because
    there's a problem with the given arguments.
    """
    pass


class LoadTableError(LoadFailure):
    """
    Errors of this class represent situations where we can't get started because
    there's a problem with some table's syntax, for example headers that don't make sense.
    """
    pass


@contextlib.contextmanager
def deferred_problems():
    problems = []

    def note_problems(problem):
        problems.append(problem)

    yield note_problems

    if problems:
        for problem in problems:
            PRINT(f"Problem: {problem}")
        raise Exception(there_are(problems, kind='problem while compiling hints', tense='past', show=False))


def unwanted_kwargs(*, context, kwargs, context_plural=False, detailed=False):
    if kwargs:
        unwanted = [f"{argname}={value!r}" if detailed else argname
                    for argname, value in kwargs.items()
                    if value is not None]
        if unwanted:
            does_not = "don't" if context_plural else "doesn't"
            raise LoadArgumentsError(f"{context} {does_not} use"
                                     f" {maybe_pluralize(unwanted, 'keyword argument')} {conjoined_list(unwanted)}.")


def prefer_number(value: SheetCellValue):
    if isinstance(value, str):  # the given value might be an int or float, in which case just fall through
        if not value:
            return None
        value = value
        ch0 = value[0]
        if ch0 == '+' or ch0 == '-' or ch0.isdigit():
            try:
                return int(value)
            except Exception:
                pass
            try:
                return float(value)
            except Exception:
                pass
        # If we couldn't parse it as an int or float, fall through to returning the original value
        pass
    return value


def expand_string_escape_sequences(text: str) -> str:
    s = io.StringIO()
    escaping = False
    for ch in text:
        if escaping:
            if ch == 'r':
                s.write('\r')
            elif ch == 't':
                s.write('\t')
            elif ch == 'n':
                s.write('\n')
            elif ch == '\\':
                s.write('\\')
            else:
                # Rather than err, just leave other sequences as-is.
                s.write(f"\\{ch}")
            escaping = False
        elif ch == '\\':
            escaping = True
        else:
            s.write(ch)
    return s.getvalue()


def open_unicode_text_input_file_respecting_byte_order_mark(filename):
    """
    Opens a file for text input, respecting a byte-order mark (BOM).
    """
    with io.open(filename, 'rb') as fp:
        leading_bytes = fp.read(4 * 8)  # 4 bytes is all we need
        bom_info = chardet.detect(leading_bytes, should_rename_legacy=True)
        detected_encoding = bom_info and bom_info.get('encoding')  # tread lightly
    use_encoding = 'utf-8' if detected_encoding == 'ascii' else detected_encoding
    return io.open(filename, 'r', encoding=use_encoding)


class TypeHint:
    def apply_hint(self, value):
        return value

    def __str__(self):
        return f"<{self.__class__.__name__}>"

    def __repr__(self):
        return self.__str__()


class BoolHint(TypeHint):

    def apply_hint(self, value):
        if isinstance(value, str) and value:
            if 'true'.startswith(value.lower()):
                return True
            elif 'false'.startswith(value.lower()):
                return False
        return super().apply_hint(value)


class EnumHint(TypeHint):

    def __str__(self):
        return f"<EnumHint {','.join(f'{key}={val}' for key, val in self.value_map.items())}>"

    def __init__(self, value_map):
        self.value_map = value_map

    def apply_hint(self, value):
        if isinstance(value, str):
            if value in self.value_map:
                result = self.value_map[value]
                return result
            else:
                lvalue = value.lower()
                found = []
                for lkey, key in self.value_map.items():
                    if lkey.startswith(lvalue):
                        found.append(lkey)
                if len(found) == 1:
                    [only_found] = found
                    result = self.value_map[only_found]
                    return result
        return super().apply_hint(value)


OptionalTypeHints = List[Optional[TypeHint]]


class ItemTools:
    """
    Implements operations on table-related data without pre-supposing the specific representation of the table.
    It is assumed this can be used for data that was obtained from .json, .csv, .tsv, and .xlsx files because
    it does not presuppose the source of the data nor where it will be written to.

    For the purpose of this class:

    * a 'header' is a string representing the top of a column.

    * a 'parsed header' is a list of strings and/or ints, after splitting at uses of '#' or '.', so that
      "a.b.c" is represented as ["a", "b", "c"], and "x.y#0" is represented as ["x", "y", 0], and representing
      each numeric token as an int instead of a string.

    * a 'headers' object is just a list of strings, each of which is a 'header'.

    * a 'parsed headers' object is a non-empty list of lists, each of which is a 'parsed header'.
      e..g., the headers ["a.b.c", "x.y#0"] is represented as parsed hearders [["a", "b", "c"], ["x", "y", 0]].

   """

    @classmethod
    def parse_sheet_header(cls, header: Header) -> ParsedHeader:
        result = []
        token = ""
        for i in range(len(header)):
            ch = header[i]
            if ch == '.' or ch == '#':
                if token:
                    result.append(int(token) if token.isdigit() else token)
                    token = ""
            else:
                token += ch
        if token:
            result.append(int(token) if token.isdigit() else token)
        return result

    @classmethod
    def parse_sheet_headers(cls, headers: Headers):
        return [cls.parse_sheet_header(header)
                for header in headers]

    @classmethod
    def compute_patch_prototype(cls, parsed_headers: ParsedHeaders):
        prototype = {}
        for parsed_header in parsed_headers:
            parsed_header0 = parsed_header[0]
            if isinstance(parsed_header0, int):
                raise LoadTableError(f"A header cannot begin with a numeric ref: {parsed_header0}")
            cls.assure_patch_prototype_shape(parent=prototype, keys=parsed_header)
        return prototype

    @classmethod
    def assure_patch_prototype_shape(cls, *, parent: Union[Dict, List], keys: ParsedHeader):
        [key0, *more_keys] = keys
        key1 = more_keys[0] if more_keys else None
        if isinstance(key1, int):
            placeholder = []
        elif isinstance(key1, str):
            placeholder = {}
        else:
            placeholder = None
        if isinstance(key0, int):
            n = len(parent)
            if key0 == n:
                parent.append(placeholder)
            elif key0 > n:
                raise LoadTableError("Numeric items must occur sequentially.")
        elif isinstance(key0, str):
            if key0 not in parent:
                parent[key0] = placeholder
        if key1 is not None:
            cls.assure_patch_prototype_shape(parent=parent[key0], keys=more_keys)
        return parent

    INSTAGUIDS_ENABLED = False  # Experimental feature not enabled by default

    @classmethod
    def parse_item_value(cls, value: SheetCellValue, context=None) -> AnyJsonData:
        # TODO: Remodularize this for easier testing and more Schema-driven effect
        # Doug asks that this be broken up into different mechanisms, more modular and separately testable.
        # I pretty much agree with that. I'm just waiting for suggestions on what kinds of features are desired.
        if isinstance(value, str):
            lvalue = value.lower()
            # TODO: We could consult a schema to make this less heuristic, but this may do for now
            if lvalue == 'true':
                return True
            elif lvalue == 'false':
                return False
            elif lvalue == 'null' or lvalue == '':
                return None
            elif '|' in value:
                if value == '|':  # Use '|' for []
                    return []
                else:
                    if value.endswith("|"):  # Use 'foo|' for ['foo']
                        value = value[:-1]
                    return [cls.parse_item_value(subvalue, context=context) for subvalue in value.split('|')]
            elif cls.INSTAGUIDS_ENABLED and context is not None and value.startswith('#'):
                # Note that this clause MUST follow '|' clause above so '#foo|#bar' isn't seen as instaguid
                return cls.get_instaguid(value, context=context)
            else:
                # Doug points out that the schema might not agree, might want a string representation of a number.
                # At this semantic layer, this might be a bad choice.
                return prefer_number(value)
        else:  # presumably a number (int or float)
            return value

    @classmethod
    def get_instaguid(cls, guid_placeholder: str, *, context: Optional[Dict] = None):
        if context is None:
            return guid_placeholder
        else:
            referent = context.get(guid_placeholder)
            if not referent:
                context[guid_placeholder] = referent = str(uuid.uuid4())
            return referent

    @classmethod
    def set_path_value(cls, datum: Union[List, Dict], path: ParsedHeader, value: Any, force: bool = False):
        if (value is None or value == '') and not force:
            return
        [key, *more_path] = path
        if not more_path:
            datum[key] = value
        else:
            cls.set_path_value(datum[key], more_path, value)

    @classmethod
    def find_type_hint(cls, parsed_header: Optional[ParsedHeader], schema: Any):

        def finder(subheader, subschema):
            if not parsed_header:
                return None
            else:
                [key1, *other_headers] = subheader
                if isinstance(key1, str) and isinstance(subschema, dict):
                    if subschema.get('type') == 'object':
                        def1 = subschema.get('properties', {}).get(key1)
                        if not other_headers:
                            if def1 is not None:
                                t = def1.get('type')
                                if t == 'string':
                                    enum = def1.get('enum')
                                    if enum:
                                        mapping = {e.lower(): e for e in enum}
                                        return EnumHint(mapping)
                                elif t == 'boolean':
                                    return BoolHint()
                                else:
                                    pass  # fall through to asking super()
                            else:
                                pass  # fall through to asking super()
                        else:
                            return finder(subheader=other_headers, subschema=def1)

        return finder(subheader=parsed_header, subschema=schema)

    @classmethod
    def infer_tab_name(cls, filename):
        return os.path.basename(filename).split('.')[0]


# TODO: Consider whether this might want to be an abstract base class. Some change might be needed.
#
# Doug thinks we might want (metaclass=ABCMeta) here to make this an abstract base class.
# I am less certain but open to discussion. Among other things, as implemented now,
# the __init__ method here needs to run and the documentation says that ABC's won't appear
# in the method resolution order. -kmp 17-Aug-2023
# See also discussion at https://github.com/4dn-dcic/utils/pull/276#discussion_r1297775535
class AbstractTableSetManager:
    """
    The TableSetManager is the spanning class of anything that wants to be able to load a table set,
    regardless of what it wants to load it from. To do this, it must support a load method
    that takes a filename and returns the file content in the form:
        {
            "Sheet1": [
                          {...representation of row1 as some kind of dict...},
                          {...representation of row2 as some kind of dict...}
                      ],
            "Sheet2": [...],
            ...,
        }
    It also needs some implementation of the .tab_names property.
    Note that at this level of abstraction, we take no position on what form of representation is used
    for the rows, as long as it is JSON data of some kind. It might be
         {"col1": "val1", "col2": "val2", ...}
    or it might be something more structured like
         {"something": "val1", {"something_else": ["val2"]}}
    Additionally, the values stored might be altered as well. In particular, the most likely alteration
    is to turn "123" to 123 or "" to None, though the specifics of whether and how such transformations
    happen is not constrained by this class.
    """

    ALLOWED_FILE_EXTENSIONS: List[str] = []

    def __init__(self, filename: str, **kwargs):
        self.filename: str = filename
        unwanted_kwargs(context=self.__class__.__name__, kwargs=kwargs)

    # TODO: Consider whether this should be an abstractmethod (but first see detailed design note at top of class.)
    @classmethod
    def load(cls, filename: str, **kwargs) -> TabbedSheetData:
        """
        Reads a filename and returns a dictionary that maps sheet names to rows of dictionary data.
        For more information, see documentation of AbstractTableSetManager.
        """
        raise NotImplementedError(f".load(...) is not implemented for {cls.__name__}.")  # noQA

    @property
    def tab_names(self) -> List[str]:
        raise NotImplementedError(f".tab_names is not implemented for {self.__class__.__name__}..")  # noQA

    def load_content(self) -> Any:
        raise NotImplementedError(f".load_content() is not implemented for {self.__class__.__name__}.")  # noQA


class BasicTableSetManager(AbstractTableSetManager):
    """
    A BasicTableManager provides some structure that most kinds of parsers will need.
    In particular, everything will likely need some way of storing headers and some way of storing content
    of each sheet. Even a csv file, which doesn't have multiple tabs can be seen as the degenerate case
    of this where there's only one set of headers and only one block of content.
    """

    def __init__(self, filename: str, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.headers_by_tab_name: Dict[str, Headers] = {}
        self.content_by_tab_name: Dict[str, SheetData] = {}
        self.reader_agent: Any = self._get_reader_agent()

    def tab_headers(self, tab_name: str) -> Headers:
        return self.headers_by_tab_name[tab_name]

    def tab_content(self, tab_name: str) -> List[AnyJsonData]:
        return self.content_by_tab_name[tab_name]

    @classmethod
    def _create_tab_processor_state(cls, tab_name: str) -> Any:
        """
        This method provides for the possibility that some parsers will want auxiliary state,
        (such as parsed headers or a line count or a table of temporary names for objects to cross-link
        or some other such feature) that it carries with it as it moves from line to line parsing things.
        Subclasses might therefore want to make this do something more interesting.
        """
        ignored(tab_name)  # subclasses might need this, but we don't
        return None

    def _get_reader_agent(self) -> Any:
        """This function is responsible for opening the workbook and returning a workbook object."""
        raise NotImplementedError(f"._get_reader_agent() is not implemented for {self.__class__.__name__}.")  # noQA


class SemanticTableSetManager(BasicTableSetManager):
    """
    This is the base class for all workbook-like data sources, i.e., that may need to apply semantic processing.
    Those may be:
    * Excel workbook readers (.xlsx)
    * Comma-separated file readers (.csv)
    * Tab-separarated file readers (.tsv in most of the world, but Microsoft stupidly calls this .txt, outright
      refusing to write a .tsv file, so many people seem to compromise and call this .tsv.txt)
    There are two levels to each of these: a class that is not semantically interpreted,
    and a class that is semantically interpreted as an "item".

    This is NOT a parent class of these kinds of files, which we always take literally as if semantic processing
    were already done (in part so that they can be used to test the results of other formats):
    * Json files
    * Yaml files
    * Inserts directories
    * JsonLines files
    """

    @classmethod
    def load(cls, filename: str, **kwargs) -> AnyJsonData:
        if cls.ALLOWED_FILE_EXTENSIONS:
            if not any(filename.lower().endswith(suffix) for suffix in cls.ALLOWED_FILE_EXTENSIONS):
                raise LoadArgumentsError(f"The TableSetManager subclass {cls.__name__} expects only"
                                         f" {disjoined_list(cls.ALLOWED_FILE_EXTENSIONS)} filenames: {filename}")

        table_set_manager: SemanticTableSetManager = cls(filename=filename, **kwargs)
        return table_set_manager.load_content()

    def __init__(self, filename: str, **kwargs):
        super().__init__(filename=filename, **kwargs)

    def _raw_row_generator_for_tab_name(self, tab_name: str) -> Iterable[SheetRow]:
        """
        Given a tab_name and a state (returned by _sheet_loader_state), return a generator for a set of row values.
        """
        raise NotImplementedError(f"._rows_for_tab_name(...) is not implemented for {self.__class__.__name__}.")  # noQA

    def _process_row(self, tab_name: str, state: Any, row: List[SheetCellValue]) -> AnyJsonData:
        """
        This needs to take a state and whatever represents a row and
        must return a list of objects representing column values.
        What constitutes a processed up to the class, but other than that the result must be a JSON dictionary.
        """
        raise NotImplementedError(f"._process_row(...) is not implemented for {self.__class__.__name__}.")  # noQA

    def load_content(self) -> AnyJsonData:
        for tab_name in self.tab_names:
            sheet_content = []
            state = self._create_tab_processor_state(tab_name)
            for row_data in self._raw_row_generator_for_tab_name(tab_name):
                processed_row_data: AnyJsonData = self._process_row(tab_name, state, row_data)
                sheet_content.append(processed_row_data)
            self.content_by_tab_name[tab_name] = sheet_content
        return self.content_by_tab_name

    @classmethod
    def parse_cell_value(cls, value: SheetCellValue) -> AnyJsonData:
        return prefer_number(value)


class AbstractItemManager(AbstractTableSetManager):

    pass


class TableSetManagerRegistry:

    def __init__(self):
        self.manager_table: Dict[str, Type[AbstractTableSetManager]] = {}
        self.regexp_mappings: List[Tuple[Regexp, Type[AbstractTableSetManager]]] = []

    def register(self, regexp: Optional[str] = None):
        def _wrapped_register(class_to_register: Type[AbstractTableSetManager]):
            if regexp:
                self.regexp_mappings.append((re.compile(regexp), class_to_register))
            for ext in class_to_register.ALLOWED_FILE_EXTENSIONS:
                existing = self.manager_table.get(ext)
                if existing:
                    raise Exception(f"Tried to define {class_to_register} to extension {ext},"
                                    f" but {existing} already claimed that.")
                self.manager_table[ext] = class_to_register
            return class_to_register
        return _wrapped_register

    register1 = register

    def manager_for_filename(self, filename: str) -> Type[AbstractTableSetManager]:
        base: str = os.path.basename(filename)
        suffix_parts = base.split('.')[1:]
        if suffix_parts:
            for i in range(0, len(suffix_parts)):
                suffix = f".{'.'.join(suffix_parts[i:])}"
                found: Optional[Type[AbstractTableSetManager]] = self.manager_table.get(suffix)
                if found:
                    return found
        else:
            special_case: Optional[Type[AbstractItemManager]] = self.manager_for_special_filename(filename)
            if special_case:
                return special_case
        raise LoadArgumentsError(f"Unknown file type: {filename}")

    def manager_for_special_filename(self, filename: str) -> Optional[Type[AbstractTableSetManager]]:
        for pattern, manager_class in self.regexp_mappings:
            if pattern.match(filename):
                return manager_class
        return None


TABLE_SET_MANAGER_REGISTRY = TableSetManagerRegistry()
ITEM_MANAGER_REGISTRY = TableSetManagerRegistry()


@TABLE_SET_MANAGER_REGISTRY.register()
class XlsxManager(SemanticTableSetManager):
    """
    This implements the mechanism to get a series of rows out of the sheets in an XLSX file.
    """

    ALLOWED_FILE_EXTENSIONS = ['.xlsx']

    @classmethod
    def _all_rows(cls, sheet: Worksheet):
        row_max = sheet.max_row
        for row in range(2, row_max + 1):
            yield row

    @classmethod
    def _all_cols(cls, sheet: Worksheet):
        col_max = sheet.max_column
        for col in range(1, col_max + 1):
            yield col

    @property
    def tab_names(self) -> List[str]:
        return self.reader_agent.sheetnames

    def _get_reader_agent(self) -> Workbook:
        return openpyxl.load_workbook(self.filename)

    def _raw_row_generator_for_tab_name(self, tab_name: str) -> Iterable[SheetRow]:
        sheet = self.reader_agent[tab_name]
        return (self._get_raw_row_content_tuple(sheet, row)
                for row in self._all_rows(sheet))

    def _get_raw_row_content_tuple(self, sheet: Worksheet, row: int) -> SheetRow:
        return [sheet.cell(row=row, column=col).value
                for col in self._all_cols(sheet)]

    def _create_tab_processor_state(self, tab_name: str) -> Headers:
        sheet = self.reader_agent[tab_name]
        headers: Headers = [str(sheet.cell(row=1, column=col).value)
                            for col in self._all_cols(sheet)]
        self.headers_by_tab_name[sheet.title] = headers
        return headers

    def _process_row(self, tab_name: str, headers: Headers, row_data: SheetRow) -> AnyJsonData:
        ignored(tab_name)
        return {headers[i]: self.parse_cell_value(row_datum)
                for i, row_datum in enumerate(row_data)}


class SchemaAutoloadMixin(AbstractTableSetManager):

    SCHEMA_CACHE = {}  # Shared cache. Do not override. Use .clear_schema_cache() to clear it.
    CACHE_SCHEMAS = True  # Controls whether we're doing caching at all
    AUTOLOAD_SCHEMAS_DEFAULT = True

    def __init__(self, filename: str, autoload_schemas: Optional[bool] = None, portal_env: Optional[str] = None,
                 portal_vapp: Optional[AbstractVirtualApp] = None, **kwargs):
        # This setup must be in place before the class initialization is done (via the super call).
        self.autoload_schemas: bool = self.AUTOLOAD_SCHEMAS_DEFAULT if autoload_schemas is None else autoload_schemas
        if self.autoload_schemas:  # If autoload_schemas is False, we don't care about doing this defaulting.
            if portal_env is None and portal_vapp is None:
                portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)
                PRINT(f"The portal_env was not explicitly supplied. Schemas will come from portal_env={portal_env!r}.")
        self.portal_env: Optional[str] = portal_env
        self.portal_vapp: Optional[AbstractVirtualApp] = portal_vapp
        super().__init__(filename=filename, **kwargs)

    def fetch_relevant_schemas(self, schema_names: List[str]):
        # The schema_names argument is not normally given, but it is there for easier testing
        def fetch_schema(schema_name):
            schema = self.fetch_schema(schema_name, portal_env=self.portal_env, portal_vapp=self.portal_vapp)
            return schema_name, schema
        if self.autoload_schemas and (self.portal_env or self.portal_vapp):
            autoloaded = {tab_name: schema
                          for tab_name, schema in pmap(fetch_schema, schema_names)}
            return autoloaded
        else:
            return {}

    @classmethod
    def fetch_schema(cls, schema_name: str, *, portal_env: Optional[str] = None,
                     portal_vapp: Optional[AbstractVirtualApp] = None):
        def just_fetch_it():
            return get_schema(schema_name, portal_env=portal_env, portal_vapp=portal_vapp)
        if cls.CACHE_SCHEMAS:
            schema: Optional[AnyJsonData] = cls.SCHEMA_CACHE.get(schema_name)
            if schema is None:
                cls.SCHEMA_CACHE[schema_name] = schema = just_fetch_it()
            return schema
        else:
            return just_fetch_it()

    @classmethod
    def clear_schema_cache(cls):
        for key in list(cls.SCHEMA_CACHE.keys()):  # important to get the list of keys as a separate object first
            cls.SCHEMA_CACHE.pop(key, None)


class ItemManagerMixin(SchemaAutoloadMixin, AbstractItemManager, BasicTableSetManager):
    """
    This can add functionality to a reader such as an XlsxManager or a CsvManager in order to make its rows
    get handled like Items instead of just flat table rows.
    """

    def __init__(self, filename: str, schemas: Optional[Dict[str, AnyJsonData]] = None, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.patch_prototypes_by_tab_name: Dict[str, Dict] = {}
        self.parsed_headers_by_tab_name: Dict[str, ParsedHeaders] = {}
        self.type_hints_by_tab_name: Dict[str, OptionalTypeHints] = {}
        self._schemas = schemas
        self._instaguid_context_table: Dict[str, str] = {}

    @property
    def schemas(self):
        schemas = self._schemas
        if schemas is None:
            self._schemas = schemas = self.fetch_relevant_schemas(self.tab_names)
        return schemas

    def sheet_patch_prototype(self, tab_name: str) -> Dict:
        return self.patch_prototypes_by_tab_name[tab_name]

    def sheet_parsed_headers(self, tab_name: str) -> ParsedHeaders:
        return self.parsed_headers_by_tab_name[tab_name]

    def sheet_type_hints(self, tab_name: str) -> OptionalTypeHints:
        return self.type_hints_by_tab_name[tab_name]

    class SheetState:

        def __init__(self, parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
            self.parsed_headers = parsed_headers
            self.type_hints = type_hints

    def _compile_type_hints(self, tab_name: str):
        parsed_headers = self.sheet_parsed_headers(tab_name)
        schema = self.schemas.get(tab_name)
        with deferred_problems() as note_problem:
            for required_header in self._schema_required_headers(schema):
                if required_header not in parsed_headers:
                    note_problem("Missing required header")
        type_hints = [ItemTools.find_type_hint(parsed_header, schema) if schema else None
                      for parsed_header in parsed_headers]
        self.type_hints_by_tab_name[tab_name] = type_hints

    @classmethod
    def _schema_required_headers(cls, schema):
        ignored(schema)
        return []  # TODO: Make this compute a list of required headers (in parsed header form)

    def _compile_sheet_headers(self, tab_name: str):
        headers = self.headers_by_tab_name[tab_name]
        parsed_headers = ItemTools.parse_sheet_headers(headers)
        self.parsed_headers_by_tab_name[tab_name] = parsed_headers
        prototype = ItemTools.compute_patch_prototype(parsed_headers)
        self.patch_prototypes_by_tab_name[tab_name] = prototype

    def _create_tab_processor_state(self, tab_name: str) -> SheetState:
        super()._create_tab_processor_state(tab_name)
        # This will create state that allows us to efficiently assign values in the right place on each row
        # by setting up a prototype we can copy and then drop values into.
        self._compile_sheet_headers(tab_name)
        self._compile_type_hints(tab_name)
        return self.SheetState(parsed_headers=self.sheet_parsed_headers(tab_name),
                               type_hints=self.sheet_type_hints(tab_name))

    def _process_row(self, tab_name: str, state: SheetState, row_data: SheetRow) -> AnyJsonData:
        parsed_headers = state.parsed_headers
        type_hints = state.type_hints
        patch_item = copy.deepcopy(self.sheet_patch_prototype(tab_name))
        for i, value in enumerate(row_data):
            parsed_value = self.parse_cell_value(value)
            type_hint = type_hints[i]
            if type_hint:
                parsed_value = type_hint.apply_hint(parsed_value)
            ItemTools.set_path_value(patch_item, parsed_headers[i], parsed_value)
        return patch_item

    def parse_cell_value(self, value: SheetCellValue) -> AnyJsonData:
        return ItemTools.parse_item_value(value, context=self._instaguid_context_table)


@ITEM_MANAGER_REGISTRY.register()
class XlsxItemManager(ItemManagerMixin, XlsxManager):
    """
    This layers item-style row processing functionality on an XLSX file.
    """
    pass


class SingleTableMixin(AbstractTableSetManager):

    def __init__(self, filename: str, tab_name: Optional[str] = None, **kwargs):
        self._tab_name = tab_name or ItemTools.infer_tab_name(filename)
        super().__init__(filename=filename, **kwargs)

    @property
    def tab_names(self) -> List[str]:
        return [self._tab_name]


class InsertsManager(BasicTableSetManager):  # ItemManagerMixin isn't really appropriate here

    ALLOWED_FILE_EXTENSIONS = []

    def _parse_inserts_data(self, filename: str) -> AnyJsonData:
        raise NotImplementedError(f"._parse_inserts_dataa(...) is not implemented for {self.__class__.__name__}.")  # noQA

    def _load_inserts_data(self, filename: str) -> TabbedSheetData:
        data: AnyJsonData = self._parse_inserts_data(filename)
        tabbed_inserts: AnyJsonData = self._wrap_inserts_data(filename, data)
        if (not isinstance(tabbed_inserts, dict)
                or not all(isinstance(tab_name, str) for tab_name in tabbed_inserts.keys())
                or not all(isinstance(content, list) and all(isinstance(item, dict) for item in content)
                           for content in tabbed_inserts.values())):
            raise ValueError(f"Data in {filename} is not of type TabbedSheetData (Dict[str, List[dict]]).")
        tabbed_inserts: TabbedSheetData  # we've just checked that
        return tabbed_inserts

    @classmethod
    def _wrap_inserts_data(cls, filename: str, data: AnyJsonData) -> AnyJsonData:
        ignored(filename)
        return data

    @property
    def tab_names(self) -> List[str]:
        return list(self.content_by_tab_name.keys())

    def _get_reader_agent(self) -> Any:
        return self

    def load_content(self) -> Dict[str, AnyJsonData]:
        data = self._load_inserts_data(self.filename)
        for tab_name, tab_content in data.items():
            self.content_by_tab_name[tab_name] = tab_content
            if not tab_content:
                self.headers_by_tab_name[tab_name] = []
            else:
                self.headers_by_tab_name[tab_name] = list(tab_content[0].keys())
        return self.content_by_tab_name


class SimpleInsertsMixin(SingleTableMixin):

    def _wrap_inserts_data(self, filename: str, data: AnyJsonData) -> TabbedSheetData:
        if (not isinstance(data, list)
                or not all(isinstance(item, dict) for item in data)):
            raise ValueError(f"Data in {filename} is not of type SheetData (List[dict]).")
        return {self._tab_name: data}


class JsonInsertsMixin:

    @classmethod
    def _parse_inserts_data(cls, filename: str) -> AnyJsonData:
        return json.load(open_unicode_text_input_file_respecting_byte_order_mark(filename))


@TABLE_SET_MANAGER_REGISTRY.register()
class TabbedJsonInsertsManager(JsonInsertsMixin, InsertsManager):

    ALLOWED_FILE_EXTENSIONS = [".tabs.json"]  # If you want them all in one family, use this extension


@TABLE_SET_MANAGER_REGISTRY.register()
class SimpleJsonInsertsManager(SimpleInsertsMixin, JsonInsertsMixin, InsertsManager):

    ALLOWED_FILE_EXTENSIONS = [".json"]


class YamlInsertsMixin:

    def _parse_inserts_data(self, filename) -> AnyJsonData:
        return yaml.safe_load(open_unicode_text_input_file_respecting_byte_order_mark(filename))


@TABLE_SET_MANAGER_REGISTRY.register()
class TabbedYamlInsertsManager(YamlInsertsMixin, InsertsManager):

    ALLOWED_FILE_EXTENSIONS = [".tabs.yaml"]

    def _parse_inserts_data(self, filename) -> AnyJsonData:
        return yaml.safe_load(open_unicode_text_input_file_respecting_byte_order_mark(filename))


@TABLE_SET_MANAGER_REGISTRY.register()
class SimpleYamlInsertsManager(SimpleInsertsMixin, YamlInsertsMixin, InsertsManager):

    ALLOWED_FILE_EXTENSIONS = [".yaml"]


class InsertsItemMixin(AbstractItemManager):  # ItemManagerMixin isn't really appropriate here
    """
    This class is used for inserts directories and other JSON-like data that will be literally used as an Item
    without semantic pre-processing. In other words, these classes will not be pre-checked for semantic correctness
    but instead assumed to have been checked by other means.
    """

    AUTOLOAD_SCHEMAS_DEFAULT = False  # Has no effect, but someone might inspect the value.

    def __init__(self, filename: str, *, autoload_schemas: Optional[bool] = None, portal_env: Optional[str] = None,
                 portal_vapp: Optional[AbstractVirtualApp] = None, schemas: Optional[Dict[str, AnyJsonData]] = None,
                 **kwargs):
        ignored(portal_env, portal_vapp)  # Would only be used if autoload_schemas was true, and we don't allow that.
        if schemas not in [None, {}]:
            raise ValueError(f"{self.__class__.__name__} does not allow schemas={schemas!r}.")
        if autoload_schemas not in [None, False]:
            raise ValueError(f"{self.__class__.__name__} does not allow autoload_schemas={autoload_schemas!r}.")
        super().__init__(filename=filename, **kwargs)


@ITEM_MANAGER_REGISTRY.register()
class TabbedJsonInsertsItemManager(InsertsItemMixin, TabbedJsonInsertsManager):
    pass


@ITEM_MANAGER_REGISTRY.register()
class SimpleJsonInsertsItemManager(InsertsItemMixin, SimpleJsonInsertsManager):
    pass


@ITEM_MANAGER_REGISTRY.register()
class TabbedYamlInsertsItemManager(InsertsItemMixin, TabbedYamlInsertsManager):
    pass


@ITEM_MANAGER_REGISTRY.register()
class SimpleYamlInsertsItemManager(InsertsItemMixin, SimpleYamlInsertsManager):
    pass


@TABLE_SET_MANAGER_REGISTRY.register()
class SimpleJsonLinesInsertsManager(SimpleInsertsMixin, InsertsManager):

    ALLOWED_FILE_EXTENSIONS = [".jsonl"]

    def _parse_inserts_data(self, filename: str) -> AnyJsonData:
        return [line for line in JsonLinesReader(open_unicode_text_input_file_respecting_byte_order_mark(filename))]


@ITEM_MANAGER_REGISTRY.register()
class SimpleJsonLinesInsertsItemManager(InsertsItemMixin, SimpleJsonLinesInsertsManager):
    pass


@TABLE_SET_MANAGER_REGISTRY.register(regexp="^(.*/)?(|[^/]*[-_])inserts/?$")
class InsertsDirectoryManager(InsertsManager):

    ALLOWED_FILE_EXTENSIONS = []

    def _parse_inserts_data(self, filename: str) -> AnyJsonData:
        if not os.path.isdir(filename):
            raise LoadArgumentsError(f"{filename} is not the name of an inserts directory.")
        tab_files = glob.glob(os.path.join(filename, "*.json"))
        data = {}
        for tab_file in tab_files:
            tab_content = json.load(open_unicode_text_input_file_respecting_byte_order_mark(tab_file))
            # Here we don't use os.path.splitext because we want to split on the first dot.
            # e.g., for foo.bar.baz, return just foo
            #       this allows names like ExperimentSet.tab.json that might need to use multi-dot suffixes
            #       for things unrelated to the tab name.
            tab_name = os.path.basename(tab_file).split('.')[0]
            data[tab_name] = tab_content
        return data


@ITEM_MANAGER_REGISTRY.register(regexp="^(.*/)?(|[^/]*[-_])inserts/?$")
class InsertsDirectoryItemManager(InsertsItemMixin, InsertsDirectoryManager):
    pass


@TABLE_SET_MANAGER_REGISTRY.register()
class CsvManager(SingleTableMixin, SemanticTableSetManager):
    """
    This implements the mechanism to get a series of rows out of the sheet in a csv file,
    returning a result that still looks like there could have been multiple tabs.
    """

    ALLOWED_FILE_EXTENSIONS = ['.csv']

    def __init__(self, filename: str, escaping: Optional[bool] = None, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.escaping: bool = escaping or False

    def _get_reader_agent(self) -> CsvReader:
        return self._get_reader_agent_for_filename(self.filename)

    @classmethod
    def _get_reader_agent_for_filename(cls, filename) -> CsvReader:
        return csv.reader(open_unicode_text_input_file_respecting_byte_order_mark(filename))

    PAD_TRAILING_TABS = True

    def _raw_row_generator_for_tab_name(self, tab_name: str) -> Iterable[SheetRow]:
        headers = self.tab_headers(tab_name)
        n_headers = len(headers)
        for row_data in self.reader_agent:
            if self.PAD_TRAILING_TABS:
                row_data = pad_to(n_headers, row_data, padding='')
            yield row_data

    def _create_tab_processor_state(self, tab_name: str) -> Headers:
        headers: Optional[Headers] = self.headers_by_tab_name.get(tab_name)
        if headers is None:
            self.headers_by_tab_name[tab_name] = headers = self.reader_agent.__next__()
        return headers

    @classmethod
    def _escape_cell_text(cls, cell_text):
        if '\\' in cell_text:
            return expand_string_escape_sequences(cell_text)
        else:
            return cell_text

    def _process_row(self, tab_name: str, headers: Headers, row_data: SheetRow) -> AnyJsonData:
        ignored(tab_name)
        if self.escaping:
            return {headers[i]: self.parse_cell_value(self._escape_cell_text(cell_text))
                    for i, cell_text in enumerate(row_data)}
        else:
            return {headers[i]: self.parse_cell_value(cell_text)
                    for i, cell_text in enumerate(row_data)}


@ITEM_MANAGER_REGISTRY.register()
class CsvItemManager(ItemManagerMixin, CsvManager):
    """
    This layers item-style row processing functionality on a CSV file.
    """
    pass


@TABLE_SET_MANAGER_REGISTRY.register()
class TsvManager(CsvManager):
    """
    TSV files are just CSV files with tabs instead of commas as separators.
    (We do not presently handle any escaping of strange characters. May need to add handling for backslash escaping.)
    """
    ALLOWED_FILE_EXTENSIONS = ['.tsv', '.tsv.txt']

    @classmethod
    def _get_reader_agent_for_filename(cls, filename) -> CsvReader:
        return csv.reader(open_unicode_text_input_file_respecting_byte_order_mark(filename), delimiter='\t')


@ITEM_MANAGER_REGISTRY.register()
class TsvItemManager(ItemManagerMixin, TsvManager):
    """
    This layers item-style row processing functionality on a TSV file.
    """
    pass


class TableSetManager(AbstractTableSetManager):
    """
    This class will open a .xlsx or .csv file and load its content in our standard format.
    (See more detailed description in AbstractTableManager.)
    """

    @classmethod
    def create_implementation_manager(cls, filename: str, **kwargs) -> AbstractTableSetManager:
        reader_agent_class = TABLE_SET_MANAGER_REGISTRY.manager_for_filename(filename)
        if issubclass(reader_agent_class, AbstractItemManager):
            raise ValueError(f"TableSetManager unexpectedly found reader agent class {reader_agent_class}.")
        reader_agent = reader_agent_class(filename=filename, **kwargs)
        return reader_agent

    @classmethod
    def load(cls, filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
             **kwargs) -> TabbedSheetData:
        """
        Given a filename and various options
        """
        manager = cls.create_implementation_manager(filename=filename, tab_name=tab_name, escaping=escaping, **kwargs)
        return manager.load_content()


class ItemManager(AbstractTableSetManager):
    """
    This class will open a .xlsx or .csv file and load its content in our standard format.
    (See more detailed description in AbstractTableManager.)
    """

    @classmethod
    def create_implementation_manager(cls, filename: str, **kwargs) -> AbstractItemManager:
        reader_agent_class: Type[AbstractTableSetManager] = ITEM_MANAGER_REGISTRY.manager_for_filename(filename)
        if not issubclass(reader_agent_class, AbstractItemManager):
            raise ValueError(f"ItemManager unexpectedly found reader agent class {reader_agent_class}.")
        reader_agent_class: Type[AbstractItemManager]
        reader_agent = reader_agent_class(filename=filename, **kwargs)
        return reader_agent

    @classmethod
    def load(cls, filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
             schemas: Optional[Dict] = None, autoload_schemas: Optional[bool] = None,
             portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None,
             **kwargs) -> TabbedSheetData:
        """
        Given a filename and various options, loads the items associated with that filename.

        :param filename: The name of the file to load.
        :param tab_name: For files that lack multiple tabs (such as .csv or .tsv),
            the tab name to associate with the data.
        :param escaping: Whether to perform escape processing on backslashes.
        :param schemas: A set of schemas to use instead of trying to load them.
        :param autoload_schemas: Whether to try autoloading schemas.
        :param portal_env: A portal to consult to find schemas (usually if calling from the outside of a portal).
        :param portal_vapp: A vapp to use (usually if calling from within a portal).
        """
        manager = cls.create_implementation_manager(filename=filename, tab_name=tab_name, escaping=escaping,
                                                    schemas=schemas, autoload_schemas=autoload_schemas,
                                                    portal_env=portal_env, portal_vapp=portal_vapp,
                                                    **kwargs)
        return manager.load_content()


load_table_set = TableSetManager.load
load_items = ItemManager.load
