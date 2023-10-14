import contextlib
import copy
# import os
# import uuid
# import warnings

from typing import Any, Dict, List, Optional, Union  # , Type
from .common import AnyJsonData  # , Regexp, CsvReader
from .env_utils import EnvUtils, public_env_name
from .ff_utils import get_schema, get_metadata
from .lang_utils import there_are
from .misc_utils import AbstractVirtualApp, ignored, PRINT, to_camel_case
from .sheet_utils import (
    TabbedSchemas, LoadTableError, prefer_number,
    Header, Headers, TabbedHeaders,
    ParsedHeader, ParsedHeaders, TabbedParsedHeaders,
    SheetCellValue, TabbedSheetData,  # SheetRow, SheetData,
    TableSetManagerRegistry, AbstractTableSetManager,  # BasicTableSetManager,
    # CsvManager, TsvManager, XlsxManager,
    # SimpleJsonInsertsManager, SimpleYamlInsertsManager, SimpleJsonLinesInsertsManager,
    # TabbedJsonInsertsManager, TabbedYamlInsertsManager,
    # InsertsDirectoryManager,
    load_table_set
)
from .task_utils import pmap


# @contextlib.contextmanager
# def deferred_problems():
#     problems = []
#
#     def note_problem(problem):
#         problems.append(problem)
#
#     yield note_problem
#
#     if problems:
#         for problem in problems:
#             PRINT(f"Problem: {problem}")
#         raise Exception(there_are(problems, kind='problem while compiling hints', tense='past', show=False))


class TypeHintContext:

    @classmethod
    def schema_exists(cls, schema_name: str) -> bool:  # noQA - PyCharm complains wrongly about return value
        ignored(schema_name)
        raise NotImplementedError(f"{cls.__name__}.schema_exists(...) is not implemented.")

    @classmethod
    def validate_ref(cls, item_type: str, item_ref: str) -> str:  # noQA - PyCharm complains wrongly about return value
        ignored(item_type, item_ref)
        raise NotImplementedError(f"{cls.__name__}.validate_ref(...) is not implemented.")

    @classmethod
    def note_problem(cls, problem: str):
        ignored(problem)
        raise NotImplementedError(f"{cls.__name__}.note_problem(...) is not implemented.")

    def __str__(self):
        return f"<{self.__class__.__name__} {id(self)}>"


class ValidationProblem(Exception):
    pass


class TypeHint:
    def apply_hint(self, value):
        return value

    def __str__(self):
        return f"<{self.__class__.__name__}>"

    def __repr__(self):
        return self.__str__()


class BoolHint(TypeHint):

    # We could use other ways to do this, such as initial substring, but this is more likely to be right.
    # Then again, we might want to consder accepting athers like 'yes/no', 'y/n', 'on/off', '1/0'.
    TRUE_VALUES = ['true', 't']
    FALSE_VALUES = ['false', 'f']

    def apply_hint(self, value):
        if isinstance(value, str) and value:
            l_value = value.lower()
            if l_value in self.TRUE_VALUES:
                return True
            elif l_value in self.FALSE_VALUES:
                return False
        return super().apply_hint(value)


class NumHint(TypeHint):

    PREFERENCE_MAP = {'number': 'num', 'integer': 'int', 'float': 'float'}

    def __init__(self, declared_type):
        self.preferred_type = self.PREFERENCE_MAP.get(declared_type)

    def apply_hint(self, value):
        if isinstance(value, str) and value:
            if self.preferred_type:
                return prefer_number(value, kind=self.preferred_type)
            else:
                return value
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


class RefHint(TypeHint):

    def __str__(self):
        return f"<RefHint {self.schema_name} context={self.context}>"

    def __init__(self, schema_name: str, context: TypeHintContext):
        self.schema_name = schema_name
        self.context = context

    def apply_hint(self, value):
        if not self.context.validate_ref(item_type=self.schema_name, item_ref=value):
            raise ValidationProblem(f"Unable to validate {self.schema_name} reference: {value!r}")
        return value


OptionalTypeHints = List[Optional[TypeHint]]


class AbstractStructureManager(AbstractTableSetManager):

    pass


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
    def parse_sheet_headers(cls, headers: Headers) -> ParsedHeaders:
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
    def parse_item_value(cls, value: SheetCellValue) -> AnyJsonData:
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
                    return [cls.parse_item_value(subvalue) for subvalue in value.split('|')]
            else:
                # Doug points out that the schema might not agree, might want a string representation of a number.
                # At this semantic layer, this might be a bad choice.
                # return prefer_number(value)
                return value
        else:  # presumably a number (int or float)
            return value

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
    def find_type_hint(cls, parsed_header: Optional[ParsedHeader], schema: Any, context: Optional[TypeHintContext] = None):

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
                                    link_to = def1.get('linkTo')
                                    if link_to and context.schema_exists(link_to):
                                        return RefHint(schema_name=link_to, context=context)
                                elif t in ('integer', 'float', 'number'):
                                    return NumHint(declared_type=t)
                                elif t == 'boolean':
                                    return BoolHint()
                                else:
                                    pass  # fall through to asking super()
                            else:
                                pass  # fall through to asking super()
                        else:
                            return finder(subheader=other_headers, subschema=def1)

        return finder(subheader=parsed_header, subschema=schema)


class SchemaManager:

    SCHEMA_CACHE = {}  # Shared cache. Do not override. Use .clear_schema_cache() to clear it.

    @classmethod
    @contextlib.contextmanager
    def fresh_schema_manager_context_for_testing(cls):
        old_schema_cache = cls.SCHEMA_CACHE
        try:
            cls.SCHEMA_CACHE = {}
            yield
        finally:
            cls.SCHEMA_CACHE = old_schema_cache

    def __init__(self, portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None):
        if portal_env is None and portal_vapp is None:
            portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)
            PRINT(f"The portal_env was not explicitly supplied. Schemas will come from portal_env={portal_env!r}.")
        self.portal_env = portal_env
        self.portal_vapp = portal_vapp

    def fetch_relevant_schemas(self, schema_names: List[str], schemas: Optional[TabbedSchemas] = None):
        if schemas is None:
            schemas = {}
        # The schema_names argument is not normally given, but it is there for easier testing
        def fetch_schema(schema_name):
            schema = schemas.get(schema_name)
            schema = (self.fetch_schema(schema_name, portal_env=self.portal_env, portal_vapp=self.portal_vapp)
                      if schema is None
                      else schema)
            return schema_name, schema
        return {schema_name: schema
                for schema_name, schema in pmap(fetch_schema, schema_names)}

    @classmethod
    def schema_exists(cls, schema_name: str, *, portal_env: Optional[str] = None,
                     portal_vapp: Optional[AbstractVirtualApp] = None):
        return bool(cls.fetch_schema(schema_name=schema_name, portal_env=portal_env, portal_vapp=portal_vapp))

    @classmethod
    def fetch_schema(cls, schema_name: str, *, portal_env: Optional[str] = None,
                     portal_vapp: Optional[AbstractVirtualApp] = None):
        schema: Optional[AnyJsonData] = cls.SCHEMA_CACHE.get(schema_name)
        if schema is None and schema_name not in cls.SCHEMA_CACHE:  # If None is already stored, don't look it up again
            schema = get_schema(schema_name, portal_env=portal_env, portal_vapp=portal_vapp)
            cls.SCHEMA_CACHE[schema_name] = schema
        return schema

    @classmethod
    def clear_schema_cache(cls):
        for key in list(cls.SCHEMA_CACHE.keys()):  # important to get the list of keys as a separate object first
            cls.SCHEMA_CACHE.pop(key, None)


ITEM_MANAGER_REGISTRY = TableSetManagerRegistry()

PatchPrototype = Dict
TabbedPatchPrototypes = Dict[str, PatchPrototype]


def extract_tabbed_headers(data: TabbedSheetData) -> TabbedHeaders:
    result: TabbedHeaders = {}
    for tab, rows in data.items():
        if rows:
            # Data is homogeneous, so whatever the headers for the first row should be the same for all
            headers: List[str] = list(rows[0].keys())
        else:
            # If there's no data in the tab, there are also no headers
            headers: List[str] = []
        result[tab] = headers
    return result


class TableInflater:
    """
    This tool can be used independently of the item tools. It doesn't involve schemas, but it does allow the
    inflation of a table with dotted names to structures. e.g., a table with headers mother.name, mother.age,
    father.name, and father.age, as in
      data = load_table_set(<some-file>)
    to bring in the flat representation with:
      {"mother.name": <mother.name>, "mother.age": <mother.age>, ...}
    one can use inflate(data) to get:
      {"mother": {"name": <mother.name>, "age": <mother.age>},
       "father:  {"name": <father.name>, "age": <father.age>}}
    Note, too, that although data != inflate(data), once inflated, inflate(inflate(data)) == inflate(data).
    """

    def __init__(self, tabbed_sheet_data: TabbedSheetData):
        self.tabbed_sheet_data: TabbedSheetData = tabbed_sheet_data
        self.headers_by_tab_name: TabbedHeaders = extract_tabbed_headers(tabbed_sheet_data)
        self.parsed_headers_by_tab_name: TabbedParsedHeaders = {
            tab_name: ItemTools.parse_sheet_headers(headers)
            for tab_name, headers in self.headers_by_tab_name.items()
        }
        self.patch_prototypes_by_tab_name: TabbedPatchPrototypes = {
            tab_name: ItemTools.compute_patch_prototype(parsed_headers)
            for tab_name, parsed_headers in self.parsed_headers_by_tab_name.items()
        }

    @property
    def tab_names(self):
        return list(self.tabbed_sheet_data.keys())

    def inflate_tabs(self):
        return {tab_name: self.inflate_tab(tab_name)
                for tab_name in self.tab_names}

    def inflate_tab(self, tab_name: str):
        prototype = self.patch_prototypes_by_tab_name[tab_name]
        parsed_headers = self.parsed_headers_by_tab_name[tab_name]
        result = [self.inflate_row(row, prototype=prototype, parsed_headers=parsed_headers)
                  for row in self.tabbed_sheet_data[tab_name]]
        return result

    @classmethod
    def inflate_row(cls, row: Dict, *, prototype: Dict, parsed_headers: ParsedHeaders):
        patch_item = copy.deepcopy(prototype)
        for column_number, column_value in enumerate(row.values()):
            parsed_value = ItemTools.parse_item_value(column_value)
            ItemTools.set_path_value(patch_item, parsed_headers[column_number], parsed_value)
        return patch_item

    @classmethod
    def inflate(cls, tabbed_sheet_data: TabbedSheetData):
        inflater = cls(tabbed_sheet_data)
        inflated = inflater.inflate_tabs()
        return inflated


inflate = TableInflater.inflate


def load_table_structures(filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
                          prefer_number: bool = True, **kwargs):
    """This differs from load_table_set only in that it inflates the content. It does not apply schemas."""
    tabbed_rows = load_table_set(filename=filename, tab_name=tab_name, escaping=escaping, prefer_number=prefer_number,
                                 **kwargs)
    tabbed_structures = inflate(tabbed_rows)
    return tabbed_structures


class TabbedItemTable:

    def __init__(self, tabbed_sheet_data: TabbedSheetData,
                 portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None):
        self.portal_env = portal_env
        self.portal_vapp = portal_vapp
        self.headers_by_tab_name: Dict[str, str] = {
            tab_name: list(rows[0].keys()) if rows else []
            for tab_name, rows in tabbed_sheet_data.items()
        }
        self.lookup_tables_by_tab_name: Dict[str, Dict[str, Dict]] = {
            tab_name: self.build_lookup_table_for_tab(tab_name, rows=rows)
            for tab_name, rows in tabbed_sheet_data.items()
        }

    def build_lookup_table_for_tab(self, tab_name: str, *, rows: List[Dict]) -> Dict[str, Dict]:
        # TODO: It might be enough to just return the keys as a set, not a full dict
        schema = get_schema(tab_name, portal_env=self.portal_env, portal_vapp=self.portal_vapp)
        possible_identifying_properties = set(schema.get("identifyingProperties") or []) | {'uuid'}
        identifying_properties = [prop
                                  for prop in self.headers_by_tab_name[tab_name]
                                  if prop in possible_identifying_properties]
        if not identifying_properties:
            # Maybe issue a warning here that we're going to lose
            empty_lookup_table: Dict[str, Dict] = {}
            return empty_lookup_table
        lookup_table: Dict[str, Dict] = {}
        for row in rows:
            for identifying_property in identifying_properties:
                value = row.get(identifying_property)
                if value is not '' and value is not None:
                    lookup_table[str(value)] = row
        return lookup_table

    def contains_ref(self, item_type, item_ref):
        ref = self.resolve_ref(item_type=item_type, item_ref=item_ref)
        if ref is None:
            return False
        else:
            return True

    def resolve_ref(self, item_type, item_ref):
        lookup_table = self.lookup_tables_by_tab_name.get(item_type)
        if lookup_table:  # Is it a type we're tracking?
            return lookup_table.get(item_ref) or None
        else:  # Apparently some stray type not in our tables
            return None


class TableChecker(TableInflater, TypeHintContext):

    def __init__(self, tabbed_sheet_data: TabbedSheetData, schemas: Optional[TabbedSchemas] = None,
                 portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None):
        self.portal_env = portal_env
        self.portal_vapp = portal_vapp
        self._problems: List[str] = []
        super().__init__(tabbed_sheet_data=tabbed_sheet_data)
        self.schema_manager = SchemaManager(portal_env=portal_env, portal_vapp=portal_vapp)
        self.schemas = self.schema_manager.fetch_relevant_schemas(self.tab_names, schemas=schemas)
        self.type_hints_by_tab_name: Dict[str, OptionalTypeHints] = {
            tab_name: self.compile_type_hints(tab_name)
            for tab_name in self.tab_names
        }
        self.tabbed_item_table = TabbedItemTable(tabbed_sheet_data, portal_env=portal_env, portal_vapp=portal_vapp)
        # self.lookup_tables_by_tab_name: Dict[str, Dict[str, Dict]] = {
        #     tab_name: self.build_lookup_table_for_tab(tab_name, rows=rows)
        #     for tab_name, rows in tabbed_sheet_data.items()
        # }

    def note_problem(self, problem: str):
        self._problems.append(problem)

    def raise_any_pending_problems(self):
        problems = self._problems
        if problems:
            for problem in problems:
                PRINT(f"Problem: {problem}")
            raise Exception(there_are(problems, kind='problem while compiling hints', tense='past', show=False))

    def check_tabs(self):
        result = {tab_name: self.check_tab(tab_name)
                  for tab_name in self.tab_names}
        # At this point, doing the checking will have already raised certain errors, if those errors interfere
        # with continued checking, but some smaller problems may have been deferred until the end, so we have to
        # check for and raise an error for any such pending problems now.
        self.raise_any_pending_problems()
        return result

    def validate_ref(self, item_type, item_ref):
        if self.tabbed_item_table.contains_ref(item_type=item_type, item_ref=item_ref):
            return True
        # lookup_table = self.lookup_tables_by_tab_name.get(item_type)
        # if lookup_table:
        #     if item_ref in lookup_table:
        #         return True
        try:
            info = get_metadata(f"/{to_camel_case(item_type)}/{item_ref}")
            # Basically return True if there's a value at all,
            # but still check it's not an error message that didn't get raised.
            return isinstance(info, dict) and 'uuid' in info
        except Exception:
            return False

    def schema_exists(self, schema_name: str) -> bool:
        return self.schema_manager.schema_exists(schema_name)

    # def build_lookup_table_for_tab(self, tab_name: str, *, rows: List[Dict]) -> Dict[str, Dict]:
    #     # TODO: It might be enough to just return the keys as a set, not a full dict
    #     schema = get_schema(tab_name, portal_env=self.portal_env, portal_vapp=self.portal_vapp)
    #     possible_identifying_properties = set(schema.get("identifyingProperties") or []) | {'uuid'}
    #     identifying_properties = [prop
    #                               for prop in self.headers_by_tab_name[tab_name]
    #                               if prop in possible_identifying_properties]
    #     if not identifying_properties:
    #         # Maybe issue a warning here that we're going to lose
    #         empty_lookup_table: Dict[str, Dict] = {}
    #         return empty_lookup_table
    #     lookup_table: Dict[str, Dict] = {}
    #     for row in rows:
    #         for identifying_property in identifying_properties:
    #             value = row.get(identifying_property)
    #             if value is not '' and value is not None:
    #                 lookup_table[str(value)] = row
    #     return lookup_table

    def check_tab(self, tab_name: str):
        prototype = self.patch_prototypes_by_tab_name[tab_name]
        parsed_headers = self.parsed_headers_by_tab_name[tab_name]
        type_hints = self.type_hints_by_tab_name[tab_name]
        result = [self.check_row(row, tab_name=tab_name, row_number=row_number, prototype=prototype,
                                 parsed_headers=parsed_headers, type_hints=type_hints)
                  for row_number, row in enumerate(self.tabbed_sheet_data[tab_name])]
        return result

    def check_row(self, row: Dict, *, tab_name: str, row_number: int, prototype: Dict,
                  parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
        patch_item = copy.deepcopy(prototype)
        for column_number, column_value in enumerate(row.values()):
            parsed_value = ItemTools.parse_item_value(column_value)
            type_hint = type_hints[column_number]
            if type_hint:
                try:
                    parsed_value = type_hint.apply_hint(parsed_value)
                except ValidationProblem as e:
                    headers = self.headers_by_tab_name[tab_name]
                    column_name = headers[column_number]
                    self.note_problem(f"{tab_name}[{row_number}].{column_name}: {e}")
            ItemTools.set_path_value(patch_item, parsed_headers[column_number], parsed_value)
        return patch_item

    @classmethod
    def check(cls, tabbed_sheet_data: TabbedSheetData, schemas: Optional[TabbedSchemas] = None, **kwargs):
        checker = cls(tabbed_sheet_data, schemas=schemas, **kwargs)
        checked = checker.check_tabs()
        return checked

    class SheetState:

        def __init__(self, parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
            self.parsed_headers = parsed_headers
            self.type_hints = type_hints

    def compile_type_hints(self, tab_name: str) -> OptionalTypeHints:
        parsed_headers = self.parsed_headers_by_tab_name[tab_name]
        schema = self.schemas.get(tab_name)
        for required_header in self._schema_required_headers(schema):
            if required_header not in parsed_headers:
                self.note_problem("Missing required header")
        type_hints = [ItemTools.find_type_hint(parsed_header, schema, context=self) if schema else None
                      for parsed_header in parsed_headers]
        return type_hints

    @classmethod
    def _schema_required_headers(cls, schema):
        ignored(schema)
        return []  # TODO: Make this compute a list of required headers (in parsed header form)

    def create_tab_processor_state(self, tab_name: str) -> SheetState:
                                          # This will create state that allows us to efficiently assign values in the right place on each row
        return self.SheetState(parsed_headers=self.parsed_headers_by_tab_name[tab_name],
                               type_hints=self.type_hints_by_tab_name[tab_name])


check = TableChecker.check


def load_items(filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
               schemas: Optional[TabbedSchemas] = None, **kwargs):
    tabbed_rows = load_table_set(filename=filename, tab_name=tab_name, escaping=escaping, prefer_number=False,
                                 **kwargs)
    checked_items = check(tabbed_rows, schemas=schemas)
    return checked_items
