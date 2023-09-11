import contextlib
import copy
# import os
import uuid

from typing import Any, Dict, List, Optional, Type, Union
from .common import AnyJsonData  # , Regexp, CsvReader
from .env_utils import EnvUtils, public_env_name
from .ff_utils import get_schema
from .lang_utils import there_are
from .misc_utils import AbstractVirtualApp, ignored, PRINT
from .sheet_utils import (
    Header, Headers, ParsedHeader, ParsedHeaders, SheetCellValue, SheetRow,  TabbedSheetData,  # SheetData,
    prefer_number,
    LoadTableError,
    TableSetManagerRegistry, AbstractTableSetManager, BasicTableSetManager,
    CsvManager, TsvManager, XlsxManager,
    SimpleJsonInsertsManager, SimpleYamlInsertsManager, SimpleJsonLinesInsertsManager,
    TabbedJsonInsertsManager, TabbedYamlInsertsManager,
    InsertsDirectoryManager,
)
from .task_utils import pmap


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


class AbstractItemManager(AbstractTableSetManager):

    pass


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


ITEM_MANAGER_REGISTRY = TableSetManagerRegistry()


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


@ITEM_MANAGER_REGISTRY.register()
class XlsxItemManager(ItemManagerMixin, XlsxManager):
    """
    This layers item-style row processing functionality on an XLSX file.
    """
    pass


@ITEM_MANAGER_REGISTRY.register()
class SimpleJsonLinesInsertsItemManager(InsertsItemMixin, SimpleJsonLinesInsertsManager):
    pass


@ITEM_MANAGER_REGISTRY.register(regexp="^(.*/)?(|[^/]*[-_])inserts/?$")
class InsertsDirectoryItemManager(InsertsItemMixin, InsertsDirectoryManager):
    pass


@ITEM_MANAGER_REGISTRY.register()
class CsvItemManager(ItemManagerMixin, CsvManager):
    """
    This layers item-style row processing functionality on a CSV file.
    """
    pass


@ITEM_MANAGER_REGISTRY.register()
class TsvItemManager(ItemManagerMixin, TsvManager):
    """
    This layers item-style row processing functionality on a TSV file.
    """
    pass


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

load_items = ItemManager.load