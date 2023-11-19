import copy

from typing import Any, Dict, List, Optional, Tuple, Union
from .common import AnyJsonData
from .env_utils import EnvUtils, public_env_name
from .ff_utils import get_metadata
from .misc_utils import AbstractVirtualApp, ignored, ignorable, PRINT, remove_empty_properties, to_camel_case
from .sheet_utils import (
    LoadTableError, prefer_number, TabbedJsonSchemas,
    Header, Headers, TabbedHeaders, ParsedHeader, ParsedHeaders, TabbedParsedHeaders, SheetCellValue, TabbedSheetData,
    TableSetManagerRegistry, AbstractTableSetManager, InsertsManager, TableSetManager, load_table_set,
)
from .validation_utils import SchemaManager, validate_data_against_schemas


PatchPrototype = Dict
TabbedPatchPrototypes = Dict[str, PatchPrototype]
ARRAY_VALUE_DELIMITER = "|"


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
    def __init__(self, problems: Optional[dict] = None):
        self.problems = problems


class TypeHint:
    def __init__(self):
        self.is_array = False

    def apply_hint(self, value, src):
        return value

    def __str__(self):
        return f"<{self.__class__.__name__}>"

    def __repr__(self):
        return self.__str__()


class BoolHint(TypeHint):
    def __init__(self):
        super().__init__()

    # We could use other ways to do this, such as initial substring, but this is more likely to be right.
    # Then again, we might want to consder accepting athers like 'yes/no', 'y/n', 'on/off', '1/0'.
    TRUE_VALUES = ['true', 't']
    FALSE_VALUES = ['false', 'f']

    def apply_hint(self, value, src):
        if isinstance(value, str) and value:
            l_value = value.lower()
            if l_value in self.TRUE_VALUES:
                return True
            elif l_value in self.FALSE_VALUES:
                return False
        return super().apply_hint(value, src)


class NumHint(TypeHint):

    PREFERENCE_MAP = {'number': 'number', 'integer': 'integer'}

    def __init__(self, declared_type: Optional[str] = None):
        if declared_type is None:
            declared_type = 'number'
        self.preferred_type = self.PREFERENCE_MAP.get(declared_type)
        super().__init__()

    def apply_hint(self, value, src):
        if isinstance(value, str) and value:
            if self.preferred_type:
                return prefer_number(value, kind=self.preferred_type)
            else:
                return value
        return super().apply_hint(value, src)


class EnumHint(TypeHint):

    def __str__(self):
        return f"<EnumHint {','.join(f'{key}={val}' for key, val in self.value_map.items())}>"

    def __init__(self, value_map):
        self.value_map = value_map
        super().__init__()

    def apply_hint(self, value, src):
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
        return super().apply_hint(value, src)


class ArrayHint(TypeHint):
    def __init__(self):
        super().__init__()

    def apply_hint(self, value, src):
        if value is None or value == '':
            return []
        if isinstance(value, str):
            if not value:
                return []
            return [value.strip() for value in value.split(ARRAY_VALUE_DELIMITER)]
        return super().apply_hint(value, src)


class StringHint(TypeHint):
    def __init__(self):
        super().__init__()

    def apply_hint(self, value, src):
        return str(value).strip() if value is not None else ""


class RefHint(TypeHint):

    def __str__(self):
        return f"<RefHint {self.schema_name} context={self.context}>"

    def __init__(self, schema_name: str, required: bool, context: TypeHintContext):
        self.schema_name = schema_name
        self.context = context
        self.required = required
        super().__init__()

    def apply_hint(self, value, src):
        if self.is_array and isinstance(value, str):
            value = [value.strip() for value in value.split(ARRAY_VALUE_DELIMITER)] if value else []
        if self.is_array and isinstance(value, list):
            for item in value:
                self._apply_ref_hint(item, src)
        else:
            self._apply_ref_hint(value, src)
        return value

    def _apply_ref_hint(self, value, src):
        if not value and self.required:
            raise ValidationProblem(f"No required reference (linkTo) value for: {self.schema_name}"
                                    f"{f' from {src}' if src else ''}")
        if value and not self.context.validate_ref(item_type=self.schema_name, item_ref=value):
            raise ValidationProblem(f"Cannot resolve reference (linkTo) for: {self.schema_name}"
                                    f"{f'/{value}' if value else ''}{f' from {src}' if src else ''}")
        return value


class OptionalTypeHints:

    def __init__(self, positional_hints: Optional[List[Optional[TypeHint]]] = None,
                 positional_breadcrumbs: Optional[List[Union[List, Tuple]]] = None):
        self.other_hints: Dict[Any, TypeHint] = {}
        self.positional_hints: List[Optional[TypeHint]] = [] if positional_hints is None else positional_hints
        if positional_breadcrumbs and positional_hints:
            n = len(positional_breadcrumbs)
            if n != len(positional_hints):
                raise Exception("positional_hints and positional_breadcrumbs must have the same length.")
            for i in range(n):
                # for convenience, we accept this as a list or tuple, but it must be a tuple to be a key
                breadcrumbs = tuple(positional_breadcrumbs[i])
                if not isinstance(breadcrumbs, tuple):
                    raise Exception(f"Each of the positional breadcrumbs must be a tuple: {breadcrumbs}")
                hint = positional_hints[i]
                self.other_hints[breadcrumbs] = hint

    def __getitem__(self, key: Any) -> Optional[TypeHint]:
        """
        For enumerated positional information, we consult our initial type vector.
        For other situations, we do a general lookup of the hint in our lookup table.
        """
        if isinstance(key, int):
            hints = self.positional_hints
            if key < 0:
                raise ValueError(f"Negative hint positions are not allowed: {key}")
            elif key >= len(hints):
                return None
            else:
                return hints[key]
        elif isinstance(key, tuple):  # a parsed header (or schema breadcrumbs)
            return self.other_hints.get(key)
        else:
            raise ValueError(f"Key of unexpected type for OptionalTypeHints: {key}")

    def __setitem__(self, key: Any, value: TypeHint):
        if isinstance(key, int):
            raise ValueError(f"Cannot assign OptionalTypeHints by position after initial creation: {key!r}")
        elif key in self.other_hints:
            raise ValueError(f"Attempt to redefine OptionalTypeHint key {key!r}.")
        elif isinstance(key, tuple):
            self.other_hints[key] = value
        else:
            raise ValueError(f"Attempt to set an OptionalTypeHints key to other than a breadcrumbs tuple: {key!r}")


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
            parsed_header0 = parsed_header[0] if parsed_header else ""
            if isinstance(parsed_header0, int):
                raise LoadTableError(f"A header cannot begin with a numeric ref: {parsed_header0}")
            cls.assure_patch_prototype_shape(parent=prototype, keys=parsed_header)
        return prototype

    @classmethod
    def assure_patch_prototype_shape(cls, *, parent: Union[Dict, List], keys: ParsedHeader):
        key0 = None
        more_keys = None
        [key0, *more_keys] = keys if keys else [None]
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
    def parse_item_value(cls, value: SheetCellValue,
                         apply_heuristics: bool = False, split_pipe: bool = False) -> AnyJsonData:
        """
        Returns the item value unmodified, unless apply_heuristics=True is given,
        in which case heuristics ARE applied. This is intended to be used for spreadsheet
        values that look like non-strings and should perhaps be interepreted as such.

        This is a vestige of an older plan to have these things happen magically behind the scenes early in
        the process. Unfortunately, that was found to impede correct processing later, so now this is disabled
        by default. It may still be useful in some cases when dealing with data that has no schema, so the
        functionality is still here and must be explicitly requested.

        :param value: a value in a table (such as a spreadsheet)
        :param apply_heuristics: whether to apply heuristic coercions based on what the value looks like (default False)
        :param split_pipe: whether to apply the 'split pipe' heuristic, changing 'a|1' to ['a', 1], even if
           apply_heuristics=True was given (default False)
        """
        if not apply_heuristics:
            # In order to not interfere with schema-driven processing, we mostly default to
            # NOT applying heuristics. You have to ask for them explicitly if you want them.
            # -kmp 23-Oct-2023
            return value
        if isinstance(value, str):
            lvalue = value.lower()
            if lvalue == 'true':
                return True
            elif lvalue == 'false':
                return False
            # elif lvalue == 'null' or lvalue == '':
            elif lvalue == 'null':
                return None
            elif lvalue == '':
                return lvalue
            elif split_pipe and '|' in value:
                if value == '|':  # Use '|' for []
                    return []
                else:
                    if value.endswith("|"):  # Use 'foo|' for ['foo']
                        value = value[:-1]
                    return [cls.parse_item_value(subvalue, apply_heuristics=apply_heuristics, split_pipe=split_pipe)
                            for subvalue in value.split('|')]
            else:
                # Doug points out that the schema might not agree, might want a string representation of a number.
                # At this semantic layer, this might be a bad choice.
                return prefer_number(value)
        else:  # presumably a number (int or float)
            return value

    @classmethod
    def set_path_value(cls, datum: Union[List, Dict], path: ParsedHeader, value: Any, force: bool = False):
        # if (value is None or value == '') and not force:
        if value is None and not force:
            return
        [key, *more_path] = path if path else [None]
        if not more_path:
            datum[key] = value
        else:
            cls.set_path_value(datum[key], more_path, value)

    @classmethod
    def find_type_hint_for_subschema(cls, subschema: Any, required: bool = False,
                                     context: Optional[TypeHintContext] = None):
        if subschema is not None:
            t = subschema.get('type')
            if t == 'string':
                enum = subschema.get('enum')
                if enum:
                    mapping = {e.lower(): e for e in enum}
                    return EnumHint(mapping)
                link_to = subschema.get('linkTo')
                if link_to and context.schema_exists(link_to):
                    return RefHint(schema_name=link_to, required=required, context=context)
                return StringHint()
            elif t in ('integer', 'number'):
                return NumHint(declared_type=t)
            elif t == 'boolean':
                return BoolHint()
            elif t == 'array':
                array_type_hint = cls.find_type_hint_for_subschema(subschema.get("items"),
                                                                   required=required, context=context)
                if type(array_type_hint) is RefHint:
                    array_type_hint.is_array = True
                    return array_type_hint
                return ArrayHint()

    @classmethod
    def find_type_hint_for_parsed_header(cls, parsed_header: Optional[ParsedHeader], schema: Any,
                                         context: Optional[TypeHintContext] = None):
        def finder(subheader, subschema):
            if not parsed_header:
                return None
            else:
                [key1, *other_headers] = subheader
                if isinstance(key1, str) and isinstance(subschema, dict):
                    if subschema.get('type') == 'object':
                        subsubschema = subschema.get('properties', {}).get(key1)
                        if not other_headers:
                            required = key1 and subschema and key1 in subschema.get('required', [])
                            hint = cls.find_type_hint_for_subschema(subsubschema, required=required, context=context)
                            if hint:
                                return hint
                            else:
                                pass  # fall through to asking super()
                        else:
                            return finder(subheader=other_headers, subschema=subsubschema)

        return finder(subheader=parsed_header, subschema=schema)


ITEM_MANAGER_REGISTRY = TableSetManagerRegistry()


class InflatableTabbedDataManager:
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

    def __init__(self, tabbed_sheet_data: TabbedSheetData, apply_heuristics: bool = False):
        self.tabbed_sheet_data: TabbedSheetData = tabbed_sheet_data
        self.apply_heuristics = apply_heuristics
        self.headers_by_tab_name: TabbedHeaders = InsertsManager.extract_tabbed_headers(tabbed_sheet_data)
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

    def inflate_row(self, row: Dict, *, prototype: Dict, parsed_headers: ParsedHeaders):
        patch_item = copy.deepcopy(prototype)
        for column_number, column_value in enumerate(row.values()):
            parsed_value = ItemTools.parse_item_value(column_value, apply_heuristics=self.apply_heuristics)
            ItemTools.set_path_value(patch_item, parsed_headers[column_number], parsed_value)
        return patch_item

    @classmethod
    def inflate(cls, tabbed_sheet_data: TabbedSheetData, apply_heuristics: bool = False):
        inflater = cls(tabbed_sheet_data, apply_heuristics=apply_heuristics)
        inflated = inflater.inflate_tabs()
        return inflated


inflate = InflatableTabbedDataManager.inflate


def load_table_structures(filename: str, *, apply_heuristics: bool = True,
                          tab_name: Optional[str] = None, escaping: Optional[bool] = None, **kwargs):
    """This differs from load_table_set only in that it inflates the content. It does not apply schemas."""
    tabbed_rows = load_table_set(filename=filename, tab_name=tab_name, escaping=escaping, **kwargs)
    tabbed_structures = inflate(tabbed_rows, apply_heuristics=apply_heuristics)
    return tabbed_structures


class TableChecker(InflatableTabbedDataManager, TypeHintContext):

    def __init__(self, tabbed_sheet_data: TabbedSheetData, *, flattened: bool,
                 override_schemas: Optional[TabbedJsonSchemas] = None,
                 portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None,
                 apply_heuristics: bool = False):

        self.flattened = flattened
        # if not flattened:
        #     # TODO: Need to implement something that depends on this flattened attribute.
        #     # Also, it's possible that we can default this once we see if the new strategy is general-purpose,
        #     # rather than it being a required argument. But for now let's require it be passed.
        #     # -kmp 25-Oct-2023
        #     raise ValueError("Only flattened=True is supported by TableChecker for now.")

        if portal_env is None and portal_vapp is None:
            portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)
        # InflatableTabbedDataManager supplies:
        #   self.tabbed_sheet_data: TabbedSheetData =
        #   self.headers_by_tab_name: TabbedHeaders =
        #   self.parsed_headers_by_tab_name: TabbedParsedHeaders =
        #   self.patch_prototypes_by_tab_name: TabbedPatchPrototypes =
        self._problems: List[str] = []
        super().__init__(tabbed_sheet_data=tabbed_sheet_data, apply_heuristics=apply_heuristics)
        self.portal_env = portal_env
        self.portal_vapp = portal_vapp
        self.schema_manager: SchemaManager = SchemaManager(portal_env=portal_env, portal_vapp=portal_vapp,
                                                           override_schemas=override_schemas)
        schema_names_to_fetch = [key for key, value in tabbed_sheet_data.items() if value]
        self.schemas = self.schema_manager.fetch_relevant_schemas(schema_names_to_fetch)
        self.lookup_tables_by_tab_name: Dict[str, Dict[str, Dict]] = {
            tab_name: self.build_lookup_table_for_tab(tab_name, rows=rows)
            for tab_name, rows in tabbed_sheet_data.items()
        }
        self.type_hints_by_tab_name: Dict[str, OptionalTypeHints] = {
            tab_name: self.compile_type_hints(tab_name)
            for tab_name in self.tab_names
        }

    def schema_for_tab(self, tab_name: str) -> dict:
        # Once our class is initialized, every tab should have a schema, even if just {}
        schema = self.schemas.get(tab_name)
        if schema is None:
            raise ValueError(f"No schema was given or fetched for tab {tab_name!r}.")
        return schema

    def note_problem(self, problem: str):
        self._problems.append(problem)

    def build_lookup_table_for_tab(self, tab_name: str, *, rows: List[Dict]) -> Dict[str, Dict]:
        if not self.schema_manager or not rows:
            return []
        identifying_properties = self.schema_manager.identifying_properties(schema_name=tab_name)
        if not identifying_properties:
            # Maybe issue a warning here that we're going to lose
            empty_lookup_table: Dict[str, Dict] = {}
            return empty_lookup_table
        lookup_table: Dict[str, Dict] = {}
        for row in rows:
            for identifying_property in identifying_properties:
                value = row.get(identifying_property)
                if value != '' and value is not None:
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
            if isinstance(item_ref, list):
                found = True
                for item in item_ref:
                    if not lookup_table.get(item):
                        found = False
                        break
                return True if found else None
            else:
                return lookup_table.get(item_ref) or None
        else:  # Apparently some stray type not in our tables
            return None

    def raise_any_pending_problems(self):
        problems = self._problems
        if problems:
            for problem in problems:
                PRINT(f"Problem: {problem}")
            raise ValidationProblem(problems)
            # raise Exception(there_are(problems, kind='problem while compiling hints', tense='past', show=False))

    def check_tabs(self):
        result = {tab_name: self.check_tab(tab_name)
                  for tab_name in self.tab_names}
        # At this point, doing the checking will have already raised certain errors, if those errors interfere
        # with continued checking, but some smaller problems may have been deferred until the end, so we have to
        # check for and raise an error for any such pending problems now.
        self.raise_any_pending_problems()
        return result

    def validate_ref(self, item_type, item_ref):
        if self.contains_ref(item_type=item_type, item_ref=item_ref):
            return True
        try:
            # TODO: This probably needs a cache
            if isinstance(item_ref, list):
                found = True
                for item in item_ref:
                    info = get_metadata(f"/{to_camel_case(item_type)}/{item}",
                                        ff_env=self.portal_env, vapp=self.portal_vapp)
                    if not isinstance(info, dict) or 'uuid' not in info:
                        found = False
                        break
                return found
            else:
                info = get_metadata(f"/{to_camel_case(item_type)}/{item_ref}",
                                    ff_env=self.portal_env, vapp=self.portal_vapp)
            # Basically return True if there's a value at all,
            # but still check it's not an error message that didn't get raised.
            return isinstance(info, dict) and 'uuid' in info
        except Exception:
            return False

    def schema_exists(self, schema_name: str) -> bool:
        return self.schema_manager.schema_exists(schema_name) if self.schema_manager else False

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
        if self.flattened:
            return self.check_flattened_row(row=row, tab_name=tab_name, row_number=row_number, prototype=prototype,
                                            parsed_headers=parsed_headers, type_hints=type_hints)
        else:
            return self.check_inflated_row(row=row, tab_name=tab_name, row_number=row_number, prototype=prototype,
                                           parsed_headers=parsed_headers, type_hints=type_hints)

    def check_inflated_row(self, row: Dict, *, tab_name: str, row_number: int, prototype: Dict,
                           parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
        ignorable(self, tab_name, row_number, prototype, parsed_headers, type_hints)  #
        # TODO: Make this work...
        # def traverse(item, *, subschema, breadcrumbs):
        #     if isinstance(item, list):
        #         # check schema here to make sure it's supposed to be a list before proceeding
        #         for i, elem in enumerate(item):
        #             traverse(item, subschema=..., breadcrumbs=(*breadcrumbs, i))
        #     elif isinstance(item, dict):
        #         # check schema here to make sure it's supposed to be a dict before proceeding
        #         for k, v in item.items():
        #             traverse(v, subschema=..., breadcrumbs=(*breadcrumbs, k))
        #     else:
        #         # look up hint. if there's not a hint for these breadcrumbs, make one
        #         # apply the hint for side-effect, to get an error if we have a bad value
        #         pass
        # schema = self.schemas[tab_name]
        # if schema:
        #     traverse(row, subschema=schema, breadcrumbs=())  # for side-effect
        return row

    def check_flattened_row(self, row: Dict, *, tab_name: str, row_number: int, prototype: Dict,
                            parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
        patch_item = copy.deepcopy(prototype)
        for column_number, column_value in enumerate(row.values()):
            parsed_value = ItemTools.parse_item_value(column_value, apply_heuristics=self.apply_heuristics)
            column_name = (list(row.keys())[column_number] or "") if len(row) > column_number else ""
            if column_name.endswith("#"):
                if isinstance(parsed_value, str):
                    parsed_value = [value.strip() for value in parsed_value.split(ARRAY_VALUE_DELIMITER) if value]
            type_hint = type_hints[column_number]
            if type_hint:
                try:
                    src = f"{tab_name}{f'.{column_name}' if column_name else ''}"
                    parsed_value = type_hint.apply_hint(parsed_value, src)
                except ValidationProblem as e:
                    headers = self.headers_by_tab_name[tab_name]
                    column_name = headers[column_number]
                    self.note_problem(f"{tab_name}[{row_number}].{column_name}: {e}")
            ItemTools.set_path_value(patch_item, parsed_headers[column_number], parsed_value)
        return patch_item

    @classmethod
    def check(cls, tabbed_sheet_data: TabbedSheetData, *,
              flattened: bool,
              override_schemas: Optional[TabbedJsonSchemas] = None,
              apply_heuristics: bool = False,
              portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None):
        checker = cls(tabbed_sheet_data, flattened=flattened,
                      override_schemas=override_schemas, apply_heuristics=apply_heuristics,
                      portal_env=portal_env, portal_vapp=portal_vapp)
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
        positional_type_hints = [(ItemTools.find_type_hint_for_parsed_header(parsed_header, schema, context=self)
                                  if schema
                                  else None)
                                 for parsed_header in parsed_headers]
        type_hints = OptionalTypeHints(positional_type_hints, positional_breadcrumbs=parsed_headers)
        return type_hints

    @classmethod
    def _schema_required_headers(cls, schema):
        ignored(schema)
        return []  # TODO: Make this compute a list of required headers (in parsed header form)

    def create_tab_processor_state(self, tab_name: str) -> SheetState:
        # This will create state that allows us to efficiently assign values in the right place on each row
        return self.SheetState(parsed_headers=self.parsed_headers_by_tab_name[tab_name],
                               type_hints=self.type_hints_by_tab_name[tab_name])


# check = TableChecker.check


def load_items(filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
               override_schemas: Optional[TabbedJsonSchemas] = None, apply_heuristics: bool = False,
               portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None,
               # TODO: validate= is presently False (i.e., disabled) by default while being debugged,
               #       but for production use maybe should not be? -kmp 25-Oct-2023
               validate: bool = False,
               retain_empty_properties: bool = False,
               sheet_order: Optional[List[str]] = None,
               **kwargs):
    annotated_data = TableSetManager.load_annotated(filename=filename, tab_name=tab_name, escaping=escaping,
                                                    prefer_number=False, sheet_order=sheet_order, **kwargs)
    tabbed_rows = annotated_data['content']
    flattened = annotated_data['flattened']
    if flattened:
        checked_items = TableChecker.check(tabbed_rows, flattened=flattened,
                                           override_schemas=override_schemas,
                                           portal_env=portal_env, portal_vapp=portal_vapp,
                                           apply_heuristics=apply_heuristics)
    else:
        # No fancy checking for things like .json, etc. for now. Only check things that came from
        # spreadsheet-like data, where structural datatypes are forced into strings.
        checked_items = tabbed_rows
    if not retain_empty_properties:
        remove_empty_properties(checked_items)
    if validate:
        problems = validate_data_against_schemas(checked_items, portal_env=portal_env, portal_vapp=portal_vapp,
                                                 override_schemas=override_schemas)
        return checked_items, problems
    return checked_items
