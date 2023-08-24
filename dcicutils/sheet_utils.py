import chardet
import copy
import csv
import io
import openpyxl
import uuid

from dcicutils.common import AnyJsonData
from dcicutils.lang_utils import conjoined_list, disjoined_list, maybe_pluralize
from dcicutils.misc_utils import ignored
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook.workbook import Workbook
from tempfile import TemporaryFile
from typing import Any, Dict, Iterable, List, Optional, Union


Header = str
Headers = List[str]
ParsedHeader = List[Union[str, int]]
ParsedHeaders = List[ParsedHeader]
SheetCellValue = Union[int, float, str]
SheetRow = List[SheetCellValue]
CsvReader = type(csv.reader(TemporaryFile()))


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


def open_text_input_file_respecting_byte_order_mark(filename):
    """
    Opens a file for text input, respecting a byte-order mark (BOM).
    """
    with io.open(filename, 'rb') as fp:
        leading_bytes = fp.read(4 * 8)  # 4 bytes is all we need
        bom_info = chardet.detect(leading_bytes)
        detected_encoding = bom_info and bom_info.get('encoding')  # tread lightly

    return io.open(filename, 'r', encoding=detected_encoding)


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
    Note that at this level of abstraction, we take no position on what form of representation is used
    for the rows, as long as it is JSON data of some kind. It might be
         {"col1": "val1", "col2": "val2", ...}
    or it might be something more structured like
         {"something": "val1", {"something_else": ["val2"]}}
    Additionally, the values stored might be altered as well. In particular, the most likely alteration
    is to turn "123" to 123 or "" to None, though the specifics of whether and how such transformations
    happen is not constrained by this class.
    """

    def __init__(self, **kwargs):
        unwanted_kwargs(context=self.__class__.__name__, kwargs=kwargs)

    # TODO: Consider whether this should be an abstractmethod (but first see detailed design note at top of class.)
    @classmethod
    def load(cls, filename: str) -> Dict[str, List[AnyJsonData]]:
        """
        Reads a filename and returns a dictionary that maps sheet names to rows of dictionary data.
        For more information, see documentation of AbstractTableSetManager.
        """
        raise NotImplementedError(f".load(...) is not implemented for {cls.__name__}.")  # noQA


class BasicTableSetManager(AbstractTableSetManager):
    """
    A BasicTableManager provides some structure that most kinds of parsers will need.
    In particular, everything will likely need some way of storing headers and some way of storing content
    of each sheet. Even a csv file, which doesn't have multiple tabs can be seen as the degenerate case
    of this where there's only one set of headers and only one block of content.
    """

    def __init__(self, filename: str, **kwargs):
        super().__init__(**kwargs)
        self.filename: str = filename
        self.headers_by_tabname: Dict[str, Headers] = {}
        self.content_by_tabname: Dict[str, List[AnyJsonData]] = {}
        self.reader_agent: Any = self._get_reader_agent()

    def tab_headers(self, tabname: str) -> Headers:
        return self.headers_by_tabname[tabname]

    def tab_content(self, tabname: str) -> List[AnyJsonData]:
        return self.content_by_tabname[tabname]

    @classmethod
    def _create_tab_processor_state(cls, tabname: str) -> Any:
        """
        This method provides for the possibility that some parsers will want auxiliary state,
        (such as parsed headers or a line count or a table of temporary names for objects to cross-link
        or some other such feature) that it carries with it as it moves from line to line parsing things.
        Subclasses might therefore want to make this do something more interesting.
        """
        ignored(tabname)  # subclasses might need this, but we don't
        return None

    def _get_reader_agent(self) -> Any:
        """This function is responsible for opening the workbook and returning a workbook object."""
        raise NotImplementedError(f"._get_reader_agent() is not implemented for {self.__class__.__name__}.")  # noQA

    def load_content(self) -> Any:
        raise NotImplementedError(f".load_content() is not implemented for {self.__class__.__name__}.")  # noQA


class TableSetManager(BasicTableSetManager):

    ALLOWED_FILE_EXTENSIONS = None

    @classmethod
    def load(cls, filename: str) -> AnyJsonData:
        if cls.ALLOWED_FILE_EXTENSIONS:
            if not any(filename.lower().endswith(suffix) for suffix in cls.ALLOWED_FILE_EXTENSIONS):
                raise LoadArgumentsError(f"The TableSetManager subclass {cls.__name__} expects only"
                                         f" {disjoined_list(cls.ALLOWED_FILE_EXTENSIONS)} filenames: {filename}")

        table_set_manager: TableSetManager = cls(filename)
        return table_set_manager.load_content()

    def __init__(self, filename: str, **kwargs):
        super().__init__(filename=filename, **kwargs)

    @property
    def tabnames(self) -> List[str]:
        raise NotImplementedError(f".tabnames is not implemented for {self.__class__.__name__}..")  # noQA

    def _raw_row_generator_for_tabname(self, tabname: str) -> Iterable[SheetRow]:
        """
        Given a tabname and a state (returned by _sheet_loader_state), return a generator for a set of row values.
        """
        raise NotImplementedError(f"._rows_for_tabname(...) is not implemented for {self.__class__.__name__}.")  # noQA

    def _process_row(self, tabname: str, state: Any, row: List[SheetCellValue]) -> AnyJsonData:
        """
        This needs to take a state and whatever represents a row and
        must return a list of objects representing column values.
        What constitutes a processed up to the class, but other than that the result must be a JSON dictionary.
        """
        raise NotImplementedError(f"._process_row(...) is not implemented for {self.__class__.__name__}.")  # noQA

    def load_content(self) -> AnyJsonData:
        for tabname in self.tabnames:
            sheet_content = []
            state = self._create_tab_processor_state(tabname)
            for row_data in self._raw_row_generator_for_tabname(tabname):
                processed_row_data: AnyJsonData = self._process_row(tabname, state, row_data)
                sheet_content.append(processed_row_data)
            self.content_by_tabname[tabname] = sheet_content
        return self.content_by_tabname

    @classmethod
    def parse_cell_value(cls, value: SheetCellValue) -> AnyJsonData:
        return prefer_number(value)


class XlsxManager(TableSetManager):
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
    def tabnames(self) -> List[str]:
        return self.reader_agent.sheetnames

    def _get_reader_agent(self) -> Workbook:
        return openpyxl.load_workbook(self.filename)

    def _raw_row_generator_for_tabname(self, tabname: str) -> Iterable[SheetRow]:
        sheet = self.reader_agent[tabname]
        return (self._get_raw_row_content_tuple(sheet, row)
                for row in self._all_rows(sheet))

    def _get_raw_row_content_tuple(self, sheet: Worksheet, row: int) -> SheetRow:
        return [sheet.cell(row=row, column=col).value
                for col in self._all_cols(sheet)]

    def _create_tab_processor_state(self, tabname: str) -> Headers:
        sheet = self.reader_agent[tabname]
        headers: Headers = [str(sheet.cell(row=1, column=col).value)
                            for col in self._all_cols(sheet)]
        self.headers_by_tabname[sheet.title] = headers
        return headers

    def _process_row(self, tabname: str, headers: Headers, row_data: SheetRow) -> AnyJsonData:
        ignored(tabname)
        return {headers[i]: self.parse_cell_value(row_datum)
                for i, row_datum in enumerate(row_data)}


class ItemManagerMixin(BasicTableSetManager):
    """
    This can add functionality to a reader such as an XlsxManager or a CsvManager in order to make its rows
    get handled like Items instead of just flat table rows.
    """

    def __init__(self, filename: str, schemas=None, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.patch_prototypes_by_tabname: Dict[str, Dict] = {}
        self.parsed_headers_by_tabname: Dict[str, ParsedHeaders] = {}
        self.type_hints_by_tabname: Dict[str, OptionalTypeHints] = {}
        self.schemas = schemas or {}
        self._instaguid_context_table: Dict[str, str] = {}

    def sheet_patch_prototype(self, tabname: str) -> Dict:
        return self.patch_prototypes_by_tabname[tabname]

    def sheet_parsed_headers(self, tabname: str) -> ParsedHeaders:
        return self.parsed_headers_by_tabname[tabname]

    def sheet_type_hints(self, tabname: str) -> OptionalTypeHints:
        return self.type_hints_by_tabname[tabname]

    class SheetState:

        def __init__(self, parsed_headers: ParsedHeaders, type_hints: OptionalTypeHints):
            self.parsed_headers = parsed_headers
            self.type_hints = type_hints

    def _compile_type_hints(self, tabname: str):
        parsed_headers = self.sheet_parsed_headers(tabname)
        schema = self.schemas.get(tabname)
        type_hints = [ItemTools.find_type_hint(parsed_header, schema) if schema else None
                      for parsed_header in parsed_headers]
        self.type_hints_by_tabname[tabname] = type_hints

    def _compile_sheet_headers(self, tabname: str):
        headers = self.headers_by_tabname[tabname]
        parsed_headers = ItemTools.parse_sheet_headers(headers)
        self.parsed_headers_by_tabname[tabname] = parsed_headers
        prototype = ItemTools.compute_patch_prototype(parsed_headers)
        self.patch_prototypes_by_tabname[tabname] = prototype

    def _create_tab_processor_state(self, tabname: str) -> SheetState:
        super()._create_tab_processor_state(tabname)
        # This will create state that allows us to efficiently assign values in the right place on each row
        # by setting up a prototype we can copy and then drop values into.
        self._compile_sheet_headers(tabname)
        self._compile_type_hints(tabname)
        return self.SheetState(parsed_headers=self.sheet_parsed_headers(tabname),
                               type_hints=self.sheet_type_hints(tabname))

    def _process_row(self, tabname: str, state: SheetState, row_data: SheetRow) -> AnyJsonData:
        parsed_headers = state.parsed_headers
        type_hints = state.type_hints
        patch_item = copy.deepcopy(self.sheet_patch_prototype(tabname))
        for i, value in enumerate(row_data):
            parsed_value = self.parse_cell_value(value)
            type_hint = type_hints[i]
            if type_hint:
                parsed_value = type_hint.apply_hint(parsed_value)
            ItemTools.set_path_value(patch_item, parsed_headers[i], parsed_value)
        return patch_item

    def parse_cell_value(self, value: SheetCellValue) -> AnyJsonData:
        return ItemTools.parse_item_value(value, context=self._instaguid_context_table)


class XlsxItemManager(ItemManagerMixin, XlsxManager):
    """
    This layers item-style row processing functionality on an XLSX file.
    """
    pass


class CsvManager(TableSetManager):
    """
    This implements the mechanism to get a series of rows out of the sheet in a csv file,
    returning a result that still looks like there could have been multiple tabs.
    """

    ALLOWED_FILE_EXTENSIONS = ['.csv']

    DEFAULT_TAB_NAME = 'Sheet1'

    def __init__(self, filename: str, tab_name: Optional[str] = None, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.tab_name = tab_name or self.DEFAULT_TAB_NAME

    @property
    def tabnames(self) -> List[str]:
        return [self.tab_name]

    def _get_reader_agent(self) -> CsvReader:
        return self._get_csv_reader(self.filename)

    @classmethod
    def _get_csv_reader(cls, filename) -> CsvReader:
        return csv.reader(open_text_input_file_respecting_byte_order_mark(filename))

    PAD_TRAILING_TABS = True

    def _raw_row_generator_for_tabname(self, tabname: str) -> Iterable[SheetRow]:
        headers = self.tab_headers(tabname)
        n_headers = len(headers)
        for row_data in self.reader_agent:
            n_cols = len(row_data)
            if self.PAD_TRAILING_TABS and n_cols < n_headers:
                row_data = row_data + [''] * (n_headers - n_cols)
            yield row_data

    def _create_tab_processor_state(self, tabname: str) -> Headers:
        headers: Optional[Headers] = self.headers_by_tabname.get(tabname)
        if headers is None:
            self.headers_by_tabname[tabname] = headers = self.reader_agent.__next__()
        return headers

    def _process_row(self, tabname: str, headers: Headers, row_data: SheetRow) -> AnyJsonData:
        ignored(tabname)
        return {headers[i]: self.parse_cell_value(row_datum)
                for i, row_datum in enumerate(row_data)}


class CsvItemManager(ItemManagerMixin, CsvManager):
    """
    This layers item-style row processing functionality on a CSV file.
    """
    pass


class TsvManager(CsvManager):
    """
    TSV files are just CSV files with tabs instead of commas as separators.
    (We do not presently handle any escaping of strange characters. May need to add handling for backslash escaping.)
    """
    ALLOWED_FILE_EXTENSIONS = ['.tsv', '.tsv.txt']

    def __init__(self, filename: str, escaping: Optional[bool] = None, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.escaping: bool = escaping or False

    @classmethod
    def _get_csv_reader(cls, filename) -> CsvReader:
        return csv.reader(open_text_input_file_respecting_byte_order_mark(filename), delimiter='\t')

    def parse_cell_value(self, value: SheetCellValue) -> AnyJsonData:
        if self.escaping and isinstance(value, str) and '\\' in value:
            value = self.expand_escape_sequences(value)
        return super().parse_cell_value(value)

    @classmethod
    def expand_escape_sequences(cls, text: str) -> str:
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
    def create_implementation_manager(cls, filename: str, **kwargs) -> BasicTableSetManager:
        if filename.endswith(".xlsx"):
            reader_agent = XlsxItemManager(filename, **kwargs)
        elif filename.endswith(".csv"):
            tab_name = kwargs.pop('tab_name', None)
            reader_agent = CsvItemManager(filename, tab_name=tab_name, **kwargs)
        elif filename.endswith(".tsv"):
            escaping = kwargs.pop('escaping', None)
            tab_name = kwargs.pop('tab_name', None)
            reader_agent = TsvItemManager(filename, escaping=escaping, tab_name=tab_name, **kwargs)
        else:
            raise LoadArgumentsError(f"Unknown file type: {filename}")
        return reader_agent

    @classmethod
    def load(cls, filename: str, tab_name: Optional[str] = None, escaping: Optional[bool] = None,
             schemas: Optional[Dict] = None) -> AnyJsonData:
        manager = cls.create_implementation_manager(filename, tab_name=tab_name, escaping=escaping, schemas=schemas)
        return manager.load_content()


load_items = ItemManager.load
