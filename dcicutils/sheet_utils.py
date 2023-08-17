import chardet
import copy
import csv
import io
import openpyxl

from dcicutils.common import AnyJsonData
from dcicutils.misc_utils import ignored
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook.workbook import Workbook
from tempfile import TemporaryFile
from typing import Any, Dict, Iterable, List, Union


Header = str
Headers = List[str]
ParsedHeader = List[Union[str, int]]
ParsedHeaders = List[ParsedHeader]
SheetCellValue = Union[int, float, str]
SheetRow = List[SheetCellValue]
CsvReader = type(csv.reader(TemporaryFile()))


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
                raise ValueError(f"A header cannot begin with a numeric ref: {parsed_header0}")
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
                raise Exception("Numeric items must occur sequentially.")
        elif isinstance(key0, str):
            if key0 not in parent:
                parent[key0] = placeholder
        if key1 is not None:
            cls.assure_patch_prototype_shape(parent=parent[key0], keys=more_keys)
        return parent

    @classmethod
    def parse_item_value(cls, value: SheetCellValue) -> AnyJsonData:
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
                return [cls.parse_item_value(subvalue) for subvalue in value.split('|')]
            else:
                return prefer_number(value)
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
        if kwargs:
            raise ValueError(f"Got unexpected keywords: {kwargs}")

    # TODO: Consider whether this should be an abstractmethod (but first see detailed design note at top of class.)
    @classmethod
    def load(cls, filename: str) -> Dict[str, List[AnyJsonData]]:
        """
        Reads a filename and returns a dictionary that maps sheet names to rows of dictionary data.
        For more information, see documentation of AbstractTableSetManager.
        """
        raise NotImplementedError(f".load(...) is not implemented for {cls.__name__}.")


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
        self.headers_by_tabname: Dict[str, List[str]] = {}
        self.content_by_tabname: Dict[str, List[AnyJsonData]] = {}
        self.reader_agent: Any = self._get_reader_agent()

    def tab_headers(self, tabname: str) -> List[str]:
        return self.headers_by_tabname[tabname]

    def tab_content(self, tabname: str) -> List[AnyJsonData]:
        return self.content_by_tabname[tabname]

    def _create_tab_processor_state(self, tabname: str) -> Any:
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
        raise NotImplementedError(f"._get_reader_agent() is not implemented for {self.__class__.__name__}.")

    def load_content(self) -> Any:
        raise NotImplementedError(f".load_content() is not implemented for {self.__class__.__name__}.")


class TableSetManager(BasicTableSetManager):

    @classmethod
    def load(cls, filename: str) -> AnyJsonData:
        table_set_manager: TableSetManager = cls(filename)
        return table_set_manager.load_content()

    def __init__(self, filename: str):
        super().__init__(filename=filename)

    @property
    def tabnames(self) -> List[str]:
        raise NotImplementedError(f".tabnames is not implemented for {self.__class__.__name__}..")

    def _raw_row_generator_for_tabname(self, tabname: str) -> Iterable[SheetRow]:
        """
        Given a tabname and a state (returned by _sheet_loader_state), return a generator for a set of row values.
        """
        raise NotImplementedError(f"._rows_for_tabname(...) is not implemented for {self.__class__.__name__}.")

    def _process_row(self, tabname: str, state: Any, row: List[SheetCellValue]) -> AnyJsonData:
        """
        This needs to take a state and whatever represents a row and
        must return a list of objects representing column values.
        What constitutes a processed up to the class, but other than that the result must be a JSON dictionary.
        """
        raise NotImplementedError(f"._process_row(...) is not implemented for {self.__class__.__name__}.")

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
        headers: List[str] = [str(sheet.cell(row=1, column=col).value)
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

    def __init__(self, filename: str, **kwargs):
        super().__init__(filename=filename, **kwargs)
        self.patch_prototypes_by_tabname: Dict[str, Dict] = {}
        self.parsed_headers_by_tabname: Dict[str, List[List[Union[int, str]]]] = {}

    def sheet_patch_prototype(self, tabname: str) -> Dict:
        return self.patch_prototypes_by_tabname[tabname]

    def sheet_parsed_headers(self, tabname: str) -> List[List[Union[int, str]]]:
        return self.parsed_headers_by_tabname[tabname]

    def _create_tab_processor_state(self, tabname: str) -> ParsedHeaders:
        super()._create_tab_processor_state(tabname)
        # This will create state that allows us to efficiently assign values in the right place on each row
        # by setting up a prototype we can copy and then drop values into.
        self._compile_sheet_headers(tabname)
        return self.sheet_parsed_headers(tabname)

    def _compile_sheet_headers(self, tabname: str):
        headers = self.headers_by_tabname[tabname]
        parsed_headers = ItemTools.parse_sheet_headers(headers)
        self.parsed_headers_by_tabname[tabname] = parsed_headers
        prototype = ItemTools.compute_patch_prototype(parsed_headers)
        self.patch_prototypes_by_tabname[tabname] = prototype

    def _process_row(self, tabname: str, parsed_headers: ParsedHeaders, row_data: SheetRow) -> AnyJsonData:
        patch_item = copy.deepcopy(self.sheet_patch_prototype(tabname))
        for i, value in enumerate(row_data):
            parsed_value = self.parse_cell_value(value)
            ItemTools.set_path_value(patch_item, parsed_headers[i], parsed_value)
        return patch_item

    @classmethod
    def parse_cell_value(cls, value: SheetCellValue) -> AnyJsonData:
        return ItemTools.parse_item_value(value)


class ItemXlsxManager(ItemManagerMixin, XlsxManager):
    """
    This layers item-style row processing functionality on an XLSX file.
    """
    pass


class CsvManager(TableSetManager):
    """
    This implements the mechanism to get a series of rows out of the sheet in a csv file,
    returning a result that still looks like there could have been multiple tabs.
    """

    DEFAULT_TAB_NAME = 'Sheet1'

    def __init__(self, filename: str, tab_name=None):
        super().__init__(filename=filename)
        self.tab_name = tab_name or self.DEFAULT_TAB_NAME

    @property
    def tabnames(self) -> List[str]:
        return [self.tab_name]

    def _get_reader_agent(self) -> CsvReader:
        return self._get_csv_reader(self.filename)

    @classmethod
    def _get_csv_reader(cls, filename) -> CsvReader:
        return csv.reader(open_text_input_file_respecting_byte_order_mark(filename))

    def _raw_row_generator_for_tabname(self, tabname: str) -> Iterable[SheetRow]:
        return self.reader_agent

    def _create_tab_processor_state(self, tabname: str) -> Headers:
        headers: Headers = self.headers_by_tabname.get(tabname)
        if headers is None:
            self.headers_by_tabname[tabname] = headers = self.reader_agent.__next__()
            print(f"Headers={headers}")
        return headers

    def _process_row(self, tabname: str, headers: Headers, row_data: SheetRow) -> AnyJsonData:
        ignored(tabname)
        return {headers[i]: self.parse_cell_value(row_datum)
                for i, row_datum in enumerate(row_data)}


class ItemCsvManager(ItemManagerMixin, CsvManager):
    """
    This layers item-style row processing functionality on a CSV file.
    """
    pass


class TsvManager(CsvManager):
    """
    TSV files are just CSV files with tabs instead of commas as separators.
    (We do not presently handle any escaping of strange characters. May need to add handling for backslash escaping.)
    """
    @classmethod
    def _get_csv_reader(cls, filename) -> CsvReader:
        return csv.reader(open_text_input_file_respecting_byte_order_mark(filename), delimiter='\t')


class ItemTsvManager(ItemManagerMixin, TsvManager):
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
    def create_implementation_manager(cls, filename: str, tab_name=None) -> BasicTableSetManager:
        if filename.endswith(".xlsx"):
            if tab_name is not None:
                raise ValueError(f".xlsx files don't need tab_name={tab_name!r}")
            reader_agent = ItemXlsxManager(filename)
        elif filename.endswith(".csv"):
            reader_agent = ItemCsvManager(filename, tab_name=tab_name)
        elif filename.endswith(".tsv"):
            reader_agent = ItemTsvManager(filename, tab_name=tab_name)
        else:
            raise ValueError(f"Unknown file type: {filename}")
        return reader_agent

    @classmethod
    def load(cls, filename: str, tab_name=None) -> AnyJsonData:
        manager = cls.create_implementation_manager(filename, tab_name=tab_name)
        return manager.load_content()


load_items = ItemManager.load
