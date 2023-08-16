import copy

from dcicutils.common import AnyJsonData
from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook.workbook import Workbook
from typing import Any, Dict, List, Optional, Union


Header = str
Headers = List[str]
ParsedHeader = List[Union[str, int]]
ParsedHeaders = List[ParsedHeader]
SheetCellValue = Union[int, float, str]


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
    def parse_value(cls, value: SheetCellValue) -> AnyJsonData:
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
                return [cls.parse_value(subvalue) for subvalue in value.split('|')]
            else:
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


class WorkbookManager:

    @classmethod
    def load_workbook(cls, filename: str):
        wb = cls(filename)
        return wb.load_content()

    def __init__(self, filename: str):
        self.filename: str = filename
        self.workbook: Optional[Workbook] = None
        self.headers_by_sheetname: Dict[str, List[str]] = {}
        self.content_by_sheetname: Dict[str, List[Any]] = {}

    def sheet_headers(self, sheetname: str) -> List[str]:
        return self.headers_by_sheetname[sheetname]

    def sheet_content(self, sheetname: str) -> List[Any]:
        return self.content_by_sheetname[sheetname]

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

    def _load_headers(self, sheet: Worksheet):
        headers: List[str] = [str(sheet.cell(row=1, column=col).value)
                              for col in self._all_cols(sheet)]
        self.headers_by_sheetname[sheet.title] = headers

    def _load_row(self, *, sheet: Worksheet, row: int):
        headers = self.sheet_headers(sheet.title)
        row_dict: Dict[str, Any] = {headers[col-1]: sheet.cell(row=row, column=col).value
                                    for col in self._all_cols(sheet)}
        return row_dict

    def load_content(self):
        workbook: Workbook = load_workbook(self.filename)
        self.workbook = workbook
        for sheetname in workbook.sheetnames:
            sheet: Worksheet = workbook[sheetname]
            self._load_headers(sheet)
            content = []
            for row in self._all_rows(sheet):
                row_dict = self._load_row(sheet=sheet, row=row)
                content.append(row_dict)
            self.content_by_sheetname[sheetname] = content
        return self.content_by_sheetname


class ItemManager(ItemTools, WorkbookManager):

    def __init__(self, filename: str):
        super().__init__(filename=filename)
        self.patch_prototypes_by_sheetname: Dict[str, Dict] = {}
        self.parsed_headers_by_sheetname: Dict[str, List[List[Union[int, str]]]] = {}

    def sheet_patch_prototype(self, sheetname: str) -> Dict:
        return self.patch_prototypes_by_sheetname[sheetname]

    def sheet_parsed_headers(self, sheetname: str) -> List[List[Union[int, str]]]:
        return self.parsed_headers_by_sheetname[sheetname]

    def _load_headers(self, sheet: Worksheet):
        super()._load_headers(sheet)
        self._compile_sheet_headers(sheet.title)

    def _compile_sheet_headers(self, sheetname: str):
        headers = self.headers_by_sheetname[sheetname]
        parsed_headers = self.parse_sheet_headers(headers)
        self.parsed_headers_by_sheetname[sheetname] = parsed_headers
        prototype = self.compute_patch_prototype(parsed_headers)
        self.patch_prototypes_by_sheetname[sheetname] = prototype

    def _load_row(self, *, sheet: Worksheet, row: int):
        parsed_headers = self.sheet_parsed_headers(sheet.title)
        patch_item = copy.deepcopy(self.sheet_patch_prototype(sheet.title))
        for col in self._all_cols(sheet):
            value = sheet.cell(row=row, column=col).value
            parsed_value = self.parse_value(value)
            self.set_path_value(patch_item, parsed_headers[col - 1], parsed_value)
        return patch_item
