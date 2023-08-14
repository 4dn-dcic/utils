import copy

from openpyxl import load_workbook
from openpyxl.worksheet.worksheet import Worksheet
from openpyxl.workbook.workbook import Workbook
from typing import Any, Dict, List, Optional, Union


class WorkbookManager:

    @classmethod
    def load_workbook(cls, filename: str):
        wb = cls(filename)
        return wb.load_content()

    def __init__(self, filename: str):
        self.filename: str = filename
        self.workbook: Optional[Workbook] = None
        self.headers_by_sheetname: Dict[List[str]] = {}
        self.content_by_sheetname: Dict[List[Any]] = {}

    def sheet_headers(self, sheet: Worksheet) -> List[str]:
        return self.headers_by_sheetname[sheet.title]

    def sheet_content(self, sheet: Worksheet) -> List[Any]:
        return self.content_by_sheetname[sheet.title]

    @classmethod
    def all_rows(cls, sheet: Worksheet):
        row_max = sheet.max_row
        for row in range(2, row_max + 1):
            yield row

    @classmethod
    def all_cols(cls, sheet: Worksheet):
        col_max = sheet.max_column
        for col in range(1, col_max + 1):
            yield col

    def load_headers(self, sheet: Worksheet):
        headers: List[str] = [str(sheet.cell(row=1, column=col).value)
                              for col in self.all_cols(sheet)]
        self.headers_by_sheetname[sheet.title] = headers

    def load_content(self):
        workbook: Workbook = load_workbook(self.filename)
        self.workbook = workbook
        for sheetname in workbook.sheetnames:
            sheet: Worksheet = workbook[sheetname]
            self.load_headers(sheet)
            content = []
            for row in self.all_rows(sheet):
                row_dict = self.load_row(sheet=sheet, row=row)
                content.append(row_dict)
            self.content_by_sheetname[sheetname] = content
        return self.content_by_sheetname

    def load_row(self, *, sheet: Worksheet, row: int):
        headers = self.sheet_headers(sheet)
        row_dict: Dict[str, Any] = {headers[col-1]: sheet.cell(row=row, column=col).value
                                    for col in self.all_cols(sheet)}
        return row_dict


class ItemTools:

    @classmethod
    def compute_patch_prototype(cls, parsed_headers):
        prototype = {}
        for parsed_header in parsed_headers:
            parsed_header0 = parsed_header[0]
            if isinstance(parsed_header0, int):
                raise ValueError(f"A header cannot begin with a numeric ref: {parsed_header0}")
            cls.assure_patch_prototype_shape(parent=prototype, keys=parsed_header)
        return prototype

    @classmethod
    def assure_patch_prototype_shape(cls, *, parent: Union[Dict, List], keys: List[Union[int, str]]):
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
    def parse_sheet_headers(cls, headers):
        return [cls.parse_sheet_header(header)
                for header in headers]

    @classmethod
    def parse_sheet_header(cls, header) -> List[Union[int, str]]:
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
    def set_path_value(cls, datum, path, value, force=False):
        if (value is None or value == '') and not force:
            return
        [key, *more_path] = path
        if not more_path:
            datum[key] = value
        else:
            cls.set_path_value(datum[key], more_path, value)

    @classmethod
    def parse_value(cls, value):
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
        else:  # probably a number
            return value


class ItemManager(ItemTools, WorkbookManager):

    def __init__(self, filename: str):
        super().__init__(filename=filename)
        self.patch_prototypes_by_sheetname: Dict[Dict] = {}
        self.parsed_headers_by_sheetname: Dict[List[List[Union[int, str]]]] = {}

    def sheet_patch_prototype(self, sheet: Worksheet) -> Dict:
        return self.patch_prototypes_by_sheetname[sheet.title]

    def sheet_parsed_headers(self, sheet: Worksheet) -> List[List[Union[int, str]]]:
        return self.parsed_headers_by_sheetname[sheet.title]

    def load_headers(self, sheet: Worksheet):
        super().load_headers(sheet)
        self.compile_sheet_headers(sheet)

    def compile_sheet_headers(self, sheet: Worksheet):
        headers = self.headers_by_sheetname[sheet.title]
        parsed_headers = self.parse_sheet_headers(headers)
        self.parsed_headers_by_sheetname[sheet.title] = parsed_headers
        prototype = self.compute_patch_prototype(parsed_headers)
        self.patch_prototypes_by_sheetname[sheet.title] = prototype

    def load_row(self, *, sheet: Worksheet, row: int):
        parsed_headers = self.sheet_parsed_headers(sheet)
        patch_item = copy.deepcopy(self.sheet_patch_prototype(sheet))
        for col in self.all_cols(sheet):
            value = sheet.cell(row=row, column=col).value
            parsed_value = self.parse_value(value)
            self.set_path_value(patch_item, parsed_headers[col - 1], parsed_value)
        return patch_item
