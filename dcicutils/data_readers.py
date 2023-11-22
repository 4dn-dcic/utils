import abc
import csv
import openpyxl
from typing import Any, Generator, Iterator, List, Optional, Tuple, Union
from dcicutils.misc_utils import right_trim


class RowReader(abc.ABC):  # These readers may evenutally go into dcicutils.

    def __init__(self):
        self.header = None
        self.location = 0
        self._warning_empty_headers = False
        self._warning_extra_values = []  # Line numbers.
        self.open()

    def __iter__(self) -> Iterator:
        for row in self.rows:
            self.location += 1
            if self.is_terminating_row(row):
                break
            if len(self.header) < len(row):  # Row values beyond what there are headers for are ignored.
                self._warning_extra_values.append(self.location)
            yield {column: self.cell_value(value) for column, value in zip(self.header, row)}

    def _define_header(self, header: List[Optional[Any]]) -> None:
        self.header = []
        for index, column in enumerate(header or []):
            if not (column := str(column).strip() if column is not None else ""):
                self._warning_empty_headers = True
                break  # Empty header column signals end of header.
            self.header.append(column)

    def rows(self) -> Generator[Union[List[Optional[Any]], Tuple[Optional[Any], ...]], None, None]:
        yield

    def is_terminating_row(self, row: Union[List[Optional[Any]], Tuple[Optional[Any]]]) -> bool:
        return False

    def cell_value(self, value: Optional[Any]) -> Optional[Any]:
        return str(value).strip() if value is not None else ""

    def open(self) -> None:
        pass

    @property
    def issues(self) -> Optional[List[str]]:
        issues = []
        if self._warning_empty_headers:
            issues.append("Empty header column encountered; ignoring it and all subsequent columns.")
        if self._warning_extra_values:
            issues.extend([f"Extra column values on row [{row_number}]" for row_number in self._warning_extra_values])
        return issues if issues else None


class ListReader(RowReader):

    def __init__(self, rows: List[List[Optional[Any]]]) -> None:
        self._rows = rows
        super().__init__()

    @property
    def rows(self) -> Generator[List[Optional[Any]], None, None]:
        for row in self._rows[1:]:
            yield row

    def open(self) -> None:
        if not self.header:
            self._define_header(self._rows[0] if self._rows else [])


class CsvReader(RowReader):

    def __init__(self, file: str) -> None:
        self._file = file
        self._file_handle = None
        self._rows = None
        super().__init__()

    @property
    def rows(self) -> Generator[List[Optional[Any]], None, None]:
        for row in self._rows:
            yield right_trim(row)

    def open(self) -> None:
        if self._file_handle is None:
            self._file_handle = open(self._file)
            self._rows = csv.reader(self._file_handle, delimiter="\t" if self._file.endswith(".tsv") else ",")
            self._define_header(right_trim(next(self._rows, [])))

    def __del__(self) -> None:
        if (file_handle := self._file_handle) is not None:
            self._file_handle = None
            file_handle.close()


class ExcelSheetReader(RowReader):

    def __init__(self, sheet_name: str, workbook: openpyxl.workbook.workbook.Workbook) -> None:
        self.sheet_name = sheet_name or "Sheet1"
        self._workbook = workbook
        self._rows = None
        super().__init__()

    @property
    def rows(self) -> Generator[Tuple[Optional[Any], ...], None, None]:
        for row in self._rows(min_row=2, values_only=True):
            yield right_trim(row)

    def is_terminating_row(self, row: Tuple[Optional[Any]]) -> bool:
        return all(cell is None for cell in row)  # Empty row signals end of data.

    def open(self) -> None:
        if not self._rows:
            self._rows = self._workbook[self.sheet_name].iter_rows
            self._define_header(right_trim(next(self._rows(min_row=1, max_row=1, values_only=True), [])))


class Excel:

    def __init__(self, file: str) -> None:
        self._file = file
        self._workbook = None
        self.sheet_names = None
        self.open()

    def sheet_reader(self, sheet_name: str) -> ExcelSheetReader:
        return ExcelSheetReader(sheet_name=sheet_name, workbook=self._workbook)

    def open(self) -> None:
        if self._workbook is None:
            self._workbook = openpyxl.load_workbook(self._file, data_only=True)
            self.sheet_names = self._workbook.sheetnames or []

    def __del__(self) -> None:
        if (workbook := self._workbook) is not None:
            self._workbook = None
            workbook.close()
