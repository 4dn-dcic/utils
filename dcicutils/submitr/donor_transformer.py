from typing import Callable, List, Optional

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet


DONOR_SHEET = "Donor"
PROTECTED_DONOR_SHEET = "ProtectedDonor"
SUBMITTED_ID_COLUMN = "submitted_id"
PROTECTED_DONOR_COLUMN = "protected_donor"
DONOR_LINK_COLUMN = "donor"
DONOR_TOKEN = "_DONOR_"
PROTECTED_DONOR_TOKEN = "_PROTECTED-DONOR_"
PROTECTED_DONOR_STATUS = "in review"
PROTECTED_REFERENCE_SHEETS = {
    "Demographic",
    "DeathCircumstances",
    "FamilyHistory",
    "MedicalHistory",
    "TissueCollection",
}
SERVER_MANAGED_COLUMNS = {
    "accession",
    "uuid",
    "alternate_accessions",
}


class ProtectedDonorTransformError(ValueError):
    pass


def to_protected_donor_submitted_id(submitted_id: Optional[str]) -> Optional[str]:
    if not isinstance(submitted_id, str) or DONOR_TOKEN not in submitted_id:
        raise ProtectedDonorTransformError(
            f"Cannot derive ProtectedDonor submitted_id from {submitted_id!r}; expected token {DONOR_TOKEN!r}."
        )
    return submitted_id.replace(DONOR_TOKEN, PROTECTED_DONOR_TOKEN, 1)


class ProtectedDonorWorkbookTransformer:
    """Transforms SMaHT donor workbooks in memory for ProtectedDonor ingestion."""

    def __init__(self, effective_sheet_name: Optional[Callable[[str], str]] = None) -> None:
        self._effective_sheet_name = effective_sheet_name or (lambda sheet_name: sheet_name)

    def transform(self, workbook: openpyxl.Workbook) -> bool:
        transformed = False
        donor_sheets = self._visible_sheets_by_effective_name(workbook, DONOR_SHEET)
        if not donor_sheets:
            return transformed
        for donor_sheet in donor_sheets:
            transformed = self._transform_donor_sheet(workbook, donor_sheet) or transformed
        for sheet in self._visible_sheets(workbook):
            if self._effective_sheet_name(sheet.title) in PROTECTED_REFERENCE_SHEETS:
                transformed = self._rewrite_donor_links(sheet) or transformed
        return transformed

    def _transform_donor_sheet(self, workbook: openpyxl.Workbook, donor_sheet: Worksheet) -> bool:
        headers = self._headers(donor_sheet)
        submitted_id_column = self._column_index(headers, SUBMITTED_ID_COLUMN)
        if submitted_id_column is None:
            return False
        protected_donor_column = self._ensure_column(donor_sheet, headers, PROTECTED_DONOR_COLUMN)
        expected_rows = []
        for row_number in range(2, donor_sheet.max_row + 1):
            donor_id = donor_sheet.cell(row_number, submitted_id_column).value
            if donor_id in [None, ""]:
                continue
            protected_id = to_protected_donor_submitted_id(str(donor_id).strip())
            existing = donor_sheet.cell(row_number, protected_donor_column).value
            if existing not in [None, ""] and str(existing).strip() != protected_id:
                raise ProtectedDonorTransformError(
                    f"Donor row {row_number} protected_donor value {existing!r} does not match "
                    f"expected {protected_id!r}."
                )
            donor_sheet.cell(row_number, protected_donor_column).value = protected_id
            expected_rows.append(self._protected_row_from_donor_row(donor_sheet, row_number, headers, protected_id))
        if not expected_rows:
            return False
        protected_sheet_name = self._protected_sheet_name_for(donor_sheet.title)
        if protected_sheet_name in workbook.sheetnames:
            self._validate_existing_protected_donor_sheet(workbook[protected_sheet_name], expected_rows)
        else:
            self._create_protected_donor_sheet(workbook, protected_sheet_name, expected_rows)
        return True

    def _protected_sheet_name_for(self, donor_sheet_name: str) -> str:
        if self._effective_sheet_name(donor_sheet_name) == DONOR_SHEET:
            prefix, separator, _ = donor_sheet_name.rpartition("_")
            if separator and prefix:
                return f"{prefix}_{PROTECTED_DONOR_SHEET}"
        return PROTECTED_DONOR_SHEET

    def _protected_row_from_donor_row(self, sheet: Worksheet, row_number: int,
                                      headers: List[str], protected_id: str) -> dict:
        row = {}
        for column_number, header in enumerate(headers, start=1):
            if not header or header in SERVER_MANAGED_COLUMNS or header == PROTECTED_DONOR_COLUMN:
                continue
            value = sheet.cell(row_number, column_number).value
            if header == SUBMITTED_ID_COLUMN:
                value = protected_id
            elif header == "status":
                # ProtectedDonor submissions should enter the portal as in review, regardless
                # of any public Donor status value in the source workbook.
                value = PROTECTED_DONOR_STATUS
            row[header] = value
        if "status" not in row:
            row["status"] = PROTECTED_DONOR_STATUS
        return row

    def _create_protected_donor_sheet(self, workbook: openpyxl.Workbook,
                                      sheet_name: str, rows: List[dict]) -> None:
        sheet = workbook.create_sheet(sheet_name)
        headers = list(rows[0].keys())
        for column_number, header in enumerate(headers, start=1):
            sheet.cell(1, column_number).value = header
        for row_number, row in enumerate(rows, start=2):
            for column_number, header in enumerate(headers, start=1):
                sheet.cell(row_number, column_number).value = row.get(header)

    def _validate_existing_protected_donor_sheet(self, sheet: Worksheet, expected_rows: List[dict]) -> None:
        headers = self._headers(sheet)
        submitted_id_column = self._column_index(headers, SUBMITTED_ID_COLUMN)
        if submitted_id_column is None:
            raise ProtectedDonorTransformError(
                f"Existing {sheet.title!r} sheet is missing required {SUBMITTED_ID_COLUMN!r} column."
            )
        actual_by_id = {}
        for row_number in range(2, sheet.max_row + 1):
            submitted_id = sheet.cell(row_number, submitted_id_column).value
            if submitted_id in [None, ""]:
                continue
            actual_by_id[str(submitted_id).strip()] = self._row_dict(sheet, row_number, headers)
        expected_ids = {expected.get(SUBMITTED_ID_COLUMN) for expected in expected_rows}
        unexpected_ids = set(actual_by_id.keys()) - expected_ids
        if unexpected_ids:
            raise ProtectedDonorTransformError(
                f"Existing {sheet.title!r} sheet contains unexpected ProtectedDonor submitted_id(s): "
                f"{', '.join(sorted(unexpected_ids))}."
            )
        for expected in expected_rows:
            expected_id = expected.get(SUBMITTED_ID_COLUMN)
            actual = actual_by_id.get(expected_id)
            if actual is None:
                raise ProtectedDonorTransformError(
                    f"Existing {sheet.title!r} sheet is missing expected ProtectedDonor {expected_id!r}."
                )
            for key, expected_value in expected.items():
                if key not in actual:
                    continue
                actual_value = actual.get(key)
                if self._normalized(actual_value) != self._normalized(expected_value):
                    raise ProtectedDonorTransformError(
                        f"Existing {sheet.title!r} row for {expected_id!r} has {key!r} value "
                        f"{actual_value!r}; expected {expected_value!r}."
                    )

    def _rewrite_donor_links(self, sheet: Worksheet) -> bool:
        transformed = False
        headers = self._headers(sheet)
        donor_column = self._column_index(headers, DONOR_LINK_COLUMN)
        if donor_column is None:
            return transformed
        for row_number in range(2, sheet.max_row + 1):
            value = sheet.cell(row_number, donor_column).value
            if isinstance(value, str) and DONOR_TOKEN in value:
                sheet.cell(row_number, donor_column).value = to_protected_donor_submitted_id(value)
                transformed = True
        return transformed

    def _visible_sheets_by_effective_name(self, workbook: openpyxl.Workbook, effective_name: str) -> List[Worksheet]:
        return [sheet for sheet in self._visible_sheets(workbook)
                if self._effective_sheet_name(sheet.title) == effective_name]

    @staticmethod
    def _visible_sheets(workbook: openpyxl.Workbook) -> List[Worksheet]:
        return [sheet for sheet in workbook.worksheets if not ProtectedDonorWorkbookTransformer._is_hidden_sheet(sheet)]

    @staticmethod
    def _is_hidden_sheet(sheet: Worksheet) -> bool:
        title = sheet.title
        if sheet.sheet_state == "hidden":
            return True
        return ((title.startswith("(") and title.endswith(")")) or
                (title.startswith("[") and title.endswith("]")) or
                (title.startswith("{") and title.endswith("}")) or
                (title.startswith("<") and title.endswith(">")))

    @staticmethod
    def _headers(sheet: Worksheet) -> List[str]:
        headers = []
        for cell in sheet[1]:
            value = cell.value
            if value is None or not str(value).strip():
                break
            headers.append(str(value).strip())
        return headers

    @staticmethod
    def _column_index(headers: List[str], column_name: str) -> Optional[int]:
        try:
            return headers.index(column_name) + 1
        except ValueError:
            return None

    @staticmethod
    def _ensure_column(sheet: Worksheet, headers: List[str], column_name: str) -> int:
        if (column_index := ProtectedDonorWorkbookTransformer._column_index(headers, column_name)) is not None:
            return column_index
        column_index = len(headers) + 1
        sheet.cell(1, column_index).value = column_name
        headers.append(column_name)
        return column_index

    @staticmethod
    def _row_dict(sheet: Worksheet, row_number: int, headers: List[str]) -> dict:
        return {header: sheet.cell(row_number, column_number).value
                for column_number, header in enumerate(headers, start=1)}

    @staticmethod
    def _normalized(value):
        if value is None:
            return ""
        return str(value).strip()
