"""Analysis and selective transformation of ProtectedDonor workbook references.

The public entry points are :func:`analyze_protected_donors` and
:class:`ProtectedDonorWorkbookTransformer`.  ``analyze`` inspects the logical
data rows (the rows the workbook reader ingests: those before the first empty row)
in the five protected-item sheets and classifies each ``donor`` value as
``plain_donor``, ``protected_donor``, ``missing``, ``invalid``, ``permission_denied``,
or ``lookup_failed``.  ``transform`` rewrites only referenced plain Donors and adds the
corresponding ProtectedDonor rows.

The submitr worker should enable this in its ``CustomExcel`` class with
``transform_protected_donor=True`` and pass its portal object through
``CustomExcel.with_portal``.  Portal lookups are deliberately made here,
before StructuredDataSet reads the workbook; ``structured_data.py`` does not
need to know about this SMaHT-specific conversion.
"""

from dataclasses import dataclass, field
from enum import Enum
import re
from typing import Callable, Dict, Iterable, List, Optional, Set
from urllib.parse import quote

import openpyxl
from openpyxl.worksheet.worksheet import Worksheet

from dcicutils.misc_utils import right_trim


DONOR_SHEET = "Donor"
PROTECTED_DONOR_SHEET = "ProtectedDonor"
SUBMITTED_ID_COLUMN = "submitted_id"
PROTECTED_DONOR_COLUMN = "protected_donor"
DONOR_LINK_COLUMN = "donor"
DONOR_TOKEN = "_DONOR_"
PROTECTED_DONOR_TOKEN = "_PROTECTED-DONOR_"
PROTECTED_DONOR_STATUS = "in review"
PROTECTED_REFERENCE_SHEETS = frozenset({
    "Demographic",
    "DeathCircumstances",
    "FamilyHistory",
    "MedicalHistory",
    "TissueCollection",
})
SERVER_MANAGED_COLUMNS = frozenset({"accession", "uuid", "alternate_accessions"})


class ProtectedDonorTransformError(ValueError):
    """Raised when a workbook cannot be safely transformed."""


class ProtectedDonorLookupError(ProtectedDonorTransformError):
    """Raised when portal lookup of a ProtectedDonor failed, as opposed to finding it absent."""


class DonorReferenceKind(str, Enum):
    PLAIN_DONOR = "plain_donor"
    PROTECTED_DONOR = "protected_donor"
    MISSING = "missing"
    INVALID = "invalid"
    PERMISSION_DENIED = "permission_denied"
    LOOKUP_FAILED = "lookup_failed"


class LookupOutcome(str, Enum):
    FOUND = "found"
    ABSENT = "absent"
    PERMISSION_DENIED = "permission_denied"
    FAILED = "failed"


@dataclass(frozen=True)
class ProtectedDonorLookup:
    """Typed result of looking up a ProtectedDonor identifier in the portal."""

    outcome: LookupOutcome
    detail: Optional[str] = None


_ERROR_STATUS_PATTERNS = (
    (re.compile(r"HTTPNotFound"), 404),
    (re.compile(r"HTTPForbidden"), 403),
    (re.compile(r"HTTPUnauthorized"), 401),
    (re.compile(r": (\d{3})\. Reason"), None),  # dcicutils.ff_utils request errors
    (re.compile(r"Bad response: (\d{3})"), None),  # webtest
    (re.compile(r"\b(\d{3}) (?:Client|Server) Error"), None),  # requests
)


def _http_status_of(error) -> Optional[int]:
    for source in (error, getattr(error, "response", None)):
        for attribute in ("status_code", "status", "code"):
            value = getattr(source, attribute, None)
            if isinstance(value, int) and not isinstance(value, bool) and 100 <= value < 600:
                return value
    text = str(error)
    for pattern, status in _ERROR_STATUS_PATTERNS:
        if match := pattern.search(text):
            return status or int(match.group(1))
    return None


def _lookup_from_status(status: Optional[int], detail: str) -> ProtectedDonorLookup:
    if status in (404, 410):
        return ProtectedDonorLookup(LookupOutcome.ABSENT, detail)
    if status in (401, 403):
        return ProtectedDonorLookup(LookupOutcome.PERMISSION_DENIED, detail)
    return ProtectedDonorLookup(LookupOutcome.FAILED, detail)


def lookup_protected_donor(portal, identifier: str) -> ProtectedDonorLookup:
    """Resolve ``identifier`` (any identifier the portal accepts: submitted_id, UUID, accession)
    as a ProtectedDonor.

    Only a definitive not-found result is ``ABSENT``; permission errors and every other failure
    (outage, timeout, malformed response) are reported distinctly so they are never mistaken for
    absence.  The lookup is made through the typed ``/ProtectedDonor/<identifier>`` path, so the
    portal itself enforces the item type; a returned ``@type`` is also checked when present.
    """
    if portal is None:
        return ProtectedDonorLookup(LookupOutcome.ABSENT, "no portal available")
    path = f"/{PROTECTED_DONOR_SHEET}/{quote(identifier, safe='')}"
    get_metadata = getattr(portal, "get_metadata", None)
    ref_exists = getattr(portal, "ref_exists", None)
    try:
        if callable(get_metadata):
            try:
                result = get_metadata(path, raw=True)
            except TypeError as error:
                if "keyword" not in str(error):
                    raise
                result = get_metadata(path)
        elif callable(ref_exists):
            result = ref_exists(PROTECTED_DONOR_SHEET, identifier)
        else:
            return ProtectedDonorLookup(LookupOutcome.FAILED, "portal supports no ProtectedDonor lookup")
    except Exception as error:
        return _lookup_from_status(_http_status_of(error), f"{type(error).__name__}: {error}")
    if not result:
        return ProtectedDonorLookup(LookupOutcome.ABSENT, "not found")
    if isinstance(result, dict):
        types = result.get("@type")
        if result.get("status") == "error" or (isinstance(types, list) and "Error" in types):
            code = result.get("code")
            status = code if isinstance(code, int) else _http_status_of(" ".join(map(str, types or [])))
            return _lookup_from_status(status, str(result.get("description") or result.get("title") or result))
        if isinstance(types, list) and PROTECTED_DONOR_SHEET not in types:
            return ProtectedDonorLookup(LookupOutcome.ABSENT, f"found item is not a {PROTECTED_DONOR_SHEET}")
    return ProtectedDonorLookup(LookupOutcome.FOUND)


@dataclass(frozen=True)
class ProtectedDonorAnalysis:
    """The result of analyzing actual protected-item data rows.

    ``references`` maps each distinct non-empty donor reference to its kind.
    ``plain_donor_ids`` and ``protected_donor_ids`` contain references found
    in the workbook (or, for ProtectedDonor values, in the portal).  A value
    in ``missing_references`` has the right identifier shape but was not found
    where it can be resolved.  ``invalid_references`` has no valid Donor or
    ProtectedDonor identifier shape and does not resolve to a ProtectedDonor.
    ``permission_denied_references`` and ``lookup_failed_references`` could not be
    resolved because the portal lookup itself failed; they are not known to be absent,
    and ``lookup_errors`` holds the failure detail for each.
    """

    references: Dict[str, DonorReferenceKind]
    plain_donor_ids: frozenset
    protected_donor_ids: frozenset
    missing_references: frozenset
    invalid_references: frozenset
    missing_plain_donor_ids: frozenset
    missing_protected_donor_ids: frozenset
    permission_denied_references: frozenset = frozenset()
    lookup_failed_references: frozenset = frozenset()
    lookup_errors: Dict[str, str] = field(default_factory=dict)

    @property
    def referenced_plain_donor_ids(self) -> frozenset:
        return self.plain_donor_ids

    @property
    def referenced_protected_donor_ids(self) -> frozenset:
        return self.protected_donor_ids

    @property
    def needs_transformation(self) -> bool:
        return bool(self.plain_donor_ids)


def to_protected_donor_submitted_id(submitted_id: str) -> str:
    """Return the ProtectedDonor ID corresponding to a plain Donor ID."""
    if not isinstance(submitted_id, str) or DONOR_TOKEN not in submitted_id:
        raise ProtectedDonorTransformError(
            f"Cannot derive ProtectedDonor submitted_id from {submitted_id!r}; "
            f"expected token {DONOR_TOKEN!r}."
        )
    return submitted_id.replace(DONOR_TOKEN, PROTECTED_DONOR_TOKEN, 1)


def analyze_protected_donors(workbook: openpyxl.Workbook, portal=None,
                             effective_sheet_name: Optional[Callable[[str], str]] = None
                             ) -> ProtectedDonorAnalysis:
    """Analyze protected-item donor references in ``workbook``.

    Only logical data rows (those before the first empty row, as the workbook
    reader sees them) of :data:`PROTECTED_REFERENCE_SHEETS` count.  A reference
    that is not a workbook Donor is resolved as a ProtectedDonor, by workbook
    ProtectedDonor identifiers or else through ``portal`` when supplied, whatever
    its spelling (submitted_id, UUID, accession).  The portal is expected to provide
    ``get_metadata('/ProtectedDonor/<identifier>')`` (portal clients) or, failing that,
    ``ref_exists(type_name, value)``; see :func:`lookup_protected_donor`.
    """
    transformer = ProtectedDonorWorkbookTransformer(effective_sheet_name=effective_sheet_name)
    return transformer.analyze(workbook, portal=portal)


class ProtectedDonorWorkbookTransformer:
    """Selectively transform workbook references from Donor to ProtectedDonor."""

    def __init__(self, effective_sheet_name: Optional[Callable[[str], str]] = None,
                 portal=None) -> None:
        self._effective_sheet_name = effective_sheet_name or (lambda sheet_name: sheet_name)
        self._portal = portal

    def analyze(self, workbook: openpyxl.Workbook, portal=None) -> ProtectedDonorAnalysis:
        """Classify references without changing ``workbook``."""
        portal = self._portal if portal is None else portal
        donor_ids = self._submitted_ids(self._sheets_by_effective_name(workbook, DONOR_SHEET))
        protected_ids = self._identifiers(
            self._sheets_by_effective_name(workbook, PROTECTED_DONOR_SHEET),
            (SUBMITTED_ID_COLUMN, "uuid", "accession"),
        )
        references = self._referenced_donor_values(workbook)
        classifications = {}
        plain_ids = set()
        protected_found = set()
        missing_plain = set()
        missing_protected = set()
        invalid = set()
        permission_denied = set()
        lookup_failed = set()
        lookup_errors = {}

        for reference in sorted(references):
            if DONOR_TOKEN in reference:
                if reference in donor_ids:
                    classifications[reference] = DonorReferenceKind.PLAIN_DONOR
                    plain_ids.add(reference)
                else:
                    classifications[reference] = DonorReferenceKind.MISSING
                    missing_plain.add(reference)
                continue
            if reference in protected_ids:
                lookup = ProtectedDonorLookup(LookupOutcome.FOUND)
            else:
                lookup = lookup_protected_donor(portal, reference)
            if lookup.outcome == LookupOutcome.FOUND:
                classifications[reference] = DonorReferenceKind.PROTECTED_DONOR
                protected_found.add(reference)
            elif lookup.outcome == LookupOutcome.PERMISSION_DENIED:
                classifications[reference] = DonorReferenceKind.PERMISSION_DENIED
                permission_denied.add(reference)
                lookup_errors[reference] = lookup.detail or ""
            elif lookup.outcome == LookupOutcome.FAILED:
                classifications[reference] = DonorReferenceKind.LOOKUP_FAILED
                lookup_failed.add(reference)
                lookup_errors[reference] = lookup.detail or ""
            elif PROTECTED_DONOR_TOKEN in reference:
                classifications[reference] = DonorReferenceKind.MISSING
                missing_protected.add(reference)
            else:
                classifications[reference] = DonorReferenceKind.INVALID
                invalid.add(reference)

        return ProtectedDonorAnalysis(
            references=classifications,
            plain_donor_ids=frozenset(plain_ids),
            protected_donor_ids=frozenset(protected_found),
            missing_references=frozenset(missing_plain | missing_protected),
            invalid_references=frozenset(invalid),
            missing_plain_donor_ids=frozenset(missing_plain),
            missing_protected_donor_ids=frozenset(missing_protected),
            permission_denied_references=frozenset(permission_denied),
            lookup_failed_references=frozenset(lookup_failed),
            lookup_errors=lookup_errors,
        )

    def transform(self, workbook: openpyxl.Workbook, portal=None) -> bool:
        """Apply the analyzed selective conversion in place.

        Missing, invalid, or unresolvable references fail before any cell is changed.
        Missing plain Donors are always errors even when a portal is available; a
        ProtectedDonor can instead be resolved by that portal.  A failed portal lookup
        (permission or outage) raises :class:`ProtectedDonorLookupError`, never the
        absence error.
        """
        analysis = self.analyze(workbook, portal=portal)
        if analysis.invalid_references:
            raise ProtectedDonorTransformError(
                "Invalid donor reference(s) in protected-item data rows: "
                + self._format_ids(analysis.invalid_references)
            )
        if analysis.missing_plain_donor_ids:
            raise ProtectedDonorTransformError(
                "Plain Donor reference(s) are absent from the workbook Donor sheet: "
                + self._format_ids(analysis.missing_plain_donor_ids)
            )
        if analysis.permission_denied_references or analysis.lookup_failed_references:
            denied = analysis.permission_denied_references
            failed = analysis.lookup_failed_references
            problems = []
            if denied:
                problems.append("permission denied looking up ProtectedDonor reference(s) "
                                + self._format_ids(denied))
            if failed:
                problems.append("lookup failed for ProtectedDonor reference(s) " + self._format_ids(failed))
            details = "; ".join(sorted({analysis.lookup_errors.get(r, "") for r in denied | failed} - {""}))
            raise ProtectedDonorLookupError(
                "Cannot verify ProtectedDonor references (not known to be absent): "
                + "; ".join(problems) + (f" [{details}]" if details else "")
            )
        if analysis.missing_protected_donor_ids:
            raise ProtectedDonorTransformError(
                "ProtectedDonor reference(s) are absent from the workbook and portal: "
                + self._format_ids(analysis.missing_protected_donor_ids)
            )
        if not analysis.plain_donor_ids:
            return False

        donor_sheets = self._sheets_by_effective_name(workbook, DONOR_SHEET)
        protected_sheets = self._sheets_by_effective_name(workbook, PROTECTED_DONOR_SHEET)
        workbook_protected_ids = self._submitted_ids(protected_sheets)
        rows_to_add = []
        planned_ids = set()

        # Validate all existing protected_donor cells before mutating any row.
        for sheet in donor_sheets:
            headers = self._headers(sheet)
            submitted_id_column = self._column_index(headers, SUBMITTED_ID_COLUMN)
            protected_column = self._column_index(headers, PROTECTED_DONOR_COLUMN)
            if submitted_id_column is None:
                continue
            for row_number in self._logical_row_numbers(sheet):
                donor_id = self._normalized(sheet.cell(row_number, submitted_id_column).value)
                if donor_id not in analysis.plain_donor_ids:
                    continue
                expected = to_protected_donor_submitted_id(donor_id)
                if protected_column is not None:
                    existing = self._normalized(sheet.cell(row_number, protected_column).value)
                    if existing and existing != expected:
                        raise ProtectedDonorTransformError(
                            f"Donor row {row_number} protected_donor value "
                            f"{sheet.cell(row_number, protected_column).value!r} does not match "
                            f"expected {expected!r}."
                        )
                if expected not in workbook_protected_ids and expected not in planned_ids:
                    planned_ids.add(expected)
                    rows_to_add.append((sheet, row_number, headers, expected))

        if not rows_to_add:
            # References may already have workbook ProtectedDonor rows.  The
            # donor-link rewrite is still useful only for newly plain links.
            self._add_protected_donor_columns(workbook, analysis)
            self._rewrite_donor_links(workbook, analysis)
            return True

        new_rows = [self._protected_row_from_donor_row(sheet, row_number, headers, protected_id)
                    for sheet, row_number, headers, protected_id in rows_to_add]
        if protected_sheets:
            self._validate_append_target(protected_sheets[0], new_rows)
        protected_sheet = self._protected_sheet_for_append(workbook, protected_sheets, donor_sheets[0])
        self._append_protected_rows(protected_sheet, new_rows)

        self._add_protected_donor_columns(workbook, analysis)
        self._rewrite_donor_links(workbook, analysis)
        return True

    def _add_protected_donor_columns(self, workbook, analysis: ProtectedDonorAnalysis) -> None:
        for sheet in self._sheets_by_effective_name(workbook, DONOR_SHEET):
            headers = self._headers(sheet)
            submitted_id_column = self._column_index(headers, SUBMITTED_ID_COLUMN)
            if submitted_id_column is None:
                continue
            protected_column = self._ensure_column(sheet, headers, PROTECTED_DONOR_COLUMN)
            for row_number in self._logical_row_numbers(sheet):
                donor_id = self._normalized(sheet.cell(row_number, submitted_id_column).value)
                if donor_id in analysis.plain_donor_ids:
                    sheet.cell(row_number, protected_column).value = to_protected_donor_submitted_id(donor_id)

    def _rewrite_donor_links(self, workbook, analysis: ProtectedDonorAnalysis) -> None:
        for sheet in self._protected_reference_sheets(workbook):
            headers = self._headers(sheet)
            donor_column = self._column_index(headers, DONOR_LINK_COLUMN)
            if donor_column is None:
                continue
            for row_number in self._logical_row_numbers(sheet):
                value = self._normalized(sheet.cell(row_number, donor_column).value)
                if value in analysis.plain_donor_ids:
                    sheet.cell(row_number, donor_column).value = to_protected_donor_submitted_id(value)

    def _protected_sheet_for_append(self, workbook, protected_sheets, donor_sheet):
        if protected_sheets:
            return protected_sheets[0]
        name = PROTECTED_DONOR_SHEET
        donor_title = donor_sheet.title
        prefix, separator, _ = donor_title.rpartition("_")
        if separator and prefix:
            name = f"{prefix}_{PROTECTED_DONOR_SHEET}"
        return workbook.create_sheet(name)

    def _protected_row_from_donor_row(self, sheet, row_number, headers, protected_id):
        row = {}
        for column_number, header in enumerate(headers, start=1):
            if not header or header in SERVER_MANAGED_COLUMNS or header == PROTECTED_DONOR_COLUMN:
                continue
            value = sheet.cell(row_number, column_number).value
            if header == SUBMITTED_ID_COLUMN:
                value = protected_id
            elif header == "status":
                value = PROTECTED_DONOR_STATUS
            row[header] = value
        row.setdefault("status", PROTECTED_DONOR_STATUS)
        return row

    @staticmethod
    def _validate_append_target(sheet, rows) -> None:
        """Fail, before anything is changed, if ``rows`` cannot be appended to ``sheet`` such that
        the reader ingests all of them and nothing else."""
        headers = ProtectedDonorWorkbookTransformer._headers(sheet)
        if any(key not in headers for row in rows for key in row):
            # Adding header columns is only safe if no data or header lies beyond the current
            # headers; the reader would otherwise start ingesting that content as the new fields.
            if ProtectedDonorWorkbookTransformer._has_content(sheet, min_col=len(headers) + 1):
                raise ProtectedDonorTransformError(
                    f"Sheet {sheet.title!r} lacks columns for copied Donor fields and has content "
                    f"beyond its header columns, so columns cannot be added safely."
                )
        last_row = ProtectedDonorWorkbookTransformer._logical_last_row(sheet)
        if ProtectedDonorWorkbookTransformer._has_content(sheet, min_row=last_row + 2):
            raise ProtectedDonorTransformError(
                f"Sheet {sheet.title!r} has content after its first empty row (row {last_row + 1}); "
                f"appended ProtectedDonor rows would merge with that content."
            )

    @staticmethod
    def _append_protected_rows(sheet, rows) -> None:
        headers = ProtectedDonorWorkbookTransformer._headers(sheet)
        for row in rows:
            for key in row:
                if key not in headers:
                    ProtectedDonorWorkbookTransformer._ensure_column(sheet, headers, key)
        row_number = ProtectedDonorWorkbookTransformer._logical_last_row(sheet) + 1
        for row in rows:
            for column_number, header in enumerate(headers, start=1):
                sheet.cell(row_number, column_number).value = row.get(header)
            row_number += 1

    @staticmethod
    def _has_content(sheet, min_row: int = 1, min_col: int = 1) -> bool:
        return any(value not in (None, "")
                   for row in sheet.iter_rows(min_row=min_row, min_col=min_col, values_only=True)
                   for value in row)

    @staticmethod
    def _logical_last_row(sheet) -> int:
        """The last row the workbook reader ingests (1, the header, if none).

        Mirrors ExcelSheetReader: data starts in row 2 and ends before the first row that is empty
        once trailing empty cells are trimmed; later rows are never read, however many are formatted.
        """
        last_row = 1
        for row in sheet.iter_rows(min_row=2, values_only=True):
            if not right_trim(row):
                break
            last_row += 1
        return last_row

    @staticmethod
    def _logical_row_numbers(sheet) -> range:
        return range(2, ProtectedDonorWorkbookTransformer._logical_last_row(sheet) + 1)

    @staticmethod
    def _collect_referenced_donor_values(workbook, effective_sheet_name=None) -> Set[str]:
        effective_sheet_name = effective_sheet_name or (lambda name: name)
        result = set()
        for sheet in workbook.worksheets:
            if ProtectedDonorWorkbookTransformer._is_hidden_sheet(sheet) or \
                    effective_sheet_name(sheet.title) not in PROTECTED_REFERENCE_SHEETS:
                continue
            headers = ProtectedDonorWorkbookTransformer._headers(sheet)
            donor_column = ProtectedDonorWorkbookTransformer._column_index(headers, DONOR_LINK_COLUMN)
            if donor_column is None:
                continue
            for row_number in ProtectedDonorWorkbookTransformer._logical_row_numbers(sheet):
                value = ProtectedDonorWorkbookTransformer._normalized(
                    sheet.cell(row_number, donor_column).value
                )
                if value:
                    result.add(value)
        return result

    def _protected_reference_sheets(self, workbook):
        return [sheet for sheet in self._visible_sheets(workbook)
                if self._effective_sheet_name(sheet.title) in PROTECTED_REFERENCE_SHEETS]

    def _sheets_by_effective_name(self, workbook, name):
        return [sheet for sheet in self._visible_sheets(workbook)
                if self._effective_sheet_name(sheet.title) == name]

    @staticmethod
    def _visible_sheets(workbook):
        return [sheet for sheet in workbook.worksheets
                if not ProtectedDonorWorkbookTransformer._is_hidden_sheet(sheet)]

    @staticmethod
    def _identifiers(sheets, columns) -> Set[str]:
        """Values in the logical data rows of ``sheets`` under any of the given columns."""
        result = set()
        for sheet in sheets:
            headers = ProtectedDonorWorkbookTransformer._headers(sheet)
            column_numbers = [number for number in (
                ProtectedDonorWorkbookTransformer._column_index(headers, name) for name in columns
            ) if number is not None]
            for row_number in ProtectedDonorWorkbookTransformer._logical_row_numbers(sheet):
                for column in column_numbers:
                    value = ProtectedDonorWorkbookTransformer._normalized(sheet.cell(row_number, column).value)
                    if value:
                        result.add(value)
        return result

    @staticmethod
    def _submitted_ids(sheets):
        return ProtectedDonorWorkbookTransformer._identifiers(sheets, (SUBMITTED_ID_COLUMN,))

    @staticmethod
    def _headers(sheet) -> List[str]:
        headers = []
        for cell in sheet[1]:
            if cell.value is None or not str(cell.value).strip():
                break
            headers.append(str(cell.value).strip())
        return headers

    @staticmethod
    def _column_index(headers: List[str], column_name: str) -> Optional[int]:
        try:
            return headers.index(column_name) + 1
        except ValueError:
            return None

    @staticmethod
    def _ensure_column(sheet, headers: List[str], column_name: str) -> int:
        column = ProtectedDonorWorkbookTransformer._column_index(headers, column_name)
        if column is None:
            column = len(headers) + 1
            sheet.cell(1, column).value = column_name
            headers.append(column_name)
        return column

    @staticmethod
    def _normalized(value) -> str:
        return str(value).strip() if value is not None else ""

    @staticmethod
    def _is_hidden_sheet(sheet: Worksheet) -> bool:
        title = sheet.title
        return sheet.sheet_state == "hidden" or any(
            title.startswith(left) and title.endswith(right)
            for left, right in (("(", ")"), ("[", "]"), ("{", "}"), ("<", ">"))
        )

    @staticmethod
    def _format_ids(values: Iterable[str]) -> str:
        return ", ".join(repr(value) for value in sorted(values))

    def _referenced_donor_values(self, workbook):
        return self._collect_referenced_donor_values(
            workbook, effective_sheet_name=self._effective_sheet_name
        )
