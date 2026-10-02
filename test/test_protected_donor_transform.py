import openpyxl
import os
import pytest
import tempfile

from dcicutils.structured_data import StructuredDataSet
from dcicutils.submitr.custom_excel import CustomExcel
from dcicutils.submitr.donor_transformer import (
    DonorReferenceKind,
    ProtectedDonorLookupError,
    ProtectedDonorTransformError,
    ProtectedDonorWorkbookTransformer,
    analyze_protected_donors,
    lookup_protected_donor,
)


def _workbook(sheets):
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, rows in sheets.items():
        sheet = workbook.create_sheet(name)
        for row in rows:
            sheet.append(row)
    return workbook


def _custom_excel(tmp_path):
    input_path = tmp_path / "input.xlsx"
    _workbook({"Donor": [["submitted_id"], ["A_DONOR_1"]]}).save(input_path)
    return CustomExcel(file=str(input_path))


def test_analysis_uses_only_actual_rows_and_the_five_protected_sheets():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"], [None]],
        "DeathCircumstances": [["donor"], ["A_PROTECTED-DONOR_2"]],
        "FamilyHistory": [["donor"], ["not-an-id"]],
        "MedicalHistory": [["donor"], ["A_DONOR_MISSING"]],
        "TissueCollection": [["donor"], ["A_DONOR_1"]],
        "Tissue": [["donor"], ["A_DONOR_IGNORED"]],
    })

    analysis = analyze_protected_donors(workbook)

    assert analysis.references["A_DONOR_1"] == DonorReferenceKind.PLAIN_DONOR
    assert analysis.references["A_PROTECTED-DONOR_2"] == DonorReferenceKind.MISSING
    assert analysis.references["not-an-id"] == DonorReferenceKind.INVALID
    assert analysis.missing_plain_donor_ids == frozenset({"A_DONOR_MISSING"})
    assert analysis.invalid_references == frozenset({"not-an-id"})
    assert "A_DONOR_IGNORED" not in analysis.references


def test_transform_is_selective_and_supports_mixed_references():
    workbook = _workbook({
        "Donor": [
            ["submitted_id", "external_id", "status"],
            ["A_DONOR_REFERENCED", "one", "released"],
            ["A_DONOR_UNREFERENCED", "two", "released"],
        ],
        "ProtectedDonor": [
            ["submitted_id", "external_id", "status"],
            ["A_PROTECTED-DONOR_EXISTING", "existing", "in review"],
        ],
        "Demographic": [["donor"], ["A_DONOR_REFERENCED"]],
        "MedicalHistory": [["donor"], ["A_PROTECTED-DONOR_EXISTING"]],
        "Tissue": [["donor"], ["A_DONOR_UNREFERENCED"]],
    })

    assert ProtectedDonorWorkbookTransformer().transform(workbook) is True

    donors = workbook["Donor"]
    assert donors.cell(1, 4).value == "protected_donor"
    assert donors.cell(2, 4).value == "A_PROTECTED-DONOR_REFERENCED"
    assert donors.cell(3, 4).value is None
    protected = workbook["ProtectedDonor"]
    assert protected.max_row == 3
    assert protected.cell(3, 1).value == "A_PROTECTED-DONOR_REFERENCED"
    assert protected.cell(3, 2).value == "one"
    assert protected.cell(3, 3).value == "in review"
    assert workbook["Demographic"].cell(2, 1).value == "A_PROTECTED-DONOR_REFERENCED"
    assert workbook["MedicalHistory"].cell(2, 1).value == "A_PROTECTED-DONOR_EXISTING"
    assert workbook["Tissue"].cell(2, 1).value == "A_DONOR_UNREFERENCED"


def test_invalid_donor_reference_is_a_clear_transform_error():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_PRESENT"]],
        "Demographic": [["donor"], ["not-a-donor-id"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="Invalid donor reference"):
        ProtectedDonorWorkbookTransformer().transform(workbook)


def test_missing_plain_donor_is_a_clear_transform_error():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_PRESENT"]],
        "Demographic": [["donor"], ["A_DONOR_ABSENT"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="absent from the workbook Donor sheet"):
        ProtectedDonorWorkbookTransformer().transform(workbook)


def test_portal_only_protected_donor_is_accepted():
    class Portal:
        def get_metadata(self, path, **kwargs):
            assert path == "/ProtectedDonor/A_PROTECTED-DONOR_PORTAL"
            return {"uuid": "portal-uuid"}

    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_UNREFERENCED"]],
        "Demographic": [["donor"], ["A_PROTECTED-DONOR_PORTAL"]],
    })

    analysis = ProtectedDonorWorkbookTransformer(portal=Portal()).analyze(workbook)
    assert analysis.protected_donor_ids == frozenset({"A_PROTECTED-DONOR_PORTAL"})
    assert ProtectedDonorWorkbookTransformer(portal=Portal()).transform(workbook) is False
    assert "ProtectedDonor" not in workbook.sheetnames


def test_existing_protected_sheet_is_extended_incrementally():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_NEW"]],
        "ProtectedDonor": [["submitted_id", "status"], ["A_PROTECTED-DONOR_OLD", "in review"]],
        "FamilyHistory": [["donor"], ["A_DONOR_NEW"]],
    })

    ProtectedDonorWorkbookTransformer().transform(workbook)

    assert workbook["ProtectedDonor"].max_row == 3
    assert workbook["ProtectedDonor"].cell(2, 1).value == "A_PROTECTED-DONOR_OLD"
    assert workbook["ProtectedDonor"].cell(3, 1).value == "A_PROTECTED-DONOR_NEW"


def test_custom_excel_opt_in_integrates_transform_before_structured_data(tmp_path):
    path = tmp_path / "donor.xlsx"
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"]],
    })
    workbook.save(path)

    data = StructuredDataSet(
        file=str(path),
        portal=None,
        excel_class=CustomExcel.with_portal(None, transform_protected_donor=True),
    ).data

    assert data["ProtectedDonor"][0]["submitted_id"] == "A_PROTECTED-DONOR_1"
    assert data["Demographic"][0]["donor"] == "A_PROTECTED-DONOR_1"


def test_custom_excel_transform_preserves_custom_column_mapping(tmp_path, monkeypatch):
    monkeypatch.setattr(
        CustomExcel,
        "_get_custom_column_mappings",
        staticmethod(lambda portal=None: {
            "Demographic": {"donor": {"mapped_donor": "{value}"}}
        }),
    )
    path = tmp_path / "donor.xlsx"
    _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"]],
    }).save(path)

    data = StructuredDataSet(
        file=str(path),
        portal=None,
        excel_class=CustomExcel.with_portal(None, transform_protected_donor=True),
    ).data

    assert data["Demographic"] == [{"mapped_donor": "A_PROTECTED-DONOR_1"}]


def test_save_transformed_workbook_protects_existing_target(tmp_path):
    excel = _custom_excel(tmp_path)
    target = tmp_path / "transformed.xlsx"
    original = b"do not replace"
    target.write_bytes(original)

    with pytest.raises(ValueError, match="already exists"):
        excel._save_transformed_workbook(str(target))

    assert target.read_bytes() == original


def test_save_transformed_workbook_allows_owned_staging_path(tmp_path):
    input_path = tmp_path / "input.xlsx"
    _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"]],
    }).save(input_path)
    temporary_fd, staging_path = tempfile.mkstemp(dir=tmp_path, suffix=".xlsx")
    os.close(temporary_fd)
    with open(staging_path, "wb") as staging_file:
        staging_file.write(b"submitr-owned placeholder")

    CustomExcel(
        file=str(input_path),
        transform_protected_donor=True,
        transformed_workbook_path=staging_path,
        allow_existing_staging_path=True,
    )

    assert openpyxl.load_workbook(staging_path)["Donor"]["A1"].value == "submitted_id"


def test_save_transformed_workbook_repeated_save_requires_explicit_overwrite(tmp_path):
    excel = _custom_excel(tmp_path)
    target = tmp_path / "transformed.xlsx"

    excel._save_transformed_workbook(str(target))
    original = target.read_bytes()
    with pytest.raises(ValueError, match="already exists"):
        excel._save_transformed_workbook(str(target))
    assert target.read_bytes() == original

    excel._workbook["Donor"]["A1"] = "changed"
    excel._save_transformed_workbook(str(target), overwrite=True)
    assert openpyxl.load_workbook(target)["Donor"]["A1"].value == "changed"


def test_failed_save_does_not_damage_existing_target(tmp_path, monkeypatch):
    excel = _custom_excel(tmp_path)
    target = tmp_path / "transformed.xlsx"
    original = b"keep this file"
    target.write_bytes(original)

    def fail_save(path):
        with open(path, "wb") as temporary_file:
            temporary_file.write(b"partial workbook")
        raise OSError("simulated save failure")

    monkeypatch.setattr(excel._workbook, "save", fail_save)
    with pytest.raises(ValueError, match="Cannot save transformed workbook"):
        excel._save_transformed_workbook(str(target), overwrite=True)

    assert target.read_bytes() == original


def test_hidden_and_unlisted_sheets_are_not_transformed():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "(Demographic)": [["donor"], ["A_DONOR_1"]],
        "Tissue": [["donor"], ["A_DONOR_1"]],
    })
    workbook["(Demographic)"].sheet_state = "hidden"

    assert ProtectedDonorWorkbookTransformer().transform(workbook) is False
    assert "ProtectedDonor" not in workbook.sheetnames


# --- Regression tests for PR review findings ---

def _formatted_empty_rows(sheet, count):
    # Formatted-but-empty cells extend max_row without the reader ever seeing data there.
    for offset in range(1, count + 1):
        sheet.cell(sheet.max_row + 1, 1).number_format = "@"
    return sheet


def _read_rows(path, sheet_name):
    return StructuredDataSet(file=str(path), portal=None, norefs=True).data.get(sheet_name, [])


def test_existing_protected_sheet_gains_headers_for_copied_fields():
    workbook = _workbook({
        "Donor": [["submitted_id", "external_id", "status"], ["A_DONOR_NEW", "ext-1", "released"]],
        "ProtectedDonor": [["submitted_id"], ["A_PROTECTED-DONOR_OLD"]],
        "FamilyHistory": [["donor"], ["A_DONOR_NEW"]],
    })

    ProtectedDonorWorkbookTransformer().transform(workbook)

    protected = workbook["ProtectedDonor"]
    headers = [cell.value for cell in protected[1]]
    assert headers == ["submitted_id", "external_id", "status"]
    assert [cell.value for cell in protected[2]] == ["A_PROTECTED-DONOR_OLD", None, None]
    assert [cell.value for cell in protected[3]] == ["A_PROTECTED-DONOR_NEW", "ext-1", "in review"]


def test_existing_protected_sheet_with_unsafe_extra_columns_is_rejected_unchanged():
    workbook = _workbook({
        "Donor": [["submitted_id", "external_id"], ["A_DONOR_NEW", "ext-1"]],
        "ProtectedDonor": [["submitted_id", None, "orphan"], ["A_PROTECTED-DONOR_OLD", None, "stale"]],
        "FamilyHistory": [["donor"], ["A_DONOR_NEW"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="columns cannot be added"):
        ProtectedDonorWorkbookTransformer().transform(workbook)

    assert workbook["ProtectedDonor"].max_row == 2
    assert workbook["Donor"].max_column == 2
    assert workbook["FamilyHistory"]["A2"].value == "A_DONOR_NEW"


@pytest.mark.parametrize("donor_rows", [
    [["submitted_id", "external_id", None, "notes"], ["A_DONOR_NEW", "ext-1", None, "note"]],
    [["submitted_id", "external_id"], ["A_DONOR_NEW", "ext-1", "stray"]],
])
def test_donor_sheet_with_content_beyond_headers_is_rejected_unchanged(donor_rows):
    workbook = _workbook({
        "Donor": donor_rows,
        "FamilyHistory": [["donor"], ["A_DONOR_NEW"]],
    })
    before = {sheet.title: [list(row) for row in sheet.iter_rows(values_only=True)] for sheet in workbook}

    with pytest.raises(ProtectedDonorTransformError, match="lacks a 'protected_donor' column and has content"):
        ProtectedDonorWorkbookTransformer().transform(workbook)

    assert workbook.sheetnames == ["Donor", "FamilyHistory"]
    assert {sheet.title: [list(row) for row in sheet.iter_rows(values_only=True)] for sheet in workbook} == before


def test_donor_sheet_without_transformed_rows_is_not_given_a_protected_donor_column():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_NEW"]],
        "Other_Donor": [["submitted_id", "external_id", None, "notes"], ["A_DONOR_OTHER", "x", None, "note"]],
        "FamilyHistory": [["donor"], ["A_DONOR_NEW"]],
    })

    assert ProtectedDonorWorkbookTransformer(
        effective_sheet_name=lambda name: name.rpartition("_")[2]).transform(workbook) is True

    assert workbook["Donor"].cell(1, 2).value == "protected_donor"
    assert workbook["Donor"].cell(2, 2).value == "A_PROTECTED-DONOR_NEW"
    assert [cell.value for cell in workbook["Other_Donor"][1]] == ["submitted_id", "external_id", None, "notes"]


def test_generated_rows_are_appended_before_formatted_empty_rows_and_stay_visible(tmp_path):
    workbook = _workbook({
        "Donor": [["submitted_id", "status"], ["A_DONOR_1", "released"], ["A_DONOR_2", "released"]],
        "ProtectedDonor": [["submitted_id", "status"], ["A_PROTECTED-DONOR_OLD", "in review"]],
        "FamilyHistory": [["donor"], ["A_DONOR_1"], ["A_DONOR_2"]],
    })
    _formatted_empty_rows(workbook["ProtectedDonor"], 5)
    assert workbook["ProtectedDonor"].max_row > 6

    ProtectedDonorWorkbookTransformer().transform(workbook)

    path = tmp_path / "out.xlsx"
    workbook.save(path)
    ids = [row["submitted_id"] for row in _read_rows(path, "ProtectedDonor")]
    assert ids == ["A_PROTECTED-DONOR_OLD", "A_PROTECTED-DONOR_1", "A_PROTECTED-DONOR_2"]


def test_content_after_the_empty_row_terminator_blocks_append_and_leaves_workbook_unchanged():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "ProtectedDonor": [["submitted_id"], ["A_PROTECTED-DONOR_OLD"], [None], ["A_PROTECTED-DONOR_STALE"]],
        "FamilyHistory": [["donor"], ["A_DONOR_1"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="after its first empty row"):
        ProtectedDonorWorkbookTransformer().transform(workbook)

    assert workbook["ProtectedDonor"].max_row == 4
    assert workbook["ProtectedDonor"]["A3"].value is None
    assert workbook["FamilyHistory"]["A2"].value == "A_DONOR_1"
    assert "protected_donor" not in [cell.value for cell in workbook["Donor"][1]]


def test_references_after_the_empty_row_terminator_are_not_scanned():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"], [None], ["A_DONOR_STALE"]],
        "Demographic": [["donor"], ["A_DONOR_1"], [None], ["not-an-id"], ["A_DONOR_STALE"]],
    })

    analysis = analyze_protected_donors(workbook)
    assert set(analysis.references) == {"A_DONOR_1"}

    assert ProtectedDonorWorkbookTransformer().transform(workbook) is True
    assert workbook["Demographic"]["A2"].value == "A_PROTECTED-DONOR_1"
    # Rows after the terminator are never ingested, so they are never rewritten either.
    assert workbook["Demographic"]["A4"].value == "not-an-id"
    assert workbook["Demographic"]["A5"].value == "A_DONOR_STALE"
    assert workbook["Donor"].cell(4, 1).value == "A_DONOR_STALE"
    assert workbook["Donor"].max_column == 2
    assert workbook["Donor"].cell(4, 2).value is None


class _TypedPortal:
    """Portal stub answering typed ProtectedDonor lookups by identifier."""

    def __init__(self, items=None, error=None):
        self.items = items or {}
        self.error = error
        self.paths = []

    def get_metadata(self, path, **kwargs):
        self.paths.append(path)
        if self.error:
            raise self.error
        prefix = "/ProtectedDonor/"
        assert path.startswith(prefix)
        if path[len(prefix):] in self.items:
            return self.items[path[len(prefix):]]
        raise Exception(f"Bad status code for GET request for https://portal{path}: 404. Reason: Not Found")


UUID = "11111111-2222-3333-4444-555555555555"
ACCESSION = "SMAPD1234567"


@pytest.mark.parametrize("identifier", [UUID, ACCESSION])
def test_uuid_and_accession_references_to_existing_protected_donors_are_accepted(identifier):
    portal = _TypedPortal({identifier: {"uuid": UUID, "@type": ["ProtectedDonor", "Item"]}})
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], [identifier], ["A_DONOR_1"]],
    })

    analysis = ProtectedDonorWorkbookTransformer(portal=portal).analyze(workbook)
    assert analysis.references[identifier] == DonorReferenceKind.PROTECTED_DONOR
    assert not analysis.invalid_references

    assert ProtectedDonorWorkbookTransformer(portal=portal).transform(workbook) is True
    assert workbook["Demographic"]["A2"].value == identifier
    assert workbook["Demographic"]["A3"].value == "A_PROTECTED-DONOR_1"


def test_workbook_protected_donor_uuid_and_accession_are_resolved_without_the_portal():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_UNREFERENCED"]],
        "ProtectedDonor": [["submitted_id", "uuid", "accession"], ["A_PROTECTED-DONOR_X", UUID, ACCESSION]],
        "Demographic": [["donor"], [UUID], [ACCESSION]],
    })

    analysis = analyze_protected_donors(workbook)

    assert analysis.protected_donor_ids == frozenset({UUID, ACCESSION})


def test_identifier_resolving_to_a_different_type_is_not_a_protected_donor():
    portal = _TypedPortal({UUID: {"uuid": UUID, "@type": ["Donor", "Item"]}})
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], [UUID]],
    })

    analysis = ProtectedDonorWorkbookTransformer(portal=portal).analyze(workbook)

    assert analysis.references[UUID] == DonorReferenceKind.INVALID
    with pytest.raises(ProtectedDonorTransformError, match="Invalid donor reference"):
        ProtectedDonorWorkbookTransformer(portal=portal).transform(workbook)


def test_unresolvable_identifier_without_portal_stays_invalid():
    workbook = _workbook({"Demographic": [["donor"], [UUID]]})

    assert analyze_protected_donors(workbook).references[UUID] == DonorReferenceKind.INVALID


@pytest.mark.parametrize("error,kind", [
    (Exception("Bad status code for GET request for https://p/x: 403. Reason: Forbidden"),
     DonorReferenceKind.PERMISSION_DENIED),
    (Exception("Bad status code for GET request for https://p/x: 401. Reason: Unauthorized"),
     DonorReferenceKind.PERMISSION_DENIED),
    (Exception("HTTPForbidden: no access"), DonorReferenceKind.PERMISSION_DENIED),
    (Exception("Bad status code for GET request for https://p/404-404: 503. Reason: Unavailable"),
     DonorReferenceKind.LOOKUP_FAILED),
    (Exception("Bad status code for GET request for https://p/x: 500. Reason: Internal Server Error - "
               "{'@type': ['HTTPNotFound', 'Error'], 'detail': 'sub-lookup failed'}"),
     DonorReferenceKind.LOOKUP_FAILED),
    (ConnectionError("connection refused"), DonorReferenceKind.LOOKUP_FAILED),
    (TimeoutError("timed out"), DonorReferenceKind.LOOKUP_FAILED),
])
def test_lookup_failures_are_distinct_from_absence(error, kind):
    portal = _TypedPortal(error=error)
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_PROTECTED-DONOR_PORTAL"]],
    })

    analysis = ProtectedDonorWorkbookTransformer(portal=portal).analyze(workbook)

    assert analysis.references["A_PROTECTED-DONOR_PORTAL"] == kind
    assert not analysis.missing_references
    assert analysis.lookup_errors["A_PROTECTED-DONOR_PORTAL"]
    with pytest.raises(ProtectedDonorLookupError, match="not known to be absent") as caught:
        ProtectedDonorWorkbookTransformer(portal=portal).transform(workbook)
    assert "absent from the workbook and portal" not in str(caught.value)
    assert workbook["Demographic"]["A2"].value == "A_PROTECTED-DONOR_PORTAL"


@pytest.mark.parametrize("portal", [
    _TypedPortal(),
    _TypedPortal(error=Exception("HTTPNotFound: /ProtectedDonor/x")),
    _TypedPortal(items={"A_PROTECTED-DONOR_PORTAL": {"@type": ["HTTPNotFound", "Error"], "status": "error",
                                                     "code": 404}}),
])
def test_definitive_not_found_is_reported_as_absent(portal):
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_PROTECTED-DONOR_PORTAL"]],
    })

    analysis = ProtectedDonorWorkbookTransformer(portal=portal).analyze(workbook)

    assert analysis.references["A_PROTECTED-DONOR_PORTAL"] == DonorReferenceKind.MISSING
    with pytest.raises(ProtectedDonorTransformError, match="absent from the workbook and portal") as caught:
        ProtectedDonorWorkbookTransformer(portal=portal).transform(workbook)
    assert not isinstance(caught.value, ProtectedDonorLookupError)


def test_error_result_with_forbidden_type_is_a_permission_failure():
    portal = _TypedPortal({"A_PROTECTED-DONOR_PORTAL": {"@type": ["HTTPForbidden", "Error"], "status": "error"}})
    workbook = _workbook({"Demographic": [["donor"], ["A_PROTECTED-DONOR_PORTAL"]]})

    analysis = ProtectedDonorWorkbookTransformer(portal=portal).analyze(workbook)

    assert analysis.references["A_PROTECTED-DONOR_PORTAL"] == DonorReferenceKind.PERMISSION_DENIED


def test_lookup_identifier_is_path_quoted():
    portal = _TypedPortal({})
    lookup_protected_donor(portal, "a/b?c")

    assert portal.paths == ["/ProtectedDonor/a%2Fb%3Fc"]


def _counting_excel_class(path, **options):
    created = []

    class _Counting(CustomExcel):
        def __init__(self, *args, **kwargs):
            created.append(self)
            kwargs.setdefault("transform_protected_donor", True)
            kwargs.setdefault("transformed_workbook_path", str(path))
            super().__init__(*args, **kwargs)

    return _Counting, created


def test_progress_loading_does_not_save_during_the_counting_pass(tmp_path):
    input_path = tmp_path / "in.xlsx"
    _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"]],
    }).save(input_path)
    staged = tmp_path / "staged.xlsx"
    excel_class, created = _counting_excel_class(staged)
    progress = []

    data = StructuredDataSet(file=str(input_path), portal=None, norefs=True,
                             excel_class=excel_class, progress=progress.append).data

    assert len(created) == 1
    assert progress
    assert data["Demographic"][0]["donor"] == "A_PROTECTED-DONOR_1"
    assert [row["submitted_id"] for row in openpyxl_rows(staged, "ProtectedDonor")] == ["A_PROTECTED-DONOR_1"]


def openpyxl_rows(path, sheet_name):
    sheet = openpyxl.load_workbook(path)[sheet_name]
    headers = [cell.value for cell in sheet[1]]
    return [dict(zip(headers, row)) for row in sheet.iter_rows(min_row=2, values_only=True)]


def test_repeated_construction_still_cannot_clobber_the_staged_path(tmp_path):
    input_path = tmp_path / "in.xlsx"
    _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "Demographic": [["donor"], ["A_DONOR_1"]],
    }).save(input_path)
    staged = tmp_path / "staged.xlsx"
    kwargs = dict(file=str(input_path), transform_protected_donor=True, transformed_workbook_path=str(staged))

    CustomExcel(**kwargs)
    original = staged.read_bytes()
    with pytest.raises(ValueError, match="already exists"):
        CustomExcel(**kwargs)

    assert staged.read_bytes() == original
