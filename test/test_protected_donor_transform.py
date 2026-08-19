import openpyxl
import pytest

from dcicutils.structured_data import StructuredDataSet
from dcicutils.submitr.custom_excel import CustomExcel
from dcicutils.submitr.donor_transformer import (
    DonorReferenceKind,
    ProtectedDonorTransformError,
    ProtectedDonorWorkbookTransformer,
    analyze_protected_donors,
)


def _workbook(sheets):
    workbook = openpyxl.Workbook()
    workbook.remove(workbook.active)
    for name, rows in sheets.items():
        sheet = workbook.create_sheet(name)
        for row in rows:
            sheet.append(row)
    return workbook


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


def test_hidden_and_unlisted_sheets_are_not_transformed():
    workbook = _workbook({
        "Donor": [["submitted_id"], ["A_DONOR_1"]],
        "(Demographic)": [["donor"], ["A_DONOR_1"]],
        "Tissue": [["donor"], ["A_DONOR_1"]],
    })
    workbook["(Demographic)"].sheet_state = "hidden"

    assert ProtectedDonorWorkbookTransformer().transform(workbook) is False
    assert "ProtectedDonor" not in workbook.sheetnames
