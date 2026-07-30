import openpyxl
import pytest

from dcicutils.structured_data import StructuredDataSet
from dcicutils.submitr.custom_excel import CustomExcel
from dcicutils.submitr.donor_transformer import (
    ProtectedDonorTransformError,
    to_protected_donor_submitted_id,
)


def _write_workbook(path, sheets):
    workbook = openpyxl.Workbook()
    default = workbook.active
    workbook.remove(default)
    for sheet_name, rows in sheets.items():
        sheet = workbook.create_sheet(sheet_name)
        for row in rows:
            sheet.append(row)
    workbook.save(path)


def _load(path, excel_class=CustomExcel):
    return StructuredDataSet(file=str(path), portal=None, excel_class=excel_class).data


def test_to_protected_donor_submitted_id_replaces_only_donor_token():
    assert to_protected_donor_submitted_id("ABC_DONOR_1234") == "ABC_PROTECTED-DONOR_1234"
    assert to_protected_donor_submitted_id("ABC_DONOR_1234_DONOR_X") == "ABC_PROTECTED-DONOR_1234_DONOR_X"
    with pytest.raises(ProtectedDonorTransformError, match="expected token"):
        to_protected_donor_submitted_id("ABC-DONOR-1234")


def test_protected_donor_transform_generates_sheet_and_rewrites_protected_refs(tmp_path):
    path = tmp_path / "donor.xlsx"
    _write_workbook(path, {
        "Donor": [
            ["submitted_id", "external_id", "sex", "age", "status"],
            ["ABC_DONOR_0001", "EXT-1", "Female", "45", "released"],
        ],
        "Demographic": [
            ["submitted_id", "donor", "race"],
            ["ABC_DEMOGRAPHIC_0001", "ABC_DONOR_0001", "reported unknown"],
        ],
        "MedicalHistory": [
            ["submitted_id", "donor", "tobacco_use"],
            ["ABC_MEDICAL-HISTORY_0001", "ABC_DONOR_0001", "No"],
        ],
        "Tissue": [
            ["submitted_id", "donor", "uberon_id"],
            ["ABC_TISSUE_0001", "ABC_DONOR_0001", "UBERON:0000955"],
        ],
    })

    data = _load(path)

    assert data["Donor"] == [{
        "submitted_id": "ABC_DONOR_0001",
        "external_id": "EXT-1",
        "sex": "Female",
        "age": "45",
        "status": "released",
        "protected_donor": "ABC_PROTECTED-DONOR_0001",
    }]
    assert data["ProtectedDonor"] == [{
        "submitted_id": "ABC_PROTECTED-DONOR_0001",
        "external_id": "EXT-1",
        "sex": "Female",
        "age": "45",
        "status": "in review",
    }]
    assert data["Demographic"][0]["donor"] == "ABC_PROTECTED-DONOR_0001"
    assert data["MedicalHistory"][0]["donor"] == "ABC_PROTECTED-DONOR_0001"
    assert data["Tissue"][0]["donor"] == "ABC_DONOR_0001"


@pytest.mark.parametrize("sheet_name", [
    "Demographic",
    "DeathCircumstances",
    "FamilyHistory",
    "MedicalHistory",
    "TissueCollection",
])
def test_protected_donor_transform_rewrites_all_protected_reference_sheets(tmp_path, sheet_name):
    path = tmp_path / f"{sheet_name}.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id"], ["ABC_DONOR_0001"]],
        sheet_name: [["submitted_id", "donor"], [f"ABC_{sheet_name.upper()}_0001", "ABC_DONOR_0001"]],
    })

    data = _load(path)

    assert data[sheet_name][0]["donor"] == "ABC_PROTECTED-DONOR_0001"


def test_protected_donor_transform_ignores_parenthesized_sheets(tmp_path):
    path = tmp_path / "hidden_donor.xlsx"
    _write_workbook(path, {
        "(Donor)": [["submitted_id"], ["ABC_DONOR_0001"]],
        "Tissue": [["submitted_id", "donor"], ["ABC_TISSUE_0001", "ABC_DONOR_0001"]],
    })

    data = _load(path)

    assert "Donor" not in data
    assert "ProtectedDonor" not in data
    assert data["Tissue"][0]["donor"] == "ABC_DONOR_0001"


def test_protected_donor_transform_validates_existing_protected_donor_sheet(tmp_path):
    path = tmp_path / "existing_valid.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id", "external_id", "sex"], ["ABC_DONOR_0001", "EXT-1", "Female"]],
        "ProtectedDonor": [["submitted_id", "external_id", "sex", "status"],
                           ["ABC_PROTECTED-DONOR_0001", "EXT-1", "Female", "in review"]],
    })

    data = _load(path)

    assert len(data["ProtectedDonor"]) == 1
    assert data["Donor"][0]["protected_donor"] == "ABC_PROTECTED-DONOR_0001"


def test_protected_donor_transform_rejects_mismatched_existing_protected_donor_sheet(tmp_path):
    path = tmp_path / "existing_invalid.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id", "external_id", "sex"], ["ABC_DONOR_0001", "EXT-1", "Female"]],
        "ProtectedDonor": [["submitted_id", "external_id", "sex", "status"],
                           ["ABC_PROTECTED-DONOR_0001", "DIFFERENT", "Female", "in review"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="external_id"):
        _load(path)


def test_protected_donor_transform_rejects_donor_id_without_donor_token(tmp_path):
    path = tmp_path / "bad_donor_id.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id"], ["ABC-DONOR-0001"]],
    })

    with pytest.raises(ProtectedDonorTransformError, match="expected token"):
        _load(path)


def test_protected_donor_transform_can_save_transformed_workbook(tmp_path):
    path = tmp_path / "input.xlsx"
    output = tmp_path / "transformed.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id"], ["ABC_DONOR_0001"]],
        "Demographic": [["submitted_id", "donor"], ["ABC_DEMOGRAPHIC_0001", "ABC_DONOR_0001"]],
    })

    data = _load(path, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(output)))
    saved_data = _load(output, excel_class=CustomExcel.with_portal(None, transform_protected_donor=False))

    assert output.exists()
    assert data == saved_data
    assert saved_data["ProtectedDonor"][0]["submitted_id"] == "ABC_PROTECTED-DONOR_0001"
    assert saved_data["Demographic"][0]["donor"] == "ABC_PROTECTED-DONOR_0001"


def test_protected_donor_transform_rejects_output_path_same_as_input(tmp_path):
    path = tmp_path / "same.xlsx"
    _write_workbook(path, {"Donor": [["submitted_id"], ["ABC_DONOR_0001"]]})

    with pytest.raises(ValueError, match="must differ from input"):
        _load(path, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(path)))


def test_protected_donor_transform_rejects_existing_output_path(tmp_path):
    path = tmp_path / "input.xlsx"
    output = tmp_path / "existing.xlsx"
    _write_workbook(path, {"Donor": [["submitted_id"], ["ABC_DONOR_0001"]]})
    output.write_text("do not overwrite")

    with pytest.raises(ValueError, match="already exists"):
        _load(path, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(output)))


def test_protected_donor_transform_rejects_reusing_output_for_different_input(tmp_path):
    path1 = tmp_path / "input1.xlsx"
    path2 = tmp_path / "input2.xlsx"
    output = tmp_path / "transformed.xlsx"
    _write_workbook(path1, {"Donor": [["submitted_id"], ["ABC_DONOR_0001"]]})
    _write_workbook(path2, {"Donor": [["submitted_id"], ["ABC_DONOR_0002"]]})

    _load(path1, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(output)))
    with pytest.raises(ValueError, match="already exists"):
        _load(path2, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(output)))


def test_protected_donor_transform_does_not_save_when_no_transform_occurs(tmp_path):
    path = tmp_path / "no_donor.xlsx"
    output = tmp_path / "not_created.xlsx"
    _write_workbook(path, {"Tissue": [["submitted_id", "donor"], ["ABC_TISSUE_0001", "ABC_DONOR_0001"]]})

    _load(path, excel_class=CustomExcel.with_portal(None, transformed_workbook_path=str(output)))

    assert not output.exists()


def test_protected_donor_transform_can_be_disabled(tmp_path):
    path = tmp_path / "disabled.xlsx"
    _write_workbook(path, {
        "Donor": [["submitted_id"], ["ABC_DONOR_0001"]],
        "Demographic": [["submitted_id", "donor"], ["ABC_DEMOGRAPHIC_0001", "ABC_DONOR_0001"]],
    })

    data = _load(path, excel_class=CustomExcel.with_portal(None, transform_protected_donor=False))

    assert "ProtectedDonor" not in data
    assert "protected_donor" not in data["Donor"][0]
    assert data["Demographic"][0]["donor"] == "ABC_DONOR_0001"
