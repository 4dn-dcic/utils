SAMPLE_PROJECT_UUID = "dac6d5b3-6ef6-4271-9715-a78329acf846"
SAMPLE_PROJECT_NAME = 'test-project'
SAMPLE_PROJECT_TITLE = SAMPLE_PROJECT_NAME.title().replace('-', ' ')
SAMPLE_PROJECT = {
    "title": SAMPLE_PROJECT_TITLE,
    "uuid": SAMPLE_PROJECT_UUID,
    "description": f"This is the {SAMPLE_PROJECT_TITLE}.",
    "name": SAMPLE_PROJECT_NAME,
    "status": "shared",
    "date_created": "2020-11-24T20:46:00.000000+00:00",
}
SAMPLE_PROJECT_SANS_UUID = SAMPLE_PROJECT.copy()  # to be modified on next line
SAMPLE_PROJECT_SANS_UUID.pop('uuid')

SAMPLE_INSTITUTION_UUID = "87199845-51b5-4352-bdea-583edae4bb6a"
SAMPLE_INSTITUTION_NAME = "cgap-backend-team"
SAMPLE_INSTITUTION_TITLE = SAMPLE_INSTITUTION_NAME.title().replace('-', ' ')
SAMPLE_INSTITUTION = {
    "name": SAMPLE_INSTITUTION_NAME,
    "title": SAMPLE_INSTITUTION_TITLE,
    "status": "shared",
    "uuid": SAMPLE_INSTITUTION_UUID,
}
SAMPLE_INSTITUTION_SANS_UUID = SAMPLE_INSTITUTION.copy()  # to be modified on next line
SAMPLE_INSTITUTION_SANS_UUID.pop('uuid')

SAMPLE_USER_EMAIL = "jdoe@example.com"
SAMPLE_USER_FIRST_NAME = "Jenny"
SAMPLE_USER_LAST_NAME = "Doe"
SAMPLE_USER_ROLE = "developer"
SAMPLE_USER_UUID = "e0dec518-cb0c-45f3-8c97-21b2659ec129"
SAMPLE_USER_WITH_UUID_REFS = {
    "email": SAMPLE_USER_EMAIL,
    "first_name": SAMPLE_USER_FIRST_NAME,
    "last_name": SAMPLE_USER_LAST_NAME,
    "uuid": SAMPLE_USER_UUID,
    "project": SAMPLE_PROJECT_UUID,
    "project_roles#0.project": SAMPLE_PROJECT_UUID,
    "project_roles#0.role": SAMPLE_USER_ROLE,
    "user_institution": SAMPLE_INSTITUTION_UUID,
}
SAMPLE_USER_WITH_NAME_REFS = {
    "email": SAMPLE_USER_EMAIL,
    "first_name": SAMPLE_USER_FIRST_NAME,
    "last_name": SAMPLE_USER_LAST_NAME,
    "uuid": SAMPLE_USER_UUID,
    "project": SAMPLE_PROJECT_NAME,
    "project_roles#0.project": SAMPLE_PROJECT_NAME,
    "project_roles#0.role": SAMPLE_USER_ROLE,
    "user_institution": SAMPLE_INSTITUTION_NAME,
}

SAMPLE_WORKBOOK_WITH_UNMATCHED_UUID_REFS = {
    # Here the User refers to project and institution by UUID, but we don't have the UUID in our local cache
    "User": [SAMPLE_USER_WITH_UUID_REFS],
    "Project": [SAMPLE_PROJECT_SANS_UUID],
    "Institution": [SAMPLE_INSTITUTION_SANS_UUID],
}

SAMPLE_WORKBOOK_WITH_MATCHED_UUID_REFS = {
    "User": [SAMPLE_USER_WITH_UUID_REFS],
    "Project": [SAMPLE_PROJECT],
    "Institution": [SAMPLE_INSTITUTION],
}

SAMPLE_WORKBOOK_WITH_NAME_REFS = {
    "User": [SAMPLE_USER_WITH_NAME_REFS],
    "Project": [SAMPLE_PROJECT],
    "Institution": [SAMPLE_INSTITUTION],
}
