import os

_account_number = os.environ.get('ACCOUNT_NUMBER')

if _account_number and _account_number != '643366669028':
    raise Exception(f"These tests must be run with legacy credentials."
                    f"Your credentials are set to account {_account_number}")
