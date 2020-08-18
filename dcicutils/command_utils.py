from .misc_utils import PRINT


def _ask_boolean_question(question, quick=None, default=None):
    """
    Loops asking a question interactively until it gets a 'yes' or 'no' response. Returns True or False accordingly.

    :param question: The question to ask (without prompts for possible responses, which will be added automatically).
    :param quick: Whether to allow short-form responses.
    :param default: Whether to provide a default obtained by just pressing Enter.
    :return: True or False
    """

    if quick is None:
        # If the default is not None, we're accepting Enter for yes, so we might as well accept 'y'.
        quick = (default is not None)

    affirmatives = ['y', 'yes'] if quick else ['yes']
    negatives = ['n', 'no'] if quick else ['no']
    affirmative = affirmatives[0]
    negative = negatives[0]
    prompt = ("%s [%s/%s]: "
              % (question,
                 affirmative.upper() if default is True else affirmative,
                 negative.upper() if default is False else negative))
    while True:
        answer = input(prompt).strip().lower()
        if answer in affirmatives:
            return True
        elif answer in negatives:
            return False
        elif answer == "" and default is not None:
            return default
        else:
            PRINT("Please answer '%s' or '%s'." % (affirmative, negative))
            if default is not None:
                PRINT("The default if you just press Enter is '%s'."
                      % (affirmative if default else negative))


def yes_or_no(question):
    """
    Asks a 'yes' or 'no' question.

    Either 'y' or 'yes' (in any case) is an acceptable affirmative.
    Either 'n' or 'no' (in any case) is an acceptable negative.
    The response must be confirmed by pressing Enter.
    There is no default. If Enter is pressed after other text than the valid responses, the user is reprompted.
    """
    return _ask_boolean_question(question, quick=False)


def y_or_n(question, default=None):
    """
    Asks a 'y' or 'n' question.

    Either 'y' or 'yes' (in any case) is an acceptable affirmative.
    Either 'n' or 'no' (in any case) is an acceptable negative.
    The response must be confirmed by pressing Enter.
    If Enter is pressed with no input text, the default is returned if there is one, or else the user is re-prompted.
    If Enter is pressed after other text than the valid responses, the user is reprompted.
    """
    return _ask_boolean_question(question, quick=True, default=default)
