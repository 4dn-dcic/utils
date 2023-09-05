
Getting started
===============

The dcicutils package contains a number of helpful utility functions that are useful for both internal use (both infrastructure and scripting) and external user use. Before getting into the functions themselves, we will go over how to set up your authentication as both as internal DCIC user and external user.

First, install dcicutils using pip. Python 2.7 and 3.x are supported.

``pip install dcicutils``

Internal DCIC set up
^^^^^^^^^^^^^^^^^^^^

To fully utilize the utilities, you should have your AWS credentials set up. In addition, you should also have the ``S3_ENCRYPT_KEY`` environment variable needed for decrypting the administrator access keys stored on Amazon S3. Usually, this is done by leveraging the ``ff_env`` kwarg in various ``dcicutils.ff_utils`` functions. If you would rather not set these up, using a local administrator access key generated from Fourfront is also an option; see the instructions for external set up below.

External set up
^^^^^^^^^^^^^^^

The utilities require an access key, which is generated using your user account on Fourfront. If you do not yet have an account, you can `create one <https://data.4dnucleome.org/help/user-guide/account-creation>`_ using Google or Github authentication. Once you have an account, you can generate an access key on your `user information page <https://data.4dnucleome.org/me>`_ when your account is set up and you are logged in. Make sure to take note of the information generated when you make an access key. Store it in a safe place, because it will be needed when you make a request to Fourfront.

The main format of the authorization used for the utilities is:

``{'key': <YOUR KEY>, 'secret' <YOUR SECRET>, 'server': 'https://data.4dnucleome.org'}``

You can replace server with another Fourfront environment if you have an access key made on that environment.

Central metadata functions
^^^^^^^^^^^^^^^^^^^^^^^^^^

The most useful utilities functions for most users are the metadata functions, which generally are used to access, create, or edit object metadata on the Fourfront portal. Since this utilities module is a pip-installable Python package, they can be leveraged as an API to the portal in your scripts. All of these functions are contained within  ``dcicutils.ff_utils.py``.

See example usage of these functions `here <./examples.html#metadata>`_

Local development notes
^^^^^^^^^^^^^^^^^^^^^^^
For local debugging and development, there is some support for the ``localstack`` package (https://localstack.cloud/), or something like it, to use a local ersatz version of certain AWS services, namely, currently, for S3 and SQS, whereby any boto3 based calls which use these services will optionally use the local ersatz version. To take advantage of this simply set the environment variables ``LOCALSTACK_S3_URL`` and/or ``LOCALSTACK_SQS_URL`` environment variables when running your process, whatever that may be, e.g. ``export LOCALSTACK_S3_URL=http://localhost:4566``. To install ``localstack`` use ``pip install localstack``, and to start it use ``localstack``. FYI this support is implemented via "monkey patching" hooked on these environment variables (see the ``boto_monkey_patching.py`` module for details).
