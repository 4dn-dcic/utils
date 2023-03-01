========
Examples
========

See `getting started <./getting_started.html>`_ for help with getting up and running with dcicutils.

As a first step, we will import our modules from the dcicutils package.

.. code-block:: python

   from dcicutils import ff_utils

Making Your key
^^^^^^^^^^^^^^^

Authentication methods differ if you are an external user or an internal 4DN team member. If you are an external user, create a Python dictionary called ``key`` using your access key. This will be used in the examples below.

.. code-block:: python

   key = {'key': YOUR_KEY, 'secret': YOUR_SECRET, 'server': 'https://data.4dnucleome.org/'}

If you are an internal user, you may simply use the string Fourfront environment name for your metadata functions to get administrator access. For faster requests or if you want to emulate another user, you can also pass in keys manually. The examples below will use ``key``\ , but could also use ``ff_env``. It assumes you want to use the data Fourfront environment.

.. code-block:: python

   key = ff_utils.get_authentication_with_server(ff_env='data')

Metadata Function Examples
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use ``get_metadata`` to get the metadata for a single object. It returns a dictionary of metadata on a successful get request. In our example, we get a publicly available HEK293 biosource, which has an internal accession of 4DNSRVF4XB1F.

.. code-block:: python

   metadata = ff_utils.get_metadata('4DNSRVF4XB1F', key=key)

   # the response is a python dictionary
   metadata['accession'] == '4DNSRVF4XB1F'
   >> True

To post new data to the system, use the ``post_metadata`` function. You need to provide the body of data you want to post, as well as the schema name for the object. We want to post a fastq file.

.. code-block:: python

   post_body = {
       'file_format': 'fastq',
       'lab': '/labs/4dn-dcic-lab/',
       'award': '/awards/1U01CA200059-01/'
   }
   response = ff_utils.post_metadata(post_body, 'file_fastq', key=key)

   # response is a dictionary containing info about your post
   response['status']
   >> 'success'
   # the dictionary body of the metadata object created is in response['@graph']
   metadata = response['@graph'][0]

If you want to edit data, use the ``patch_metadata`` function. Let's say that the fastq file you just made has an accession of ``4DNFIP74UWGW`` and we want to add a description to it.

.. code-block:: python

   patch_body = {'description': 'My cool fastq file'}
   # you can explicitly pass the object ID (in this case accession)...
   response = ff_utils.patch_metadata(patch_body, '4DNFIP74UWGW', key=key)

   # or you can include the ID in the data you patch
   patch_body['accession'] = '4DNFIP74UWGW'
   response = ff_utils.patch_metadata(patch_body, key=key)

   # the response has the same format as in post_metadata
   metadata = response['@graph'][0]

Similar to ``post_metadata`` you can "UPSERT" metadata, which will perform a POST if the metadata doesn't yet exist within the system and will PATCH if it does. The ``upsert_metadata`` function takes the exact same arguments as ``post_metadata`` but will not raise an error on a metadata conflict.

.. code-block:: python

   upsert_body = {
       'file_format': 'fastq',
       'lab': '/labs/4dn-dcic-lab/',
       'award': '/awards/1U01CA200059-01/',
       'accession': '4DNFIP74UWGW'
   }
   # this will POST if file 4DNFIP74UWGW does not exist and will PATCH if it does
   response = ff_utils.post_metadata(upsert_body, 'file_fastq', key=key)

   # the response has the same format as in post_metadata
   metadata = response['@graph'][0]

You can use ``search_metadata`` to easily search through metadata in Fourfront. This function takes a string search url starting with 'search', as well as the the same authorization information as the other metadata functions. It returns a list of metadata results. Optionally, the ``page_limit`` parameter can be used to internally adjust the size of the pagination used in underlying generator used to get search results.

.. code-block:: python

   # let's search for all biosamples
   # hits is a list of metadata dictionaries
   hits = ff_utils.search_metadata('search/?type=Biosample', key=key)

   # you can also specify a limit on the number of results for your search
   # other valid query params are also allowed, including sorts and filters
   hits = ff_utils.search_metadata('search/?type=Biosample&limit=10', key=key)

In addition to ``search_metadata``, we also provide ``faceted_search`` which allows you to more cleanly construct search queries without worrying about the query string format. This function utilizes ``search_metadata`` with default arguments and thus acts as a wrapper. Users on JupyterHub should not need to configure ``key`` or ``ff_env``. See below for example usage. See doc-strings and tests for more advanced information/usage.

.. code-block:: python

  # Let's work with experiment sets (the default). We should grab facet information
  # first though. 'facet_info' keys will be the possible facets and each key contains
  # the possible values with their counts
  facet_info = get_item_facet_values('ExperimentSetReplicate')

  # now specify kwargs - say we want to search for all experiments under the 4DN
  # project that are of experiment type 'Dilution Hi-C'
  kwargs = {
    'Project': '4DN',
    'Experiment Type': 'Dilution Hi-C'
  }
  results = faceted_search(**kwargs)

  # you can also search other types by specifying 'item_type' in kwargs
  # say we'd like to search for all users affiliated with the 4DN Testing Lab
  kwargs = {
    'item_type' = 'user',
    'Affiliation' = '4DN Testing Lab'
  }
  results = faceted_search(**kwargs)

  # you can also perform negative searches by pre-pending '-' to your desired value
  # ie: get all users not affiliated with the 4DN Testing Lab
  # note that you can combine this with 'positive' searches as well
  kwargs = {
  'item_type' = 'user',
  'Affiliation' = '-4DN Testing Lab'
  }

  # You can also specify multiple pipe (|) seperated values for a field
  # ie: get all experiments sets from 4DN or External
  kwargs = {
    'Project': '4DN|External'
  }
