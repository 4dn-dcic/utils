.. role:: raw-html-m2r(raw)
   :format: html


Examples 
====================================

See `getting started <'./getting_started.md'>`_ for help with getting up and running with dcicutils.

As a first step, we will import our modules from the dcicutils package.

.. code-block::

   from dcicutils import ff_utils

:raw-html-m2r:`<a name="key"></a>`\ Making your key
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Authentication methods differ if you are an external user or an internal 4DN team member. If you are an external user, create a Python dictionary called ``key`` using your access key. This will be used in the examples below.

.. code-block::

   key = {'key': <YOUR KEY>, 'secret' <YOUR SECRET>, 'server': 'https://data.4dnucleome.org/'}

If you are an internal user, you may simply use the string Fourfront environment name for your metadata functions to get administrator access. For faster requests or if you want to emulate another user, you can also pass in keys manually. The examples below will use ``key``\ , but could also use ``ff_env``. It assumes you want to use the data Fourfront environment.

.. code-block::

   key = ff_utils.get_authentication_with_server(ff_env='data')

:raw-html-m2r:`<a name="metadata"></a>`\ Examples for metadata functions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can use ``get_metadata`` to get the metadata for a single object. It returns a dictionary of metadata on a successful get request. In our example, we get a publicly available HEK293 biosource, which has an internal accession of 4DNSRVF4XB1F.

.. code-block::

   metadata = ff_utils.get_metadata('4DNSRVF4XB1F', key=key)

   # the response is a python dictionary
   metadata['accession'] == '4DNSRVF4XB1F'
   >> True

To post new data to the system, use the ``post_metadata`` function. You need to provide the body of data you want to post, as well as the schema name for the object. We want to post a fastq file.

.. code-block::

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

.. code-block::

   patch_body = {'description': 'My cool fastq file'}
   # you can explicitly pass the object ID (in this case accession)...
   response = ff_utils.patch_metadata(patch_body, '4DNFIP74UWGW', key=key)

   # or you can include the ID in the data you patch
   patch_body['accession'] = '4DNFIP74UWGW'
   response = ff_utils.patch_metadata(patch_body, key=key)

   # the response has the same format as in post_metadata
   metadata = response['@graph'][0]

Similar to ``post_metadata`` you can "UPSERT" metadata, which will perform a POST if the metadata doesn't yet exist within the system and will PATCH if it does. The ``upsert_metadata`` function takes the exact same arguments as ``post_metadata`` but will not raise an error on a metadata conflict.

.. code-block::

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

.. code-block::

   # let's search for all biosamples
   # hits is a list of metadata dictionaries
   hits = ff_utils.search_metadata('search/?type=Biosample', key=key)

   # you can also specify a limit on the number of results for your search
   # other valid query params are also allowed, including sorts and filters
   hits = ff_utils.search_metadata('search/?type=Biosample&limit=10', key=key)
