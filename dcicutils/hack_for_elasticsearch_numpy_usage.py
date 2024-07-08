# Though dcicutils is not dependent on numpy, elasticsearch pulls it in iff it is installed,
# and if it is numpy 2.x the numpy.float_ constant has been retired and any reference to it
# yields an error from numpy (AttributeError: np.float_ was removed in the NumPy 2.0 release.
# Use np.float64 instead); this reference to numpy.float_ occurs in elasticsearch/serializer.py
try:
    import numpy
    numpy.float_ = numpy.float64
except Exception:
    pass
