import re

from dcicutils.common import S3ObjectNameDict
from typing import Optional


# NOTE: This could be done with urllib's parsing tech, but it accepts a variety of things we don't want,
#       so the error-checking would be more complicated. The documentation says particular string formats
#       are accepted, so that's what we're using for now. -kmp 16-May-2023
LOCATION_STRING_PATTERN = re.compile("^([^/?]+)/([^?]+)(?:[?]versionId=([^&]*))?$")


def parse_s3_object_name(object_name, ignore_errors=False) -> Optional[S3ObjectNameDict]:
    """
    Parses a string of the form bucket/key or bucket/key?versionId=version, yielding a dictionary form
    {"Bucket": bucket, "Key": key} or {"Bucket": bucket, "Key": key, "VersionId": version_id}

    :param object_name: a string specifying a bucket, key, and optionally a version
    :return: a dictionary
    """
    location_data = LOCATION_STRING_PATTERN.match(object_name)
    if not location_data:
        if ignore_errors:
            return None
        else:
            raise ValueError(f"Not a valid S3 object name: {object_name!r}."
                             f" Format must be bucket/key or bucket/key?versionId=version")
    bucket, key, version_id = location_data.groups()
    result: S3ObjectNameDict = {'Bucket': bucket, 'Key': key}
    if version_id:
        result['VersionId'] = version_id
    return result
