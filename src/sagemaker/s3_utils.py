# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
"""This module contains helper functions related to S3. You may want to use `s3.py` instead.

This has a subset of the functions available through s3.py. This module was initially created with
functions that were originally in `s3.py` so that those functions could be imported inside
`session.py` without circular dependencies. (`s3.py` imports Session as a dependency.)
"""
from __future__ import print_function, absolute_import

import pathlib
import logging
from typing import Optional

from six.moves.urllib.parse import urlparse

logger = logging.getLogger("sagemaker")


def parse_s3_url(url):
    """Returns an (s3 bucket, key name/prefix) tuple from a url with an s3 scheme.

    Args:
        url (str):

    Returns:
        tuple: A tuple containing:

            - str: S3 bucket name
            - str: S3 key
    """
    parsed_url = urlparse(url)
    if parsed_url.scheme != "s3":
        raise ValueError("Expecting 's3' scheme, got: {} in {}.".format(parsed_url.scheme, url))
    return parsed_url.netloc, parsed_url.path.lstrip("/")


def s3_path_join(*args):
    """Returns the arguments joined by a slash ("/"), similar to ``os.path.join()`` (on Unix).

    If the first argument is "s3://", then that is preserved.

    Args:
        *args: The strings to join with a slash. None or empty strings are skipped. For example,
            an args of ["", "key1", None, "key2"] would yield an output of "key1/key2".

    Returns:
        str: The joined string, without a slash at the end.
    """
    filtered_args = list(filter(lambda item: item is not None and item != "", args))
    if len(filtered_args) == 0:
        return ""

    if filtered_args[0].startswith("s3://"):
        path = str(pathlib.PurePosixPath(*filtered_args[1:])).lstrip("/")
        return str(pathlib.PurePosixPath(filtered_args[0], path)).replace("s3:/", "s3://")

    return str(pathlib.PurePosixPath(*filtered_args)).lstrip("/")


def s3_path_join_with_end_slash(*args):
    """Deprecated, use :func:`s3_path_join`. This returns the same output, except with an end slash.

    This function was added to maintain backwards compatibility because some S3 paths explicitly
    had an end slash added. It may be the case that this end slash is not actually necessary to
    maintain, in which case this function can be removed.
    """
    path = s3_path_join(*args)

    if path:
        return path + "/"

    return path


def calculate_bucket_and_prefix(
    bucket: Optional[str], key_prefix: Optional[str], sagemaker_session
):
    """Helper function that returns the correct S3 bucket and prefix to use depending on the inputs.

    Args:
        bucket (Optional[str]): S3 Bucket to use (if it exists)
        key_prefix (Optional[str]): S3 Object Key Prefix to use or append to (if it exists)
        sagemaker_session (sagemaker.session.Session): Session to fetch a default bucket and
            prefix from, if bucket doesn't exist. Expected to exist

    Returns: The correct S3 Bucket and S3 Object Key Prefix that should be used
    """
    if bucket:
        final_bucket = bucket
        final_key_prefix = key_prefix
    else:
        final_bucket = sagemaker_session.default_bucket()

        # default_bucket_prefix (if it exists) should be appended if (and only if) 'bucket' does not
        # exist and we are using the Session's default_bucket.
        final_key_prefix = s3_path_join(sagemaker_session.default_bucket_prefix, key_prefix)

    # We should not append default_bucket_prefix even if the bucket exists but is equal to the
    # default_bucket, because either:
    # (1) the bucket was explicitly passed in by the user and just happens to be the same as the
    # default_bucket (in which case we don't want to change the user's input), or
    # (2) the default_bucket was fetched from Session earlier already (and the default prefix
    # should have been fetched then as well), and then this function was
    # called with it. If we appended the default prefix here, we would be appending it more than
    # once in total.

    return final_bucket, final_key_prefix
