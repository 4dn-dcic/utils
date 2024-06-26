import argparse
import botocore.exceptions
import boto3
import contextlib
import os
import sys

from typing import Optional, Union, List
from .command_utils import yes_or_no
from .ecr_utils import ECRUtils
from .misc_utils import PRINT, full_class_name
from .lang_utils import n_of


EPILOG = __doc__


DEFAULT_IMAGE_REPOSITORY = ECRUtils.DEFAULT_IMAGE_REPOSITORY
DEFAULT_ECS_REPOSITORY = DEFAULT_IMAGE_REPOSITORY  # support for deprecated name

# This is the currently selected environment, though commands might want to require confirmation before using it.
DEFAULT_ACCOUNT_NUMBER = os.environ.get('ACCOUNT_NUMBER')

# Use if an unsupplied value requires interactive confirmation before defaulting
CONFIRM_DEFAULT_ACCOUNT_NUMBER = 'confirm-default-account-number'

IMAGE_COUNT_LIMIT = ECRUtils.IMAGE_LIST_DEFAULT_COUNT_LIMIT  # Deprecated (to remove in release 5.0) -kmp 4-Sep-2022
IMAGE_LIST_CHUNK_SIZE = ECRUtils.IMAGE_LIST_CHUNK_SIZE  # Deprecated (to remove in release 5.0) -kmp 4-Sep-2022
RELEASED_TAG = ECRUtils.IMAGE_RELEASED_TAG  # Deprecated (to remove in release 5.0) -kmp 4-Sep-2022


def make_sorted_string_list(elements, separator=","):
    """
    Args:
        elements: a sequence such as a list that can be sorted
        separator: a separator to use between items when the collection is more than a single element.
    Returns:
        a comma-separated string
    """
    return separator.join(sorted(elements))


@contextlib.contextmanager
def ecr_command_context(account_number, ecs_repository=None, ecr_client=None):
    """
    A context manager that takes an account AWS account number specified by the user in a command, and does
    plausibility checking on that account number against the value of the environment variable ACCOUNT_NUMBER.
    If no error occurs, it proceeds to execute a body of code in a context that traps errors in a manner
    appropriate to a script invoked from a command line.

    Args:
        account_number: an AWS account number (represented as a string)
        ecs_repository: The name of an ecs_repository, such as 'main'.
        ecr_client: an AWS ecr_client to use instead of having to make one.
    """
    try:
        try:
            if account_number == CONFIRM_DEFAULT_ACCOUNT_NUMBER:
                if yes_or_no(f"Use {DEFAULT_ACCOUNT_NUMBER} as account number?"):
                    account_number = DEFAULT_ACCOUNT_NUMBER
                else:
                    account_number = None
            if not account_number:
                raise RuntimeError("You must specify an AWS account number.")
            account_number_in_environ = os.environ.get('ACCOUNT_NUMBER')
            if not account_number_in_environ:
                raise RuntimeError("You do not have globally declared credentials."
                                   " As part of setting them, ACCOUNT_NUMBER would be set.")
            elif account_number != account_number_in_environ:
                raise RuntimeError("The account number you have specified does not match your declared credentials.")
            yield ECRCommandContext(account_number=account_number, ecs_repository=ecs_repository, ecr_client=ecr_client)
            sys.exit(0)
        except botocore.exceptions.ClientError as e:
            error_info = e.response.get('Error', {})
            message = error_info.get('Message')
            code = error_info.get('Code')
            if code == 'ExpiredTokenException':
                raise RuntimeError("Your security token seems to have expired.")
            elif message:
                PRINT(f"{code}: {message}")
                sys.exit(1)
            else:
                raise
    except Exception as e:
        PRINT(f"{full_class_name(e)}: {e}")
        sys.exit(1)


class ECRCommandContext:

    def __init__(self, account_number=None, ecs_repository=None, image_repository=None, ecr_client=None):
        """
        Initializes an ECRContextText

        Args:
            account_number: an AWS account number (represented as a string)
            ecs_repository: The name of an ecs_repository, such as 'main'.
            ecr_client: an AWS ecr_client to use instead of having to make one.
        """
        self.account_number = account_number or DEFAULT_ACCOUNT_NUMBER
        self.image_repository = (image_repository
                                 or ecs_repository  # Deprecated
                                 or DEFAULT_IMAGE_REPOSITORY
                                 or DEFAULT_ECS_REPOSITORY)  # Deprecated
        self.ecr_client = ecr_client or boto3.client('ecr', region_name=ECRUtils.REGION)
        self.ecr_utils = ECRUtils(image_repository=self.image_repository, ecr_client=self.ecr_client)

    def get_images_descriptions(self,
                                digests: Optional[List[str]] = None,
                                tags: Optional[List[str]] = None,
                                limit: Optional[Union[int, str]] = ECRUtils.IMAGE_LIST_DEFAULT_COUNT_LIMIT):
        """
        Args:
            digests: a list of image digests (each represented as a string in sha256 format)
            tags: a list of strings (each being a potential or actual image tag).
            limit: an int indicating a limit on how many results to return,
                   a string indicating a limiting tag,
                   or None indicating no limit.

        Returns:
            a dictionary with keys 'descriptions' (the primary result), 'count' (the number of descriptions),
            and 'total' (the total number of registered images in the catalog).
            The descriptions are a list of individual dictionaries with keys that include, at least,
            'imagePushedAt', 'imageTags', and 'imageDigest'.
        """
        return self.ecr_utils.get_images_descriptions(digests=digests, tags=tags, limit=limit)

    def show_image_catalog(self,
                           digests: Optional[List[str]] = None,
                           tags: Optional[List[str]] = None,
                           limit: Optional[Union[int, str]] = ECRUtils.IMAGE_LIST_DEFAULT_COUNT_LIMIT,
                           summarize: bool = True):
        """
        Shows a list of tabular information about images in the given account number and ECS respository.

        Args:
            digests: a list of image digests (each represented as a string in sha256 format)
            tags: a list of strings (each being a potential or actual image tag).
            limit: an int indicating a limit on how many results to return,
                   a string indicating a limiting tag,
                   or None indicating no limit.
            summarize: a boolean saying whether to show a summary at the end
        """
        info = self.get_images_descriptions(digests=digests, tags=tags, limit=limit)
        for image_detail in info['descriptions']:
            image_pushed_at = image_detail['imagePushedAt']
            image_tags = image_detail.get('imageTags', [])
            image_digest = image_detail['imageDigest']
            PRINT(f"{image_digest} {image_pushed_at} {make_sorted_string_list(image_tags, separator=', ')}")
        n = info['count']
        total = info['total']
        if summarize:
            current_account = f"account {self.account_number}" if self.account_number else "current account"
            if digests or tags:
                PRINT(f"Only {n_of(n, 'relevant image')} in {current_account} shown.")
            else:
                PRINT(f"{'All' if n == total else 'Most recent'} {info['count']} of {info['total']} images"
                      f" in {current_account} shown.")

    def get_image_manifest(self, tag=None, digest=None) -> str:
        """
        Returns the imageManifest of an image given a tag and/or digest describing that image.
        (There must be only one matching image.)

        Args:
            digest: a list of image digests (each represented as a string in sha256 format)
            tag: a list of strings (each being a potential or actual image tag).
        Returns:
            a string which is the imageManifest
        Raises:
            RuntimeError: if no images match, or if too many images match
        """
        if tag and not digest:
            image_id = {"imageTag": tag}
        elif digest and not tag:
            image_id = {"imageDigest": digest}
        else:  # neither or both
            raise RuntimeError("Exactly one of --tag and --digest is required.")
        options = {'repositoryName': self.image_repository, 'imageIds': [image_id]}
        response = self.ecr_client.batch_get_image(**options)
        images = response.get('images')
        if not images or len(images) == 0:
            raise RuntimeError(f"No match for {image_id}.")
        elif len(images) > 1 and not all(image['imageManifest'] == images[0]['imageManifest'] for image in images):
            # Usually we get more than one because it's the same image with different labels,
            # but this is just in case the problem is other than that.
            raise RuntimeError(f"Too many ({len(images)}) matches for {image_id}.")
        else:
            image = images[0]
        return image.get('imageManifest')

    def show_image_manifest(self, digest=None, tag=None):
        """
        Shows the 'imageManifest' for a given deployment image, which is the string representation of a JSON dictionary,
        as if json.dumps() of that dictionary had been printed.

        NOTE: This command may be useful primarily for debugging. This manifest is needed to support add_image_tag,
              but most people don't care about seeing this data.

        Args:
            digest: a list of image digests (each represented as a string in sha256 format)
            tag: a list of strings (each being a potential or actual image tag).
        """

        info = self.get_image_manifest(tag=tag, digest=digest)
        PRINT(info)

    def add_image_tag(self, digest, tag):
        """
            digest: a list of image digests (each represented as a string in sha256 format)
            tag: a list of strings (each being a potential or actual image tag).
        """
        relevant_digests = [digest]
        try:
            # This figures out where the image was before. It might be it was nowhere, in which case nothing gets added.
            info = self.get_images_descriptions(tags=[tag])
            old_digest = info['descriptions'][0]['imageDigest']
            relevant_digests.append(old_digest)
        except Exception:
            pass
        old_tags = []
        try:
            # This figures out where the image was before. It might be it was nowhere, in which case nothing gets added.
            info = self.get_images_descriptions(digests=[digest])
            old_tags = info['descriptions'][0]['imageTags']
        except Exception:
            pass
        if tag in old_tags:
            raise RuntimeError(f"Image {digest} is already labeled {tag!r}.")
        if tag.startswith("sha256:"):
            raise RuntimeError(f"Invalid tag: {tag}")
        if not digest.startswith("sha256:"):
            raise RuntimeError(f"Invalid digest (not a sha256): {digest}")
        PRINT("Before:")
        self.show_image_catalog(digests=relevant_digests, summarize=False)
        manifest = self.get_image_manifest(digest=digest)
        self.ecr_client.put_image(repositoryName=self.image_repository, imageManifest=manifest, imageTag=tag)
        # This shows what changed.
        PRINT("After:")
        self.show_image_catalog(digests=relevant_digests)

    def _most_recent_two_deploys(self):
        """
        Returns:
            the most recent two of several deploys
        Raises:
            RuntimeError: if there are not at least two
        """
        info = self.get_images_descriptions()
        descriptions = info.get('descriptions')
        if not descriptions:
            raise RuntimeError("Nothing seems to have been deployed.")
        elif len(descriptions) == 1:
            raise RuntimeError("There is only one deploy.")
        else:
            most_recent_two = descriptions[:2]
            return most_recent_two

    def unrelease_most_recent_image(self):
        """
        Moves the released tag from the most recent deploy image to the penultimate deploy image.
        This sets up for a Fargate redeploy to find that previous build when it next redeploys ECS.
        but it does not actually invoke that redeploy.
        """
        [most_recent, previous] = self._most_recent_two_deploys()
        if RELEASED_TAG not in most_recent.get('imageTags', []):
            self.show_image_catalog(limit=RELEASED_TAG)
            raise RuntimeError(f"Most recent deploy is NOT marked {RELEASED_TAG!r}."
                               f" You'll have to sort this out manually.")
        self.add_image_tag(digest=previous['imageDigest'], tag=RELEASED_TAG)


def show_image_catalog_main(override_args=None):
    """
    This is the main program that can be used from the command line to invoke the show_image_catalog function.

    Args:
        override_args: a list of arguments to  use instead of the command line arguments (usually for testing)
    """

    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is invalid
        description="Show descriptions of all images in ECS", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--account-number', dest='account_number',
                        default=DEFAULT_ACCOUNT_NUMBER, metavar="<acct-num>",
                        help=f"AWS account number containing repository (default: {DEFAULT_ACCOUNT_NUMBER})")
    parser.add_argument('--ecs-repository', dest='ecs_repository',
                        default=DEFAULT_IMAGE_REPOSITORY, metavar="<repo-name>",
                        help=f"repository name to show images for (default: {DEFAULT_IMAGE_REPOSITORY})")
    parser.add_argument('--limit', default=ECRUtils.IMAGE_LIST_DEFAULT_COUNT_LIMIT, metavar="<n>", type=int,
                        help=f"maximum number of images to describe"
                             f" (default: {ECRUtils.IMAGE_LIST_DEFAULT_COUNT_LIMIT})")
    parsed = parser.parse_args(args=override_args)

    with ecr_command_context(account_number=parsed.account_number,
                             ecs_repository=parsed.ecs_repository) as command_context:
        command_context.show_image_catalog(limit=parsed.limit)


def show_image_manifest_main(override_args=None):
    """
    This is the main program that can be used from the command line to invoke the show_image_manifest function.

    NOTE: This command may be useful primarily for debugging. This manifest is needed to support add_image_tag,
          but most people don't care about seeing this data.

    Args:
        override_args: a list of arguments to  use instead of the command line arguments (usually for testing)
    """

    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is invalid
        description="Show the manifest for an ECS image, specified by either label or digest (sha).", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--account-number', dest='account_number',
                        default=DEFAULT_ACCOUNT_NUMBER, metavar="<acct-num>",
                        help=f"AWS account number containing repository (default: {DEFAULT_ACCOUNT_NUMBER})")
    parser.add_argument('--ecs-repository', dest='ecs_repository',
                        default=DEFAULT_IMAGE_REPOSITORY, metavar="<repo-name>",
                        help=f"repository name to show images for (default: {DEFAULT_IMAGE_REPOSITORY})")
    parser.add_argument('--digest', default=None, metavar="<sha>",
                        help=f"digest (sha) to look for (required unless --tag is given)")
    parser.add_argument('--tag', default=None, metavar="<tag>",
                        help=f"tag to look for (required unless --digest is given)")
    parsed = parser.parse_args(args=override_args)

    with ecr_command_context(account_number=parsed.account_number,
                             ecs_repository=parsed.ecs_repository) as command_context:
        command_context.show_image_manifest(tag=parsed.tag, digest=parsed.digest)


def add_image_tag_main(override_args=None):
    """
    This is the main program that can be used from the command line to invoke the add_image_tag function.

    Args:
        override_args: a list of arguments to  use instead of the command line arguments (usually for testing)
    """
    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is invalid
        description="Adds a tag to a given image.", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--account-number', dest='account_number',
                        default=CONFIRM_DEFAULT_ACCOUNT_NUMBER, metavar="<acct-num>",
                        help=f"AWS account number containing repository"
                             f" (default: {DEFAULT_ACCOUNT_NUMBER}, requires interactive confirmation if unsupplied)")
    parser.add_argument('--ecs-repository', dest='ecs_repository',
                        default=DEFAULT_IMAGE_REPOSITORY, metavar="<repo-name>",
                        help=f"repository name to show images for (default: {DEFAULT_IMAGE_REPOSITORY})")
    parser.add_argument('digest', metavar="<sha>",
                        help="digest (sha) of image to add tag to (REQUIRED)")
    parser.add_argument('tag', metavar="<tag>",
                        help="tag to add (REQUIRED)")
    parsed = parser.parse_args(args=override_args)

    with ecr_command_context(account_number=parsed.account_number,
                             ecs_repository=parsed.ecs_repository) as command_context:
        command_context.add_image_tag(tag=parsed.tag, digest=parsed.digest)


def unrelease_most_recent_image_main(override_args=None):
    """
    This is the main program that can be used from the command line to invoke the unrelease_most_recent_image function.

    Args:
        override_args: a list of arguments to  use instead of the command line arguments (usually for testing)
    """
    parser = argparse.ArgumentParser(  # noqa - PyCharm wrongly thinks the formatter_class is invalid
        description=f"Moves the {RELEASED_TAG!r} tag from most recent deploy to the previous deploy.", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--account-number', dest='account_number',
                        default=CONFIRM_DEFAULT_ACCOUNT_NUMBER, metavar="<acct-num>",
                        help=f"AWS account number containing repository"
                             f" (default: {DEFAULT_ACCOUNT_NUMBER}, requires interactive confirmation if unsupplied)")
    parser.add_argument('--ecs-repository', dest='ecs_repository',
                        default=DEFAULT_IMAGE_REPOSITORY, metavar="<repo-name>",
                        help=f"repository name to show images for (default: {DEFAULT_IMAGE_REPOSITORY})")
    parsed = parser.parse_args(args=override_args)

    with ecr_command_context(account_number=parsed.account_number,
                             ecs_repository=parsed.ecs_repository) as command_context:
        command_context.unrelease_most_recent_image()
