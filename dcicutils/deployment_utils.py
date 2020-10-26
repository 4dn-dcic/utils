"""
Based on environment variables, build a suitable production.ini file for deployment.

For example, fourfront/deploy/generate_production_ini.py might contain:

  from dcicutils.deployment_utils import Deployer

  class FourfrontDeployer(Deployer):
      _MY_DIR = os.path.dirname(__file__)
      TEMPLATE_DIR = os.path.join(_MY_DIR, "ini_files")
      PYPROJECT_FILE_NAME = os.path.join(os.path.dirname(_MY_DIR), "pyproject.toml")

  def main():
      FourfrontDeployer.main()

  if __name__ == '__main__':
      main()

"""

import argparse
import datetime
import glob
import io
import json
import os
import pkg_resources
import re
import subprocess
import sys
import toml
import time
import boto3
from git import Repo

from dcicutils.beanstalk_utils import compute_ff_prd_env, compute_cgap_prd_env
from dcicutils.env_utils import (
    get_standard_mirror_env, data_set_for_env, get_bucket_env, INDEXER_ENVS,
    is_fourfront_env, is_cgap_env, is_stg_or_prd_env, is_test_env, is_hotseat_env,
    FF_ENV_INDEXER, CGAP_ENV_INDEXER, is_indexer_env, indexer_env_for_env,
)
from dcicutils.misc_utils import PRINT, Retry, apply_dict_overrides
from dcicutils.qa_utils import override_environ


# constants associated with EB-related APIs
EB_CONFIGURATION_SETTINGS = 'ConfigurationSettings'
EB_OPTION_SETTINGS = 'OptionSettings'
EB_ENV_VARIABLE_NAMESPACE = 'aws:elasticbeanstalk:application:environment'
STATUS = 'Status'
UPDATING = 'Updating'
LAUNCHING = 'Launching'
TERMINATING = 'Terminating'


def boolean_setting(settings, key, default=None):
    """
    Given a setting from a .ini file and returns a boolean interpretation of it.
    - Treats an actual python boolean as itself
    - Using case-insensitive comparison, treats the string 'true' as True and both 'false' and '' as False.
    - If an element is missing,
    - Treats None the same as the option being missing, so returns the given default.
    - Raises an error for any non-standard value. This is harsh, but it should not happen.
    """
    if key not in settings:
        return default
    setting = settings[key]
    if isinstance(setting, str):
        setting_lower = setting.lower()
        if setting_lower in ("", "false"):
            return False
        elif setting_lower == "true":
            return True
        else:
            return setting
    else:  # booleans, None, and odd types (though we might want to consider making other types an error).
        return setting


class EBDeployer:

    # Options specifically related to our deployment
    S3_BUCKET = 'dcic-application-versions'
    EB_APPLICATION = '4dn-web'  # XXX: will need to change if this changes -Will
    DEFAULT_INDEXER_SIZE = 'c5.4xlarge'

    @staticmethod
    def archive_repository(path_to_repo, application_version_name, branch='master'):
        """ Creates an application archive of the maser

        :param path_to_repo: path to repository
        :param application_version_name: name to give application version
        :param branch: branch to archive
        :return: path to application archive
        """
        repo = Repo(path_to_repo)
        repo.git.checkout(branch)
        assert not repo.bare  # noqa doing this to catch a common problem -Will
        zip_location = os.path.join(path_to_repo, (application_version_name + '.zip'))
        if os.path.exists(zip_location):
            raise RuntimeError('zip_location already exists: %s - Please "rm" this file to use this name.'
                               % zip_location)
        with open(zip_location, 'wb') as fp:
            repo.archive(fp, format='zip')
        return zip_location

    @classmethod
    def upload_application_to_s3(cls, zip_location):
        """ Uploads the zip file at zip_location to the specified S3 bucket

        :param zip_location: where to find zip file to upload
        :return: True in success, False otherwise
        """
        s3_client = boto3.client('s3')
        return s3_client.put_object(
            ACL='public-read',
            Body=open(zip_location, 'rb'),
            Bucket=cls.S3_BUCKET,
            Key=os.path.basename(zip_location)  # application_version_name.zip
        )

    @classmethod
    def _build_application_version(cls, key, name):
        """ Uploads the application version at Bucket:Key to Elastic Beanstalk

        :param key: s3 key
        :param name: name of application
        :return: True in success, False otherwise
        """
        eb_client = boto3.client('elasticbeanstalk')
        return eb_client.create_application_version(
            ApplicationName=cls.EB_APPLICATION,
            VersionLabel=name,  # application_version_name
            SourceBundle={
                "S3Bucket": cls.S3_BUCKET,
                "S3Key": key  # application_version_name.zip
            },
            Process=True
        )

    @classmethod
    def build_application_version(cls, path_to_repo, application_version_name, branch='master'):
        """ Builds and uploads an application version to S3

        :param path_to_repo: path to repository
        :param application_version_name: name to give application version
        :param branch: branch to checkout on repo, default 'master'
        :return: True in success, False otherwise
        """
        if not os.path.exists(path_to_repo):
            raise RuntimeError('Gave a non-existent path to repository: %s' % path_to_repo)

        # Archive repository
        zip_location = cls.archive_repository(path_to_repo, application_version_name, branch=branch)

        # Upload to s3
        success = cls.upload_application_to_s3(zip_location)
        if not success:  # XXX: how to correctly detect error? docs are not clear - Will
            print(success)  # look at response
            return False

        # Build application version
        key = os.path.basename(zip_location)
        return cls._build_application_version(key, key[:key.index('.zip')])

    @classmethod
    def _deploy(cls, app_version, env_name):
        """ Deploys the application at s3://application_versions/key to the given env_name

        :param app_version: name of application version
        :param env_name: env to deploy to
        :return: True in success
        """
        eb_client = boto3.client('elasticbeanstalk')
        return eb_client.update_environment(
            ApplicationName=cls.EB_APPLICATION,
            EnvironmentName=env_name,
            VersionLabel=app_version
        )

    @classmethod
    def deploy_new_version(cls, env_name, path_to_repo, application_version_name):
        """  Deploys a new version to EB env_name, where env_name must be one of the valid environments

        :param env_name: env to deploy to
        :param path_to_repo: path to repo to deploy
        :param application_version_name: application version name to give this version
        :raises: RuntimeError if repo does not match env
        """
        if is_fourfront_env(env_name):
            if '/fourfront' not in path_to_repo:
                raise RuntimeError('Tried to deploy fourfront env but path to repo does not contain "fourfront",'
                                   ' aborting: %s' % path_to_repo)
            cls._deploy(application_version_name, env_name)
        elif is_cgap_env(env_name):
            if '/cgap-portal' not in path_to_repo:
                raise RuntimeError('Tried to deploy cgap env but path to repo does not contain "cgap-portal",'
                                   ' aborting: %s' % path_to_repo)
            cls._deploy(application_version_name, env_name)
        else:
            raise RuntimeError('Tried to deploy to invalid environment: %s' % env_name)

    @staticmethod
    def extract_environment_id(env_name):
        """ Grabs the environment ID of the given env_name (to be used to base the new configuration
            template off of).

        :param env_name: name of environment you need the ID of
        :return: env_name's ID or None if env_name does not exist
        """
        eb_client = boto3.client('elasticbeanstalk')
        envs = eb_client.describe_environments()['Environments']
        for env in envs:
            if env['EnvironmentName'] == env_name:
                return env['EnvironmentId']
        return None

    @classmethod
    def extract_env_name_configuration(cls, client, env_name):
        """ Extracts the EB Configuration options corresponding to 'env_name'.

        :param client: boto3 elasticbeanstalk client
        :param env_name: env_name whose configuration we'd like to download
        :return: dictionary of configuration options. See boto3 docs for more info.
        """
        all_settings = client.describe_configuration_settings(
            ApplicationName=cls.EB_APPLICATION,
            EnvironmentName=env_name
        )
        env_settings = all_settings[EB_CONFIGURATION_SETTINGS][0]
        configurable_options = env_settings[EB_OPTION_SETTINGS]
        return configurable_options

    @classmethod
    @Retry.retry_allowed(retries_allowed=1, wait_seconds=10)
    def verify_template_creation(cls, client, template_name):
        """ Does a get for the given template_name to verify EB has recognized that it is
            available.

        :param client: boto3 elasticbeanstalk client
        :param template_name: name of template to check
        :returns: True if template can be acquired
        :raises: Exception if one is encountered
        """
        client.describe_configuration_settings(
            ApplicationName=cls.EB_APPLICATION,
            TemplateName=template_name
        )
        return True

    @classmethod
    def create_indexer_configuration_template(cls, env_name, size=None):
        """ Uploads an indexer configuration template to EB

        :param env_name: env to create an indexer for
        :param size: Machine size to use, see AWS docs for valid values. When in doubt the default should work well.
        :return: True if successful, False otherwise
        """
        eb_client = boto3.client('elasticbeanstalk')
        configuration = cls.extract_env_name_configuration(eb_client, env_name)

        # Add ENCODED_INDEX_SERVER env variable
        configuration.append({
            'Namespace': EB_ENV_VARIABLE_NAMESPACE,
            'OptionName': 'ENCODED_INDEX_SERVER',
            'Value': 'True'
        })

        # make additional updates as needed
        # XXX: Make worker tier perhaps?
        for option in configuration:

            # make it a big machine (or not if we say so)
            if option['OptionName'] == 'InstanceType':
                option['Value'] = cls.DEFAULT_INDEXER_SIZE if not size else size

            # add the additional env variable here as well
            if option['OptionName'] == 'EnvironmentVariables':
                option['Value'] += ',ENCODED_INDEX_SERVER=True'

        # filter '' option settings and known 'bad' options
        # XXX: Refactor into the above loop? Don't think it really matters since this code doesn't need
        # to be high performance and it's convenient to organize logic like this.
        configuration = list(filter(lambda d: (d.get('Value', '') != '' and d['OptionName'] not in ['AppSource']),
                                    configuration))

        # upload the template
        indexer_env = indexer_env_for_env(env_name)
        eb_client.create_configuration_template(
            ApplicationName=cls.EB_APPLICATION,
            TemplateName=indexer_env,
            OptionSettings=configuration,
            EnvironmentId=cls.extract_environment_id(env_name)
        )
        return cls.verify_template_creation(eb_client, indexer_env)

    @classmethod
    def create_indexer_environment(cls, env_name, app_version):
        """ Creates a new environment for indexing based on the given env_name. Will look for a template
            called FF_ENV_INDEXER or CGAP_ENV_INDEXER, if that does not exist this call will fail.

        :param env_name: env to base template off of
        :param app_version: application version to deploy, MUST match that running on env_name
        :return: result of EB env creation
        :raises RuntimeError if bad env given
        """
        eb_client = boto3.client('elasticbeanstalk')
        if is_cgap_env(env_name):
            return eb_client.create_environment(
                ApplicationName=cls.EB_APPLICATION,
                EnvironmentName=CGAP_ENV_INDEXER,
                TemplateName=CGAP_ENV_INDEXER,
                VersionLabel=app_version
            )[STATUS] == LAUNCHING and cls.delete_indexer_template(eb_client, CGAP_ENV_INDEXER)
        elif is_fourfront_env(env_name):
            return eb_client.create_environment(
                ApplicationName=cls.EB_APPLICATION,
                EnvironmentName=FF_ENV_INDEXER,
                TemplateName=FF_ENV_INDEXER,
                VersionLabel=app_version
            )[STATUS] == LAUNCHING and cls.delete_indexer_template(eb_client, FF_ENV_INDEXER)
        else:  # should never get here, but for good measure
            raise RuntimeError('Tried to deploy indexer from an unknown environment: %s' % env_name)

    @classmethod
    def delete_indexer_template(cls, client, template_name):
        """ Wrapper for "delete_configuration_template" that will only accept indexer_envs

        :param client: boto3 elasticbeanstalk client
        :param template_name: template to delete, will only accept indexer env templates
        :return: True in success, False otherwise
        """
        if not is_indexer_env(template_name):
            raise RuntimeError('Tried to delete non-indexer configuration template: %s. '
                               'Please use boto3 directly or the AWS Console to do this.' % template_name)
        return client.delete_configuration_template(
            ApplicationName=cls.EB_APPLICATION,
            TemplateName=template_name
        )

    @classmethod
    def deploy_indexer(cls, env_name, app_version):
        """ Deploys an indexer application based on the given env_name

        :param env_name: env_name to deploy an indexer to
        :param app_version: version of application to deploy, MUST match that running on env_name
        :return: True in success, False otherwise
        """
        template_creation_was_successful = cls.create_indexer_configuration_template(env_name)
        if template_creation_was_successful:
            return cls.create_indexer_environment(env_name, app_version)
        else:
            raise RuntimeError('Template creation unsuccessful with response: %s' % template_creation_was_successful)

    @staticmethod
    def terminate_indexer_env(client, env_name):
        """ Wrapper for "terminate_environment" that will only accept an indexer env.
            NOTE: there is 1 hr timeout before you can recreate one of these for the same application.

        :param client: boto3 elasticbeanstalk client
        :param env_name: one of: FF_ENV_INDEXER or CGAP_ENV_INDEXER
        :return: True in success, False otherwise
        """
        if not is_indexer_env(env_name):
            raise RuntimeError('Tried to terminate non-indexer environment: %s. '
                               'Please use boto3 directly or the AWS Console to do this.' % env_name)
        return client.terminate_environment(
            EnvironmentName=env_name,
        )[STATUS] == TERMINATING

    @classmethod
    def main(cls):
        """ Deploys a version to an Elastic Beanstalk environment based on arguments """
        parser = argparse.ArgumentParser(
            description='Deploys an application to Elastic Beanstalk'
        )
        parser.add_argument('env', help='Environment to deploy to')
        parser.add_argument('repo', help='Path to repository to deploy')
        parser.add_argument('version_name', help='Name of new application version we are generating')
        parser.add_argument('application_version', help='Application version to deploy, not used if not deploying'
                                                        'an indexer application.')
        parser.add_argument('--branch', help='Branch of repo to deploy', default='master')
        parser.add_argument('--indexer', help='Whether or not to deploy an indexer server. Note that only'
                                              'one can be active per HMS Domain.', action='store_true',
                            default=False)
        args = parser.parse_args()

        if not args.indexer:
            packaging_was_successful = cls.build_application_version(args.repo, args.version_name, branch=args.branch)
            if packaging_was_successful:  # XXX: how to best detect?
                time.sleep(5)  # give EB a second to catch up (it needs it)
                exit(cls.deploy_new_version(args.env, args.repo, args.version_name))
        else:
            exit(cls.deploy_indexer(args.env, args.application_version))


class IniFileManager:

    TEMPLATE_DIR = None
    INI_FILE_NAME = "production.ini"
    PYPROJECT_FILE_NAME = None

    @classmethod
    def build_ini_file_from_template(cls, template_file_name, init_file_name,
                                     bs_env=None, bs_mirror_env=None, s3_bucket_env=None,
                                     data_set=None, es_server=None, es_namespace=None,
                                     indexer=None, index_server=None, sentry_dsn=None):
        """
        Builds a .ini file from a given template file.

        Args:
            template_file_name (str): The name of the template file to drive the construction.
            init_file_name (str): The name of the .ini file to build.
            bs_env (str): The ElasticBeanstalk environment name for which this .ini file should work.
            bs_mirror_env (str): The name of the ElasticBeanstalk environment that acts as a blue/green mirror.
            s3_bucket_env (str): Environment name that is part of the s3 bucket name. (Usually defaults properly.)
            data_set (str): An identifier for data to load (either 'prod' for prd/stg envs, or 'test' for others)
            es_server (str): The server name (or server:port) for the ElasticSearch server.
            es_namespace (str): The ElasticSearch namespace to use (probably but not necessarily same as bs_env).
            indexer (bool): Whether or not we are building an ini file for an indexer.
            index_server (bool): Whether or not we are building an ini file for an index server.
        """
        with io.open(init_file_name, 'w') as init_file_fp:
            cls.build_ini_stream_from_template(template_file_name=template_file_name,
                                               init_file_stream=init_file_fp,
                                               bs_env=bs_env,
                                               bs_mirror_env=bs_mirror_env,
                                               s3_bucket_env=s3_bucket_env,
                                               data_set=data_set,
                                               es_server=es_server,
                                               es_namespace=es_namespace,
                                               indexer=indexer,
                                               sentry_dsn=sentry_dsn)

    # Ref: https://stackoverflow.com/questions/19911123/how-can-you-get-the-elastic-beanstalk-application-version-in-your-application  # noqa: E501
    EB_MANIFEST_FILENAME = "/opt/elasticbeanstalk/deploy/manifest"

    @classmethod
    def get_eb_bundled_version(cls):
        """
        Returns the version of the ElasticBeanstalk source bundle, by inspecting its manifest.
        The manifest is a JSON dictionary and the version is associated with the 'VersionLabel' key.
        This will return None if that information cannot be obtained.
        """
        if os.path.exists(cls.EB_MANIFEST_FILENAME):
            try:
                with io.open(cls.EB_MANIFEST_FILENAME, 'r') as fp:
                    data = json.load(fp)
                return data.get('VersionLabel')
            except Exception:
                return None
        else:
            return None

    @classmethod
    def get_local_git_version(cls):
        return subprocess.check_output(['git', 'describe', '--dirty']).decode('utf-8').strip('\n')

    @classmethod
    def get_app_version(cls):  # This logic (perhaps most or all of this file) should move to dcicutils
        try:
            return cls.get_eb_bundled_version() or cls.get_local_git_version()
        except Exception:
            return 'unknown-version-at-' + datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")

    PARAMETERIZED_ASSIGNMENT = re.compile(r'^[ \t]*[A-Za-z][A-Za-z0-9.-_]*[ \t]*=[ \t]*[$][{]?[A-Za-z].*$')
    EMPTY_ASSIGNMENT = re.compile(r'^[ \t]*[A-Za-z][A-Za-z0-9.-_]*[ \t]*=[ \t\r\n]*$')

    @classmethod
    def omittable(cls, line, expanded_line):
        return cls.PARAMETERIZED_ASSIGNMENT.match(line) and cls.EMPTY_ASSIGNMENT.match(expanded_line)

    AUTO_INDEX_SERVER_TOKEN = "__index_server"

    @classmethod
    def build_ini_stream_from_template(cls, template_file_name, init_file_stream,
                                       bs_env=None, bs_mirror_env=None, s3_bucket_env=None, data_set=None,
                                       es_server=None, es_namespace=None, indexer=None, index_server=None,
                                       sentry_dsn=None):
        """
        Sends output to init_file_stream corresponding to the data noe would want in an ini file
        for the given template_file_name and available environment variables.

        Args:
            template_file_name: The template file to guide the output.
            init_file_stream: A stream to send output to.
            bs_env: A beanstalk environment.
            bs_mirror_env: A beanstalk environment.
            s3_bucket_env: Environment name that is part of the s3 bucket name. (Usually defaults properly.)
            data_set: 'test' or 'prod'. Default is 'test' unless bs_env is a staging or production environment.
            es_server: The name of an es server to use.
            es_namespace: The namespace to use on the es server. If None, this uses the bs_env.
            indexer: Whether or not we are building an ini file for an indexer.
            index_server: Whether or not we are building an ini file for an index server.
            sentry_dsn: A sentry DSN specifier, or the empty string if none is desired.

        Returns: None

        """

        es_server = es_server or os.environ.get('ENCODED_ES_SERVER', "MISSING_ENCODED_ES_SERVER")
        bs_env = bs_env or os.environ.get("ENCODED_BS_ENV", "MISSING_ENCODED_BS_ENV")
        bs_mirror_env = bs_mirror_env or os.environ.get("ENCODED_BS_MIRROR_ENV", get_standard_mirror_env(bs_env)) or ""
        s3_bucket_env = s3_bucket_env or os.environ.get("ENCODED_S3_BUCKET_ENV", get_bucket_env(bs_env))
        data_set = data_set or os.environ.get("ENCODED_DATA_SET",
                                              data_set_for_env(bs_env) or "MISSING_ENCODED_DATA_SET")
        es_namespace = es_namespace or os.environ.get("ENCODED_ES_NAMESPACE", bs_env)
        sentry_dsn = sentry_dsn or os.environ.get("ENCODED_SENTRY_DSN", "")
        # Set ENCODED_INDEXER to 'true' to deploy an indexer.
        # If the value is missing, the empty string, or any other thing besides 'true' (in any case),
        # this value will default to the empty string, causing the line not to appear in the output file
        # because there is a special case that suppresses output of empty values. -kmp 27-Apr-2020

        if indexer is None:  # If argument is not None, then it's True or False. Use that.
            env_var_val = os.environ.get('ENCODED_INDEXER', "true").upper()
            if env_var_val == "FALSE":
                indexer = False
            else:
                indexer = True
        indexer = "true" if indexer else ""  # this will omit the line if it's going to be False

        app_version = cls.get_app_version()

        if index_server is None:  # If argument is not None, then it's True or False. Use that.

            if "ENCODED_INDEX_SERVER" not in os.environ and cls.AUTO_INDEX_SERVER_TOKEN in app_version:
                index_server = True
            else:
                server_env_var_val = os.environ.get('ENCODED_INDEX_SERVER', "false").upper()
                if server_env_var_val == "FALSE":
                    index_server = False
                else:
                    index_server = True
        index_server = "true" if index_server else ""  # this will omit the line if it's going to be False

        extra_vars = {
            'APP_VERSION': app_version,
            'PROJECT_VERSION': toml.load(cls.PYPROJECT_FILE_NAME)['tool']['poetry']['version'],
            'SNOVAULT_VERSION': pkg_resources.get_distribution("dcicsnovault").version,
            'UTILS_VERSION': pkg_resources.get_distribution("dcicutils").version,
            'ES_SERVER': es_server,
            'BS_ENV': bs_env,
            'BS_MIRROR_ENV': bs_mirror_env,
            'S3_BUCKET_ENV': s3_bucket_env,
            'DATA_SET': data_set,
            'ES_NAMESPACE': es_namespace,
            'INDEXER': indexer,
            'INDEX_SERVER': index_server,
            'SENTRY_DSN': sentry_dsn,
        }

        # if we specify an indexer name for bs_env, we did the deployment wrong and should bail
        if bs_env in INDEXER_ENVS:
            raise RuntimeError("Deployed with bs_env %s, which is an indexer env."
                               " Re-deploy with the env you want to index and set the 'ENCODED_INDEXER'"
                               " environment variable." % bs_env)

        # We assume these variables are not set, but best to check first. Confusion might result otherwise.
        for extra_var, extra_var_val in extra_vars.items():
            if extra_var in os.environ and extra_var_val != os.environ[extra_var]:
                raise RuntimeError("The environment variable %s is already set to %s,"
                                   " but you are trying to set it to %s."
                                   % (extra_var, os.environ[extra_var], extra_var_val))

        # When we've checked everything, go ahead and do the bindings.
        with override_environ(**extra_vars):

            with io.open(template_file_name, 'r') as template_fp:
                for line in template_fp:
                    expanded_line = os.path.expandvars(line)
                    # Uncomment for debugging, but this must not be disabled for production code so that passwords
                    # are not echoed into logs. -kmp 26-Feb-2020
                    # if '$' in line:
                    #     print("line=", line)
                    #     print("expanded_line=", expanded_line)
                    if not cls.omittable(line, expanded_line):
                        init_file_stream.write(expanded_line)

    @classmethod
    def any_environment_template_filename(cls):
        file = os.path.join(cls.TEMPLATE_DIR, "any.ini")
        if not os.path.exists(file):
            raise ValueError("Special template any.ini was not found.")
        return file

    @classmethod
    def environment_template_filename(cls, env_name):
        prefixes = ["fourfront-", "cgap-"]
        short_env_name = None
        for prefix in prefixes:
            if env_name.startswith(prefix):
                short_env_name = env_name[len(prefix):]
                break
        if short_env_name is None:
            short_env_name = env_name
        file = os.path.join(cls.TEMPLATE_DIR, short_env_name + ".ini")
        if not os.path.exists(file):
            raise ValueError("No such environment: %s" % env_name)
        return file

    @classmethod
    def template_environment_names(cls):
        return sorted([
            os.path.splitext(os.path.basename(file))[0]
            for file in glob.glob(os.path.join(cls.TEMPLATE_DIR, "*"))
        ])

    class GenerationError(Exception):
        pass

    @classmethod
    def main(cls):
        try:
            if 'ENV_NAME' not in os.environ:
                raise cls.GenerationError("ENV_NAME is not set.")
            parser = argparse.ArgumentParser(
                description="Generates a product.ini file from a template appropriate for the given environment,"
                            " which defaults from the value of the ENV_NAME environment variable "
                            " and may be given with or without a 'fourfront-' prefix. ")
            parser.add_argument("--use_any",
                                help="whether or not to prefer the new any.ini template over a named template",
                                action='store_true',
                                # In order for us to change the default to True, we'd need to re-issue beanstalks
                                # with the new environment variables. The any.ini template relies on different
                                # variables. -kmp 29-Apr-2020
                                default=False)
            parser.add_argument("--env",
                                help="environment name",
                                default=os.environ['ENV_NAME'],
                                choices=cls.template_environment_names())
            parser.add_argument("--target",
                                help="the name of a .ini file to generate",
                                default=cls.INI_FILE_NAME)
            parser.add_argument("--bs_env",
                                help="an ElasticBeanstalk environment name",
                                default=None)
            parser.add_argument("--bs_mirror_env",
                                help="the name of the mirror of the ElasticBeanstalk environment name",
                                default=None)
            parser.add_argument("--s3_bucket_env",
                                help="name of env to use in s3 bucket name, usually defaulted without specifying",
                                default=None)
            parser.add_argument("--data_set",
                                help="a data set name",
                                choices=['test', 'prod'],
                                default=None)
            parser.add_argument("--es_server",
                                help="an ElasticSearch servername or servername:port",
                                default=None)
            parser.add_argument("--es_namespace",
                                help="an ElasticSearch namespace",
                                default=None)
            parser.add_argument("--indexer",
                                help="whether this server does indexing at all",
                                choices=["true", "false"],
                                default=None)
            parser.add_argument("--index_server",
                                help="whether this is a standalone indexing server, only doing indexing",
                                choices=["true", "false"],
                                default=None)
            parser.add_argument("--sentry_dsn",
                                help="a sentry DSN",
                                default=None)
            args = parser.parse_args()
            template_file_name = (cls.any_environment_template_filename()
                                  if args.use_any
                                  else cls.environment_template_filename(args.env))
            ini_file_name = args.target
            # print("template_file_name=", template_file_name)
            # print("ini_file_name=", ini_file_name)
            cls.build_ini_file_from_template(template_file_name, ini_file_name,
                                             bs_env=args.bs_env, bs_mirror_env=args.bs_mirror_env,
                                             s3_bucket_env=args.s3_bucket_env, data_set=args.data_set,
                                             es_server=args.es_server, es_namespace=args.es_namespace,
                                             indexer=args.indexer, index_server=args.index_server,
                                             sentry_dsn=args.sentry_dsn)
        except Exception as e:
            PRINT("Error (%s): %s" % (e.__class__.__name__, e))
            sys.exit(1)


# The name Deployer is deprecated. Please use IniFileManager instead of Deployer.
Deployer = IniFileManager


class DeploymentFailure(RuntimeError):
    pass


class CreateMappingOnDeployManager:

    # Set SKIP to True to skip the create_mapping step.

    DEFAULT_DEPLOYMENT_OPTIONS = {'SKIP': False, 'STRICT': False, 'WIPE_ES': False}
    STAGING_DEPLOYMENT_OPTION_OVERRIDES = {'WIPE_ES': True, 'STRICT': True}
    HOTSEAT_DEPLOYMENT_OPTION_OVERRIDES = {'SKIP': True, 'STRICT': True}
    OTHER_TEST_DEPLOYMENT_OPTION_OVERRIDES = {'WIPE_ES': True}
    OTHER_PROD_DEPLOYMENT_OPTION_OVERRIDES = {'SKIP': True}

    @classmethod
    def _summarize_deploy_options(cls, options):
        return "SKIP" if options['SKIP'] else ",".join(k for k in ('STRICT', 'WIPE_ES') if options[k]) or "default"

    @classmethod
    def get_deploy_config(cls, *, env, args, log, client=None, allow_other_prod=False):
        """
        Returns a dictionary describing appropriate options for creating mapping on deploy of a non-prd server.
            {)
                "ENV_NAME": env,    # what environment we're working with
                "WIPE_ES": <bool>,  # whether to wipe ElasticSearch before reindex
                "STRICT": <bool>,   # whether to do a 'strict' reindex
                "SKIP": <bool>,     # whether to skip this step (notwithstanding other options)
            }
        NOTE WELL: This method will fail (raising DeploymentFailure) if called on production.
        """

        deploy_cfg = {
            'ENV_NAME': env
        }

        current_prod_env = compute_ff_prd_env() if is_fourfront_env(env) else compute_cgap_prd_env()

        apply_dict_overrides(deploy_cfg, **cls.DEFAULT_DEPLOYMENT_OPTIONS)
        apply_dict_overrides(deploy_cfg, WIPE_ES=args.wipe_es, SKIP=args.skip, STRICT=args.strict)

        if env == get_standard_mirror_env(current_prod_env):
            description = "currently the staging environment"
            apply_dict_overrides(deploy_cfg, **cls.STAGING_DEPLOYMENT_OPTION_OVERRIDES)
        elif is_stg_or_prd_env(env):
            if env == current_prod_env:
                log.info("Environment %s is currently the production environment."
                         " Something is definitely wrong. We never deploy there, we always CNAME swap."
                         " This deploy cannot proceed. DeploymentFailure will be raised." % env)
                raise DeploymentFailure('Tried to run %s on production.' % client or cls.__name__)
            elif allow_other_prod:
                description = "an uncorrelated production-class environment (neither production nor its staging mirror)"
                apply_dict_overrides(deploy_cfg, **cls.OTHER_PROD_DEPLOYMENT_OPTION_OVERRIDES)
            else:
                log.info("Environment %s is an uncorrelated production-class environment."
                         " Something is definitely wrong."
                         " This deploy cannot proceed. DeploymentFailure will be raised." % env)
                raise DeploymentFailure("Tried to run %s on a production-class environment"
                                        " (neither production nor its staging mirror)." % client or cls.__name__)
        elif is_test_env(env):
            if is_hotseat_env(env):
                description = "a hotseat test environment"
                apply_dict_overrides(deploy_cfg, **cls.HOTSEAT_DEPLOYMENT_OPTION_OVERRIDES)
            else:  # webdev or mastertest
                description = "a non-hotseat test environment"
                apply_dict_overrides(deploy_cfg, **cls.OTHER_TEST_DEPLOYMENT_OPTION_OVERRIDES)
        else:
            description = "an unrecognized environment"
        log.info('Environment %s is %s. Processing mode: %s'
                 % (env, description, cls._summarize_deploy_options(deploy_cfg)))
        return deploy_cfg

    @staticmethod
    def add_argparse_arguments(parser):
        parser.add_argument('--wipe-es', help="Specify to wipe ES", action='store_true', default=None)
        parser.add_argument('--skip', help='Specify to skip this step altogether', default=None)
        parser.add_argument('--strict', help='Specify to do a strict reindex', default=False)


if __name__ == "__main__":
    EBDeployer.main()  # noqa - this is just for debugging
