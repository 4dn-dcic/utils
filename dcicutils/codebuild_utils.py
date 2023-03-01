import boto3


class CodeBuildUtils:
    """ Class for interacting with AWS CodeBuild (for Docker builds) """

    def __init__(self, client=None):
        if client:
            self.client = client
        else:
            self.client = boto3.client('codebuild')

    def list_projects(self) -> [str]:
        """ Returns a list of projects on the current AWS Account CodeBuild"""
        return self.client.list_projects().get('projects', [])

    def run_project_build(self, *, project_name: str) -> dict:
        """ Runs the given project build. """
        return self.client.start_build(projectName=project_name)

    def run_project_build_with_overrides(self, *, project_name, branch, env_overrides):
        """ Runs the given project build with branch and environment variable overrides """
        return self.client.start_build(
            projectName=project_name,
            sourceVersion=branch,
            environmentVariablesOverride=[
                {
                    'name': k,
                    'value': v,
                    'type': 'PLAINTEXT'
                } for k, v in env_overrides.items()
            ]
        )
