import boto3
from .misc_utils import PRINT


def get_ecs_real_url(env):
    """ Grabs the ECS target URL created on orchestration from Cloudformation.
        NOTE: this is not intended to function with multiple ECS stacks in the
        same account as of right now.
    """
    cfn_client = boto3.client('cloudformation')
    stacks = cfn_client.describe_stacks().get('Stacks', [])
    for stack in stacks:
        for output in stack['Outputs']:
            if output.get('OutputKey', '') == ('ECSApplicationURL%s' % env.replace('-', '')):
                return output.get('OutputValue')
    PRINT('Did not locate the server from Cloudformation! Check ECS Stack metadata.')
    return ''
