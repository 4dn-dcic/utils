import boto3

from .common import REGION as COMMON_REGION
from .misc_utils import PRINT


class ECSUtils:
    """ Utility class for interacting with ECS - mostly stubs at this point, but will likely
        expand a lot.
    """
    DEPLOYMENT_COMPLETED = 'COMPLETED'

    REGION = COMMON_REGION  # this default must match what ecr_utils.ECRUtils and secrets_utils.assume_identity use

    def __init__(self, region=None):
        """ Creates a boto3 client for 'ecs'. """
        self.client = boto3.client('ecs', region_name=region or self.REGION)

    def list_ecs_clusters(self):
        """ Returns a list of ECS clusters ARNs. """
        return self.client.list_clusters().get('clusterArns', [])

    def list_ecs_services(self, *, cluster_name):
        """ Returns a list of ECS Service names under the given cluster. There should be 4 of them as of right now -
            adding more is considered a compatible change.
        """
        services = self.client.list_services(cluster=cluster_name).get('serviceArns', [])
        return services

    def update_ecs_service(self, *, cluster_name, service_name):
        """ Forces an update of this cluster's service, by default WSGI (for now)
            It is highly likely we want some variants of this - but will start here.
            Note that this just updates a particular service.
        """
        try:
            self.client.update_service(cluster=cluster_name, service=service_name,
                                       forceNewDeployment=True)
            PRINT('Successfully updated ECS cluster %s service %s' % (cluster_name, service_name))
        except Exception as e:
            PRINT('Error encountered triggering cluster update: %s' % e)
            raise

    def update_all_services(self, *, cluster_name):
        """ Forces an update of all services on this cluster. """
        for service_name in self.list_ecs_services(cluster_name=cluster_name):
            self.update_ecs_service(cluster_name=cluster_name, service_name=service_name)
        return True

    def list_ecs_tasks(self):
        """ Lists all available ECS task definitions. """
        return self.client.list_task_definitions().get('taskDefinitionArns', [])

    def run_ecs_task(self, *, cluster_name, task_name, subnet, security_group):
        """ Runs the given task name on the given cluster.

        :param cluster_name: name of cluster to run task on
        :param task_name: name of task (including revision) to run
        :param subnet: subnet to run task in
        :param security_group: SG to associate with task
        :return: dict response
        """
        return self.client.run_task(
            cluster=cluster_name,
            count=1,
            taskDefinition=task_name,
            networkConfiguration={
                'awsvpcConfiguration': {
                    'subnets': [
                        subnet
                    ],
                    'securityGroups': [
                        security_group
                    ]
                }
            }
        )

    def service_has_active_deployment(self, *, cluster_name: str, services: list) -> bool:
        """ Checks if the given cluster/service has an active deployment running """
        service_meta = self.client.describe_services(cluster=cluster_name, services=services)
        for service in service_meta.get('services', []):
            for deployment in service.get('deployments', []):
                if deployment['rolloutState'] != self.DEPLOYMENT_COMPLETED:
                    return True
        return False
