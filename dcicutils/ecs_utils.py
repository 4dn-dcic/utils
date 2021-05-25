import boto3
from .misc_utils import PRINT
from .ecr_utils import CGAP_ECR_REGION as CGAP_ECS_REGION


class ECSUtils:
    """ Utility class for interacting with ECS - mostly stubs at this point, but will likely
        expand a lot.
    """

    def __init__(self):
        """ Creates a boto3 client for 'ecs'. """
        self.client = boto3.client('ecs', region_name=CGAP_ECS_REGION)  # same as ECR

    def list_ecs_clusters(self):
        """ Returns a list of ECS clusters ARNs. """
        return self.client.list_clusters().get('clusterArns', [])

    def list_ecs_services(self, *, cluster_name):
        """ Returns a list of ECS Service names under the given cluster. There should be 4 of them as of right now -
            adding more is considered a compatible change.
        """
        services = self.client.list_services(cluster=cluster_name).get('serviceArns', [])
        if len(services) < 4:
            raise Exception('Environment error! Expected at least 4 services - check that ECS finished orchestrating.')
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
