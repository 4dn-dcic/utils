import boto3
from .misc_utils import PRINT
from .ecr_utils import CGAP_ECR_REGION as CGAP_ECS_REGION


class ECSUtils:
    """ Utility class for interacting with ECS - mostly stubs at this point, but will likely
        expand a lot.
    """
    WSGI = 'wsgi'  # XXX: might want to be joined with an identifier?
    INDEXER = 'indexer'
    INGESTER = 'ingester'
    SERVICES = [
        WSGI, INDEXER, INGESTER
    ]

    def __init__(self, *, cluster_name):
        """ Cluster name in this case is the env name """
        self.cluster_name = cluster_name
        self.client = boto3.client('ecs', region_name=CGAP_ECS_REGION)  # same as ECR

    def update_ecs_service(self, *, service_name=WSGI):
        """ Forces an update of this cluster's service, by default WSGI (for now)
            It is highly likely we want some variants of this - but will start here.
            Note that this just updates a particular service.
        """
        if service_name not in self.SERVICES:
            raise NotImplementedError('Specified service %s is an invalid service!' % service_name)
        try:
            self.client.update_service(cluster=self.cluster_name, service=service_name,
                                       forceNewDeployment=True)
        except Exception as e:
            PRINT('Error encountered triggering cluster update: %s' % e)
            raise

    def update_all_services(self):
        """ Forces an update of all services on this cluster. """
        for service_name in self.SERVICES:
            try:
                self.client.update_service(cluster=self.cluster_name, service=service_name,
                                           forceNewDeployment=True)
                PRINT('Successfully updated ECS cluster %s service %s' % self.cluster_name, service_name)
            except Exception as e:
                PRINT('Error encountered triggering cluster update: %s' % e)
                raise  # abort update right away
