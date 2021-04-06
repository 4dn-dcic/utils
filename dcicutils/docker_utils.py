import docker


class DockerUtils:
    """ Intended for use in conjunction with ECR Utils to facilitate automated image
        building/uploading. The caller must have AWS perms and be running Docker.
    """

    def __init__(self):
        self.client = docker.from_env()

    def login(self, *, ecr_repo_uri, ecr_user, ecr_pass):
        """ Authenticates with ECR through Docker - takes as input the result of
            ecr_client.get_authorization_token()
        """
        # works by side effect
        self.client.login(username=ecr_user, password=ecr_pass, registry=ecr_repo_uri)

    def build_image(self, *, path: str, tag='latest', rm=True):
        """ Builds an image with the given tag on the given path. """
        image, build_log = self.client.images.build(path=path, tag=tag, rm=rm)
        return image, build_log

    @staticmethod
    def tag_image(*, image, tag, ecr_repo_name):
        """ Tags a given image. """
        # works by side-effect
        image.tag(ecr_repo_name, tag=tag)

    def push_image(self, tag, ecr_repo_name):
        """ Pushes the tag - presumes the above tag_image method has been called. """
        return self.client.images.push(ecr_repo_name, tag=tag)
