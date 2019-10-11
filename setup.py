import io
from setuptools import setup
from os import path

this_directory = path.abspath(path.dirname(__file__))
with io.open(path.join(this_directory, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()


requires = [
    'boto3>=1.7.42',
    'botocore>=1.10.42',
    'elasticsearch==5.5.3',
    'aws_requests_auth>=0.4.1',
    'urllib3>=1.23',
    'structlog>=18.1.0',
    'requests>=2.20.0'
]

tests_require = [
    'pytest',
    'pytest-mock',
    'pytest-cov',
    'flaky'
]

setup(
    name='dcicutils',
    version=open("dcicutils/_version.py").readlines()[-1].split()[-1].strip("\"'"),
    description='Utility modules shared amongst several repos in the 4dn-dcic organization',
    long_description=long_description,
    long_description_content_type='text/x-rst',
    packages=['dcicutils'],
    include_package_data=True,
    zip_safe=False,
    author='4DN Team at Harvard Medical School',
    author_email='burak_alver@hms.harvard.edu',
    url='https://data.4dnucleome.org',
    license='MIT',
    install_requires=requires,
    setup_requires=['pytest-runner', 'colorama'],
    tests_require=tests_require,
    extras_require={
        'test': tests_require,
    },
)
